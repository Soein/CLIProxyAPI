// Package util provides utility functions for the CLI Proxy API server.
package util

import (
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

var gjsonPathKeyReplacer = strings.NewReplacer(".", "\\.", "*", "\\*", "?", "\\?")

const placeholderReasonDescription = "Brief explanation of why you are calling this tool"

const (
	maxInlineLocalRefInputBytes   = 256 << 10
	maxInlineLocalRefDepth        = 24
	maxInlineLocalRefNodes        = 512
	maxInlineLocalRefOutputBytes  = 64 << 10
	maxInlineLocalRefAllocItems   = maxInlineLocalRefNodes * 4
	maxInlineLocalRefPointerBytes = 1024
)

// Pass a single JSON schema to the functions below — never a whole request document.
//
// Cleaning walks every node and rewrites keys by name, and schema keywords such as "title",
// "format", "default" and "const" are also ordinary data keys. Handing these functions a request
// silently rewrites tool-call arguments inside the conversation history: the guard that protects
// a key under ".properties" does not apply to argument values, so the keys are deleted outright
// and replacements such as "enum" and "type" are fabricated. That regression reached production
// once already; scope every call site to the schema itself.

type jsonSchemaCleanOptions struct {
	addPlaceholder                    bool
	antigravitySemantics              bool
	removeToolTitle                   bool
	removeGeminiMetadata              bool
	flattenUnions                     bool
	forceEnumStringType               bool
	dropAllEnums                      bool
	dropBooleanEnums                  bool
	preserveAdditionalPropertiesFalse bool
}

// CleanJSONSchemaForAntigravity transforms a tool schema to be compatible with Antigravity API.
// It handles unsupported keywords, type flattening, and schema simplification while preserving
// semantic information as description hints and adding placeholders required by VALIDATED mode.
func CleanJSONSchemaForAntigravity(jsonStr string) string {
	return CleanJSONSchemaForAntigravityTool(jsonStr, true)
}

// CleanJSONSchemaForAntigravityTool transforms an Antigravity function schema. The private
// backend accepts enum members only as strings, but the declared type still controls the JSON
// type of generated function arguments, so numeric and boolean types must not be rewritten.
// requirePlaceholder is used only for Claude VALIDATED mode.
func CleanJSONSchemaForAntigravityTool(jsonStr string, requirePlaceholder bool) string {
	return cleanJSONSchema(jsonStr, jsonSchemaCleanOptions{
		addPlaceholder:       requirePlaceholder,
		antigravitySemantics: true,
		removeToolTitle:      !requirePlaceholder,
		flattenUnions:        true,
		dropAllEnums:         true,
	})
}

// CleanJSONSchemaForAntigravityResponse transforms a response schema without applying tool-only
// compatibility rewrites that would alter the client's structured output contract.
//
// Sanitization policy:
//   - Passthrough: type, properties, items, required, description, enum, nullable, and
//     additionalProperties: false (which Antigravity natively enforces for response schemas).
//   - Description hints + deletion: unsupported or accepted-but-ignored constraints.
//   - Flattened: allOf merged into properties/required.
//   - Projected: anyOf/oneOf select the strongest branch; null branches become nullable:true.
//   - Resolved: local $ref targets are inlined before $defs/definitions are removed.
//   - Dropped: unresolved $ref (after a hint), metadata, unsupported object-key constraints,
//     conditional keywords (after non-conflicting properties are retained), and x-* extensions.
func CleanJSONSchemaForAntigravityResponse(jsonStr string) string {
	return cleanJSONSchema(jsonStr, jsonSchemaCleanOptions{
		antigravitySemantics:              true,
		flattenUnions:                     true,
		dropBooleanEnums:                  true,
		preserveAdditionalPropertiesFalse: true,
	})
}

// CleanJSONSchemaForGemini transforms a JSON schema to be compatible with Gemini tool calling.
// It removes unsupported keywords and simplifies schemas, without adding empty-schema placeholders.
func CleanJSONSchemaForGemini(jsonStr string) string {
	return cleanJSONSchema(jsonStr, jsonSchemaCleanOptions{
		removeGeminiMetadata: true,
		flattenUnions:        true,
		forceEnumStringType:  true,
	})
}

// cleanJSONSchema performs the core cleaning operations on the JSON schema.
func cleanJSONSchema(jsonStr string, options jsonSchemaCleanOptions) string {
	fallback := schemaCleanBudgetFallback("", options.addPlaceholder)
	if len(jsonStr) > maxInlineLocalRefOutputBytes {
		return fallback
	}
	fallback = schemaCleanBudgetFallback(jsonStr, options.addPlaceholder)
	apply := func(transform func(string) string) bool {
		jsonStr = transform(jsonStr)
		return len(jsonStr) <= maxInlineLocalRefOutputBytes
	}

	// Phase 1: Convert and add hints
	if options.antigravitySemantics {
		if !apply(inlineLocalRefs) {
			return fallback
		}
	}
	if !apply(func(schema string) string { return convertRefsToHints(schema, options.antigravitySemantics) }) ||
		!apply(convertConstToEnum) ||
		!apply(func(schema string) string { return convertEnumValuesToStrings(schema, options.forceEnumStringType) }) ||
		!apply(addEnumHints) ||
		!apply(func(schema string) string { return dropIgnoredEnumsToHints(schema, options) }) {
		return fallback
	}
	if !options.preserveAdditionalPropertiesFalse {
		if !apply(addAdditionalPropertiesHints) {
			return fallback
		}
	}
	if !apply(func(schema string) string { return moveConstraintsToDescription(schema, options) }) {
		return fallback
	}
	if options.antigravitySemantics {
		if !apply(moveNotToDescription) {
			return fallback
		}
	}

	// Phase 2: Flatten complex structures
	if !apply(mergeConditionals) || !apply(mergeAllOf) {
		return fallback
	}
	if options.flattenUnions {
		if !apply(flattenAnyOfOneOf) {
			return fallback
		}
	}
	if !apply(func(schema string) string { return flattenTypeArrays(schema, options.antigravitySemantics) }) {
		return fallback
	}

	// Phase 3: Cleanup
	if !apply(func(schema string) string { return removeUnsupportedKeywords(schema, options) }) {
		return fallback
	}
	if options.removeGeminiMetadata {
		// Gemini schema cleanup: remove nullable/title and placeholder-only fields.
		if !apply(func(schema string) string { return removeKeywords(schema, []string{"nullable", "title"}) }) ||
			!apply(removePlaceholderFields) {
			return fallback
		}
	} else if options.removeToolTitle {
		// Legacy non-VALIDATED Antigravity requests used the Gemini cleaner, which drops title.
		// Keep that harmless metadata policy without losing Antigravity's native nullable support.
		if !apply(func(schema string) string { return removeKeywords(schema, []string{"title"}) }) {
			return fallback
		}
	}
	if !apply(cleanupRequiredFields) {
		return fallback
	}
	// Phase 4: Add placeholder for empty object schemas (Claude VALIDATED mode requirement)
	if options.addPlaceholder {
		if !apply(addEmptySchemaPlaceholder) {
			return fallback
		}
	}

	return jsonStr
}

func schemaCleanBudgetFallback(schema string, requirePlaceholder bool) string {
	typeName := schemaTypeForBudgetFallback(schema)
	if requirePlaceholder && typeName == "object" {
		return `{"type":"object","description":"Schema cleaning output limit exceeded","properties":{"reason":{"type":"string","description":"Brief explanation of why you are calling this tool"}},"required":["reason"]}`
	}
	return fmt.Sprintf(`{"type":%q,"description":"Schema cleaning output limit exceeded"}`, typeName)
}

func schemaTypeForBudgetFallback(schema string) string {
	if schema == "" {
		return "object"
	}
	typeValue := gjson.Get(schema, "type")
	candidates := []gjson.Result{typeValue}
	if typeValue.IsArray() {
		candidates = typeValue.Array()
	}
	for _, candidate := range candidates {
		switch candidate.String() {
		case "array", "boolean", "integer", "number", "object", "string":
			return candidate.String()
		}
	}
	if gjson.Get(schema, "properties").IsObject() {
		return "object"
	}
	if gjson.Get(schema, "items").Exists() {
		return "array"
	}
	return "object"
}

// removeKeywords removes all occurrences of specified keywords from the JSON schema.
func removeKeywords(jsonStr string, keywords []string) string {
	deletePaths := make([]string, 0)
	pathsByField := findPathsByFields(jsonStr, keywords)
	for _, key := range keywords {
		for _, p := range pathsByField[key] {
			if isPropertyDefinition(trimSuffix(p, "."+key)) {
				continue
			}
			deletePaths = append(deletePaths, p)
		}
	}
	sortByDepth(deletePaths)
	for _, p := range deletePaths {
		jsonStr, _ = sjson.Delete(jsonStr, p)
	}
	return jsonStr
}

// removePlaceholderFields removes placeholder-only properties ("_" and "reason") and their required entries.
func removePlaceholderFields(jsonStr string) string {
	// Remove "_" placeholder properties.
	paths := findPaths(jsonStr, "_")
	sortByDepth(paths)
	for _, p := range paths {
		if !strings.HasSuffix(p, ".properties._") {
			continue
		}
		jsonStr, _ = sjson.Delete(jsonStr, p)
		parentPath := trimSuffix(p, ".properties._")
		reqPath := joinPath(parentPath, "required")
		req := gjson.Get(jsonStr, reqPath)
		if req.IsArray() {
			var filtered []string
			for _, r := range req.Array() {
				if r.String() != "_" {
					filtered = append(filtered, r.String())
				}
			}
			if len(filtered) == 0 {
				jsonStr, _ = sjson.Delete(jsonStr, reqPath)
			} else {
				updated, _ := sjson.SetBytes([]byte(jsonStr), reqPath, filtered)
				jsonStr = string(updated)
			}
		}
	}

	// Remove placeholder-only "reason" objects.
	reasonPaths := findPaths(jsonStr, "reason")
	sortByDepth(reasonPaths)
	for _, p := range reasonPaths {
		if !strings.HasSuffix(p, ".properties.reason") {
			continue
		}
		parentPath := trimSuffix(p, ".properties.reason")
		props := gjson.Get(jsonStr, joinPath(parentPath, "properties"))
		if !props.IsObject() || len(props.Map()) != 1 {
			continue
		}
		desc := gjson.Get(jsonStr, p+".description").String()
		if desc != placeholderReasonDescription {
			continue
		}
		jsonStr, _ = sjson.Delete(jsonStr, p)
		reqPath := joinPath(parentPath, "required")
		req := gjson.Get(jsonStr, reqPath)
		if req.IsArray() {
			var filtered []string
			for _, r := range req.Array() {
				if r.String() != "reason" {
					filtered = append(filtered, r.String())
				}
			}
			if len(filtered) == 0 {
				jsonStr, _ = sjson.Delete(jsonStr, reqPath)
			} else {
				updated, _ := sjson.SetBytes([]byte(jsonStr), reqPath, filtered)
				jsonStr = string(updated)
			}
		}
	}

	return jsonStr
}

// inlineLocalRefs resolves JSON Pointer references against the original schema before definition
// containers are stripped. Each expansion receives its own copy, sibling object schemas are
// merged, and cycles or exhausted budgets terminate as typed hints instead of recursing forever.
func inlineLocalRefs(jsonStr string) string {
	if !strings.Contains(jsonStr, `"$ref"`) {
		return jsonStr
	}
	if len(jsonStr) > maxInlineLocalRefInputBytes {
		return `{"type":"object","description":"Local reference input limit exceeded"}`
	}

	decoder := json.NewDecoder(strings.NewReader(jsonStr))
	decoder.UseNumber()
	var root any
	if err := decoder.Decode(&root); err != nil {
		return jsonStr
	}

	resolver := localRefResolver{
		maxDepth:      maxInlineLocalRefDepth,
		maxNodes:      maxInlineLocalRefNodes,
		maxAllocItems: maxInlineLocalRefAllocItems,
		maxBytes:      maxInlineLocalRefOutputBytes,
	}
	resolved, ok := resolver.resolve(root, root, make(map[string]bool), 0)
	if !ok {
		resolved = compactLocalRefFallback(root)
	}
	// estimatedBytes is deliberately conservative, so json.Marshal cannot allocate a result above
	// the output cap on the successful resolver path.
	out, err := json.Marshal(resolved)
	if err != nil || len(out) > maxInlineLocalRefOutputBytes {
		return marshalCompactLocalRefFallback(root)
	}
	return string(out)
}

type localRefResolver struct {
	maxDepth       int
	maxNodes       int
	maxAllocItems  int
	maxBytes       int
	nodes          int
	allocItems     int
	estimatedBytes int
}

func (r *localRefResolver) resolve(root, value any, active map[string]bool, depth int) (any, bool) {
	if r == nil {
		return value, true
	}
	if depth > r.maxDepth {
		return nil, false
	}
	if !r.reserveNode() {
		return nil, false
	}
	switch node := value.(type) {
	case []any:
		if !r.preflightContainerLength(len(node)) || !r.commitContainer(len(node), arrayStructuralBytes(len(node))) {
			return nil, false
		}
		out := make([]any, len(node))
		for i, item := range node {
			resolved, ok := r.resolve(root, item, active, depth+1)
			if !ok {
				return nil, false
			}
			out[i] = resolved
		}
		return out, true
	case map[string]any:
		if !r.preflightContainerLength(retainedMapEntries(node, false)) {
			return nil, false
		}
		ref, hasRef := node["$ref"].(string)
		if hasRef && len(ref) <= maxInlineLocalRefPointerBytes && strings.HasPrefix(ref, "#/") {
			if target, ok := resolveJSONPointer(root, ref); ok {
				if active[ref] {
					hint, okHint := r.typedLocalRefHint(target, ref)
					if !okHint {
						return nil, false
					}
					siblings, okSiblings := r.resolveMapEntries(root, node, active, depth, true)
					if !okSiblings {
						return nil, false
					}
					if len(siblings) == 0 {
						return hint, true
					}
					return r.mergeRefSchemaMaps(hint, siblings)
				}
				active[ref] = true
				resolvedTarget, okResolved := r.resolve(root, target, active, depth+1)
				delete(active, ref)
				if !okResolved {
					return nil, false
				}
				if targetMap, okTarget := resolvedTarget.(map[string]any); okTarget {
					siblings, okSiblings := r.resolveMapEntries(root, node, active, depth, true)
					if !okSiblings {
						return nil, false
					}
					if len(siblings) == 0 {
						return targetMap, true
					}
					return r.mergeRefSchemaMaps(targetMap, siblings)
				}
			}
		}
		return r.resolveMapEntries(root, node, active, depth, false)
	default:
		if !r.reserveBytes(scalarJSONSize(node)) {
			return nil, false
		}
		return value, true
	}
}

func compactLocalRefFallback(root any) map[string]any {
	typeName := boundedSchemaType(root)
	description := "Local reference expansion limit exceeded"
	if rootMap, ok := root.(map[string]any); ok {
		if ref, okRef := rootMap["$ref"].(string); okRef && len(ref) <= maxInlineLocalRefPointerBytes && strings.HasPrefix(ref, "#/") {
			if target, found := resolveJSONPointer(root, ref); found {
				typeName = boundedSchemaType(target)
				description = "See: " + boundedRefName(ref)
			}
		}
	}
	return map[string]any{"type": typeName, "description": description}
}

func marshalCompactLocalRefFallback(root any) string {
	out, err := json.Marshal(compactLocalRefFallback(root))
	if err != nil {
		return `{"type":"object","description":"Local reference expansion limit exceeded"}`
	}
	return string(out)
}

func isLocalDefinitionContainer(key string) bool {
	return key == "$defs" || key == "definitions"
}

func (r *localRefResolver) resolveMapEntries(root any, node map[string]any, active map[string]bool, depth int, skipRef bool) (map[string]any, bool) {
	count := retainedMapEntries(node, skipRef)
	if !r.preflightContainerLength(count) {
		return nil, false
	}
	structuralBytes := objectStructuralBytes(node, skipRef)
	if !r.commitContainer(count, structuralBytes) {
		return nil, false
	}
	keys, okKeys := r.sortedRetainedMapKeys(node, skipRef)
	if !okKeys {
		return nil, false
	}

	out := make(map[string]any, count)
	for _, key := range keys {
		item := node[key]
		resolved, ok := r.resolve(root, item, active, depth+1)
		if !ok {
			return nil, false
		}
		out[key] = resolved
	}
	return out, true
}

func (r *localRefResolver) mergeRefSchemaMaps(base, sibling map[string]any) (map[string]any, bool) {
	if !r.reserveAllocation(len(base) + len(sibling) + 2) {
		return nil, false
	}
	out := make(map[string]any, len(base)+len(sibling)+2)
	for key, value := range base {
		if isLocalDefinitionContainer(key) {
			continue
		}
		out[key] = value
	}
	keys, okKeys := r.sortedMapKeys(sibling)
	if !okKeys {
		return nil, false
	}
	allOfMerged := false
	for _, key := range keys {
		value := sibling[key]
		if key == "const" || key == "enum" || key == "type" {
			continue
		}
		baseValue, exists := out[key]
		if !exists {
			out[key] = value
			continue
		}
		switch key {
		case "properties":
			baseProperties, baseOK := baseValue.(map[string]any)
			siblingProperties, siblingOK := value.(map[string]any)
			if baseOK && siblingOK {
				merged, ok := r.mergeRefProperties(baseProperties, siblingProperties)
				if !ok {
					return nil, false
				}
				out[key] = merged
				continue
			}
		case "required":
			baseRequired, _ := baseValue.([]any)
			siblingRequired, siblingOK := value.([]any)
			if siblingOK {
				merged, ok := r.stableRequiredUnion(baseRequired, siblingRequired)
				if !ok {
					return nil, false
				}
				out[key] = merged
				continue
			}
		case "items":
			baseSchema, baseOK := baseValue.(map[string]any)
			siblingSchema, siblingOK := value.(map[string]any)
			if baseOK && siblingOK {
				merged, ok := r.mergeRefSchemaMaps(baseSchema, siblingSchema)
				if !ok {
					return nil, false
				}
				out[key] = merged
				continue
			}
		case "additionalProperties":
			baseSchema, baseOK := baseValue.(map[string]any)
			siblingSchema, siblingOK := value.(map[string]any)
			if baseOK && siblingOK {
				merged, ok := r.mergeRefSchemaMaps(baseSchema, siblingSchema)
				if !ok {
					return nil, false
				}
				out[key] = merged
				continue
			}
			baseBool, baseBoolOK := baseValue.(bool)
			siblingBool, siblingBoolOK := value.(bool)
			if baseBoolOK && siblingBoolOK {
				out[key] = baseBool && siblingBool
				continue
			}
		case "allOf":
			baseAllOf, baseOK := baseValue.([]any)
			siblingAllOf, siblingOK := value.([]any)
			if baseOK && siblingOK {
				merged, ok := r.flattenAllOfClauses(baseAllOf, siblingAllOf)
				if !ok {
					return nil, false
				}
				out[key] = merged
				allOfMerged = true
				continue
			}
		case "minimum", "exclusiveMinimum", "minLength", "minItems", "minProperties":
			if stricter, ok := stricterNumericConstraint(baseValue, value, true); ok {
				out[key] = stricter
				continue
			}
		case "maximum", "exclusiveMaximum", "maxLength", "maxItems", "maxProperties":
			if stricter, ok := stricterNumericConstraint(baseValue, value, false); ok {
				out[key] = stricter
				continue
			}
		case "uniqueItems":
			baseBool, baseOK := baseValue.(bool)
			siblingBool, siblingOK := value.(bool)
			if baseOK && siblingOK {
				out[key] = baseBool || siblingBool
				continue
			}
		case "nullable":
			baseBool, baseOK := baseValue.(bool)
			siblingBool, siblingOK := value.(bool)
			if baseOK && siblingOK {
				out[key] = baseBool && siblingBool
				continue
			}
		case "description":
			baseDescription, baseOK := baseValue.(string)
			siblingDescription, siblingOK := value.(string)
			if baseOK && siblingOK {
				out[key] = mergeHint(baseDescription, siblingDescription)
				continue
			}
		}
		if schemaValuesEqual(baseValue, value) {
			continue
		}
		// Unknown same-name schema constraints stay conjunctive instead of silently replacing the
		// referenced constraint. Gemini cleanup later projects unsupported constraints to hints.
		if !r.appendAllOfConstraint(out, key, value) {
			return nil, false
		}
		if !r.appendConjunctionConflictHint(out, key, baseValue, value) {
			return nil, false
		}
	}
	if !allOfMerged {
		if clauses, ok := out["allOf"].([]any); ok {
			flattened, okFlattened := r.flattenAllOfClauses(clauses)
			if !okFlattened {
				return nil, false
			}
			out["allOf"] = flattened
		}
	}
	if !r.mergeCoreSchemaConstraints(out, base, sibling) {
		return nil, false
	}
	return out, true
}

func (r *localRefResolver) mergeRefProperties(base, sibling map[string]any) (map[string]any, bool) {
	if !r.reserveAllocation(len(base) + len(sibling)) {
		return nil, false
	}
	out := make(map[string]any, len(base)+len(sibling))
	for key, value := range base {
		out[key] = value
	}
	keys, okKeys := r.sortedMapKeys(sibling)
	if !okKeys {
		return nil, false
	}
	for _, key := range keys {
		value := sibling[key]
		baseValue, exists := out[key]
		if !exists {
			out[key] = value
			continue
		}
		baseSchema, baseOK := baseValue.(map[string]any)
		siblingSchema, siblingOK := value.(map[string]any)
		if baseOK && siblingOK {
			merged, ok := r.mergeRefSchemaMaps(baseSchema, siblingSchema)
			if !ok {
				return nil, false
			}
			out[key] = merged
			continue
		}
		if baseBool, baseBoolOK := baseValue.(bool); baseBoolOK {
			if siblingBool, siblingBoolOK := value.(bool); siblingBoolOK {
				out[key] = baseBool && siblingBool
				continue
			}
		}
		if !schemaValuesEqual(baseValue, value) {
			impossible, ok := r.impossibleSchema()
			if !ok {
				return nil, false
			}
			out[key] = impossible
		}
	}
	return out, true
}

func (r *localRefResolver) mergeCoreSchemaConstraints(out, base, sibling map[string]any) bool {
	impossible := hasEmptySchemaEnum(base) || hasEmptySchemaEnum(sibling)

	baseType, baseHasType := base["type"]
	siblingType, siblingHasType := sibling["type"]
	switch {
	case baseHasType && siblingHasType:
		if !schemaValuesEqual(baseType, siblingType) {
			impossible = true
		}
	case siblingHasType:
		out["type"] = siblingType
	}

	baseEnum, baseHasEnum := base["enum"].([]any)
	siblingEnum, siblingHasEnum := sibling["enum"].([]any)
	var enum []any
	hasEnum := baseHasEnum || siblingHasEnum
	switch {
	case baseHasEnum && siblingHasEnum:
		intersection, ok := r.intersectSchemaEnums(baseEnum, siblingEnum)
		if !ok {
			return false
		}
		enum = intersection
	case baseHasEnum:
		enum = baseEnum
	case siblingHasEnum:
		enum = siblingEnum
	}

	baseConst, baseHasConst := base["const"]
	siblingConst, siblingHasConst := sibling["const"]
	var constValue any
	hasConst := baseHasConst || siblingHasConst
	switch {
	case baseHasConst && siblingHasConst:
		constValue = baseConst
		if !schemaValuesEqual(baseConst, siblingConst) {
			impossible = true
		}
	case baseHasConst:
		constValue = baseConst
	case siblingHasConst:
		constValue = siblingConst
	}

	if hasConst && hasEnum {
		if !schemaEnumContains(enum, constValue) {
			impossible = true
		} else {
			if !r.reserveAllocation(1) {
				return false
			}
			enum = make([]any, 1)
			enum[0] = constValue
		}
	}
	if hasEnum && len(enum) == 0 {
		impossible = true
	}

	if impossible {
		markImpossibleSchema(out)
		return true
	}
	if hasEnum {
		out["enum"] = enum
	}
	if hasConst {
		out["const"] = constValue
	}
	return true
}

func hasEmptySchemaEnum(schema map[string]any) bool {
	enum, ok := schema["enum"].([]any)
	return ok && len(enum) == 0
}

func schemaEnumContains(enum []any, value any) bool {
	for _, candidate := range enum {
		if schemaValuesEqual(candidate, value) {
			return true
		}
	}
	return false
}

func (r *localRefResolver) sortedRetainedMapKeys(node map[string]any, skipRef bool) ([]string, bool) {
	count := retainedMapEntries(node, skipRef)
	if !r.reserveAllocation(count) {
		return nil, false
	}
	keys := make([]string, 0, count)
	for key := range node {
		if isLocalDefinitionContainer(key) || (skipRef && key == "$ref") {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, true
}

func (r *localRefResolver) sortedMapKeys(node map[string]any) ([]string, bool) {
	if !r.reserveAllocation(len(node)) {
		return nil, false
	}
	keys := make([]string, 0, len(node))
	for key := range node {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, true
}

func (r *localRefResolver) flattenAllOfClauses(groups ...[]any) ([]any, bool) {
	clauseCount := 0
	splitEntries := 0
	for _, group := range groups {
		for _, clause := range group {
			count, entries, ok := countFlattenedAllOfClause(clause, 0)
			if !ok {
				return nil, false
			}
			clauseCount += count
			splitEntries += entries
		}
	}
	if !r.reserveAllocation(clauseCount + splitEntries) {
		return nil, false
	}
	out := make([]any, 0, clauseCount)
	for _, group := range groups {
		for _, clause := range group {
			appendFlattenedAllOfClause(&out, clause, 0)
		}
	}
	return out, true
}

func countFlattenedAllOfClause(clause any, depth int) (count, splitEntries int, ok bool) {
	if depth > maxInlineLocalRefDepth {
		return 0, 0, false
	}
	clauseMap, isMap := clause.(map[string]any)
	if !isMap {
		return 1, 0, true
	}
	nested, hasNested := clauseMap["allOf"].([]any)
	if !hasNested {
		return 1, 0, true
	}
	if len(clauseMap) > 1 {
		count++
		splitEntries += len(clauseMap) - 1
	}
	for _, child := range nested {
		childCount, childEntries, childOK := countFlattenedAllOfClause(child, depth+1)
		if !childOK {
			return 0, 0, false
		}
		count += childCount
		splitEntries += childEntries
	}
	return count, splitEntries, true
}

func appendFlattenedAllOfClause(out *[]any, clause any, depth int) {
	clauseMap, isMap := clause.(map[string]any)
	if !isMap {
		*out = append(*out, clause)
		return
	}
	nested, hasNested := clauseMap["allOf"].([]any)
	if !hasNested {
		*out = append(*out, clause)
		return
	}
	if len(clauseMap) > 1 {
		siblings := make(map[string]any, len(clauseMap)-1)
		for key, value := range clauseMap {
			if key != "allOf" {
				siblings[key] = value
			}
		}
		*out = append(*out, siblings)
	}
	for _, child := range nested {
		appendFlattenedAllOfClause(out, child, depth+1)
	}
}

func (r *localRefResolver) appendConjunctionConflictHint(schema map[string]any, key string, base, sibling any) bool {
	hint := "Conjunction " + boundedHintText(key, 64) + ": " + boundedSchemaValueHint(base) + " AND " + boundedSchemaValueHint(sibling)
	if !r.reserveBytes(jsonStringEncodedLen(hint) + jsonStringEncodedLen("description") + 2) {
		return false
	}
	description, _ := schema["description"].(string)
	schema["description"] = mergeHint(description, hint)
	return true
}

func boundedSchemaValueHint(value any) string {
	switch typed := value.(type) {
	case nil:
		return "null"
	case bool:
		return strconv.FormatBool(typed)
	case string:
		return strconv.Quote(boundedHintText(typed, 64))
	case json.Number:
		return boundedHintText(typed.String(), 64)
	case []any:
		return "array(" + strconv.Itoa(len(typed)) + ")"
	case map[string]any:
		return "object(" + strconv.Itoa(len(typed)) + ")"
	default:
		return "value"
	}
}

func boundedHintText(value string, maxBytes int) string {
	if len(value) <= maxBytes {
		return value
	}
	end := maxBytes
	for end > 0 && !utf8.ValidString(value[:end]) {
		end--
	}
	return value[:end] + "..."
}

func (r *localRefResolver) stableRequiredUnion(base, sibling []any) ([]any, bool) {
	if !r.reserveAllocation(len(base)*2 + len(sibling)*2) {
		return nil, false
	}
	out := make([]any, 0, len(base)+len(sibling))
	seen := make(map[string]struct{}, len(base)+len(sibling))
	appendRequired := func(values []any) {
		for _, required := range values {
			name, ok := required.(string)
			if !ok {
				out = append(out, required)
				continue
			}
			if _, exists := seen[name]; exists {
				continue
			}
			seen[name] = struct{}{}
			out = append(out, name)
		}
	}
	appendRequired(base)
	appendRequired(sibling)
	return out, true
}

func (r *localRefResolver) reserveNode() bool {
	if r == nil {
		return true
	}
	r.nodes++
	return r.nodes <= r.maxNodes
}

func (r *localRefResolver) preflightContainerLength(items int) bool {
	if r == nil {
		return true
	}
	return items >= 0 && items <= r.maxNodes-r.nodes && items <= r.maxAllocItems-r.allocItems
}

func (r *localRefResolver) commitContainer(items, structuralBytes int) bool {
	return r.reserveAllocation(items) && r.reserveBytes(structuralBytes)
}

func (r *localRefResolver) reserveAllocation(items int) bool {
	if r == nil {
		return true
	}
	if items < 0 || items > r.maxAllocItems-r.allocItems {
		return false
	}
	r.allocItems += items
	return true
}

func (r *localRefResolver) reserveBytes(size int) bool {
	if r == nil {
		return true
	}
	if size < 0 || size > r.maxBytes-r.estimatedBytes {
		return false
	}
	r.estimatedBytes += size
	return true
}

func (r *localRefResolver) typedLocalRefHint(target any, ref string) (map[string]any, bool) {
	typeName := boundedSchemaType(target)
	description := "See: " + boundedRefName(ref)
	structuralBytes := 2 + jsonStringEncodedLen("type") + 1 + jsonStringEncodedLen(typeName) + 1 +
		jsonStringEncodedLen("description") + 1 + jsonStringEncodedLen(description)
	if !r.preflightContainerLength(2) || !r.commitContainer(2, structuralBytes) || !r.reserveNode() || !r.reserveBytes(scalarJSONSize(typeName)) ||
		!r.reserveNode() || !r.reserveBytes(scalarJSONSize(description)) {
		return nil, false
	}
	return map[string]any{"type": typeName, "description": description}, true
}

func (r *localRefResolver) intersectSchemaEnums(base, sibling []any) ([]any, bool) {
	if !r.reserveAllocation(len(base)) {
		return nil, false
	}
	out := make([]any, 0, len(base))
	for _, baseValue := range base {
		for _, siblingValue := range sibling {
			if schemaValuesEqual(baseValue, siblingValue) {
				out = append(out, baseValue)
				break
			}
		}
	}
	return out, true
}

func (r *localRefResolver) appendAllOfConstraint(schema map[string]any, key string, value any) bool {
	allOf, _ := schema["allOf"].([]any)
	if !r.reserveAllocation(2) || !r.reserveBytes(jsonStringEncodedLen("allOf")+jsonStringEncodedLen(key)+16) {
		return false
	}
	constraint := make(map[string]any, 1)
	constraint[key] = value
	allOf = append(allOf, constraint)
	schema["allOf"] = allOf
	return true
}

func (r *localRefResolver) impossibleSchema() (map[string]any, bool) {
	structuralBytes := 2 + jsonStringEncodedLen("enum") + 1 + 2
	if !r.reserveNode() || !r.preflightContainerLength(1) || !r.commitContainer(1, structuralBytes) {
		return nil, false
	}
	return map[string]any{"enum": []any{}}, true
}

func markImpossibleSchema(schema map[string]any) {
	delete(schema, "const")
	schema["enum"] = []any{}
}

func stricterNumericConstraint(base, sibling any, chooseGreater bool) (any, bool) {
	baseNumber, baseOK := base.(json.Number)
	siblingNumber, siblingOK := sibling.(json.Number)
	if !baseOK || !siblingOK {
		return nil, false
	}
	comparison, ok := compareJSONNumbers(baseNumber, siblingNumber)
	if !ok {
		return nil, false
	}
	if (chooseGreater && comparison < 0) || (!chooseGreater && comparison > 0) {
		return sibling, true
	}
	return base, true
}

func compareJSONNumbers(left, right json.Number) (int, bool) {
	leftRaw := left.String()
	rightRaw := right.String()
	if !preflightJSONNumber(leftRaw) || !preflightJSONNumber(rightRaw) {
		return 0, false
	}
	leftRat, leftOK := new(big.Rat).SetString(leftRaw)
	rightRat, rightOK := new(big.Rat).SetString(rightRaw)
	if !leftOK || !rightOK {
		return 0, false
	}
	return leftRat.Cmp(rightRat), true
}

func preflightJSONNumber(raw string) bool {
	const (
		maxLiteralBytes      = 128
		maxSignificantDigits = 128
		maxExponentMagnitude = 1024
		maxExpandedDigits    = 1024
	)
	if raw == "" || len(raw) > maxLiteralBytes {
		return false
	}

	exponentIndex := strings.IndexAny(raw, "eE")
	mantissa := raw
	exponent := 0
	if exponentIndex >= 0 {
		mantissa = raw[:exponentIndex]
		exponentRaw := raw[exponentIndex+1:]
		if exponentRaw == "" {
			return false
		}
		sign := 1
		if exponentRaw[0] == '+' || exponentRaw[0] == '-' {
			if exponentRaw[0] == '-' {
				sign = -1
			}
			exponentRaw = exponentRaw[1:]
		}
		if exponentRaw == "" {
			return false
		}
		for _, digit := range exponentRaw {
			if digit < '0' || digit > '9' {
				return false
			}
			if exponent > maxExponentMagnitude {
				return false
			}
			exponent = exponent*10 + int(digit-'0')
		}
		if exponent > maxExponentMagnitude {
			return false
		}
		exponent *= sign
	}

	digits := 0
	for i, char := range mantissa {
		if (char == '-' && i == 0) || char == '.' {
			continue
		}
		if char < '0' || char > '9' {
			return false
		}
		digits++
	}
	if digits == 0 || digits > maxSignificantDigits {
		return false
	}
	if exponent < 0 {
		exponent = -exponent
	}
	return digits+exponent <= maxExpandedDigits
}

func schemaValuesEqual(left, right any) bool {
	switch leftValue := left.(type) {
	case nil:
		return right == nil
	case bool:
		rightValue, ok := right.(bool)
		return ok && leftValue == rightValue
	case string:
		rightValue, ok := right.(string)
		return ok && leftValue == rightValue
	case json.Number:
		rightValue, ok := right.(json.Number)
		return ok && leftValue.String() == rightValue.String()
	case []any:
		rightValue, ok := right.([]any)
		if !ok || len(leftValue) != len(rightValue) {
			return false
		}
		for i := range leftValue {
			if !schemaValuesEqual(leftValue[i], rightValue[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		rightValue, ok := right.(map[string]any)
		if !ok || len(leftValue) != len(rightValue) {
			return false
		}
		for key, value := range leftValue {
			rightEntry, exists := rightValue[key]
			if !exists || !schemaValuesEqual(value, rightEntry) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func retainedMapEntries(node map[string]any, skipRef bool) int {
	count := len(node)
	if _, exists := node["$defs"]; exists {
		count--
	}
	if _, exists := node["definitions"]; exists {
		count--
	}
	if skipRef {
		if _, exists := node["$ref"]; exists {
			count--
		}
	}
	return count
}

func objectStructuralBytes(node map[string]any, skipRef bool) int {
	size := 2
	entries := 0
	for key := range node {
		if isLocalDefinitionContainer(key) || (skipRef && key == "$ref") {
			continue
		}
		if entries > 0 {
			size++
		}
		size += jsonStringEncodedLen(key) + 1
		entries++
	}
	return size
}

func arrayStructuralBytes(length int) int {
	if length <= 0 {
		return 2
	}
	return length + 1
}

func scalarJSONSize(value any) int {
	switch scalar := value.(type) {
	case nil:
		return 4
	case bool:
		if scalar {
			return 4
		}
		return 5
	case string:
		return jsonStringEncodedLen(scalar)
	case json.Number:
		return len(scalar.String())
	default:
		return maxInlineLocalRefOutputBytes + 1
	}
}

func jsonStringEncodedLen(value string) int {
	// encoding/json escapes control bytes, HTML-sensitive ASCII, and U+2028/U+2029. Counting
	// without constructing the quoted form keeps the byte budget ahead of large allocations.
	size := 2
	for i := 0; i < len(value); {
		char := value[i]
		if char < 0x80 {
			switch char {
			case '\\', '"', '\n', '\r', '\t', '\b', '\f':
				size += 2
			case '<', '>', '&':
				size += 6
			default:
				if char < 0x20 {
					size += 6
				} else {
					size++
				}
			}
			i++
			continue
		}
		runeValue, width := utf8.DecodeRuneInString(value[i:])
		if width == 1 {
			size += 6
		} else if runeValue == '\u2028' || runeValue == '\u2029' {
			size += 6
		} else {
			size += width
		}
		i += width
	}
	return size
}

func boundedSchemaType(target any) string {
	if targetMap, ok := target.(map[string]any); ok {
		if typeName, okType := targetMap["type"].(string); okType {
			switch typeName {
			case "array", "boolean", "integer", "number", "object", "string":
				return typeName
			}
		}
	}
	return "object"
}

func boundedRefName(ref string) string {
	name := refName(ref)
	if len(name) > 128 {
		return "local schema"
	}
	return name
}

func resolveJSONPointer(root any, ref string) (any, bool) {
	current := root
	for _, rawPart := range strings.Split(strings.TrimPrefix(ref, "#/"), "/") {
		part := strings.ReplaceAll(strings.ReplaceAll(rawPart, "~1", "/"), "~0", "~")
		switch node := current.(type) {
		case map[string]any:
			var ok bool
			current, ok = node[part]
			if !ok {
				return nil, false
			}
		case []any:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(node) {
				return nil, false
			}
			current = node[index]
		default:
			return nil, false
		}
	}
	return current, true
}

func refName(ref string) string {
	if index := strings.LastIndex(ref, "/"); index >= 0 && index+1 < len(ref) {
		return strings.ReplaceAll(strings.ReplaceAll(ref[index+1:], "~1", "/"), "~0", "~")
	}
	return ref
}

// convertRefsToHints retains sibling keywords and converts only unresolved or external references
// to descriptions. Local references have already been expanded by inlineLocalRefs.
func convertRefsToHints(jsonStr string, preserveSiblings bool) string {
	paths := findPaths(jsonStr, "$ref")
	sortByDepth(paths)

	for _, p := range paths {
		refVal := gjson.Get(jsonStr, p).String()
		defName := refName(refVal)

		parentPath := trimSuffix(p, ".$ref")
		hint := fmt.Sprintf("See: %s", defName)
		if !preserveSiblings {
			if existing := gjson.Get(jsonStr, descriptionPath(parentPath)).String(); existing != "" {
				hint = fmt.Sprintf("%s (%s)", existing, hint)
			}
			replacement := `{"type":"object","description":""}`
			replacementBytes, _ := sjson.SetBytes([]byte(replacement), "description", hint)
			jsonStr = setRawAt(jsonStr, parentPath, string(replacementBytes))
			continue
		}
		jsonStr, _ = sjson.Delete(jsonStr, p)
		jsonStr = appendHint(jsonStr, parentPath, hint)
	}
	return jsonStr
}

func convertConstToEnum(jsonStr string) string {
	for _, p := range findPaths(jsonStr, "const") {
		val := gjson.Get(jsonStr, p)
		if !val.Exists() {
			continue
		}
		enumPath := trimSuffix(p, ".const") + ".enum"
		if !gjson.Get(jsonStr, enumPath).Exists() {
			updated, _ := sjson.SetBytes([]byte(jsonStr), enumPath, []interface{}{val.Value()})
			jsonStr = string(updated)
		}
	}
	return jsonStr
}

// convertEnumValuesToStrings ensures all enum values use the string representation required by
// Gemini's proto schema. The declared type remains independent: Antigravity uses it to choose the
// emitted JSON type on both response and function-argument paths.
func convertEnumValuesToStrings(jsonStr string, forceStringType bool) string {
	for _, p := range findPaths(jsonStr, "enum") {
		arr := gjson.Get(jsonStr, p)
		if !arr.IsArray() {
			continue
		}

		items := arr.Array()
		stringVals := make([]string, 0, len(items))
		for _, item := range items {
			stringVals = append(stringVals, item.String())
		}

		updated, _ := sjson.SetBytes([]byte(jsonStr), p, stringVals)
		jsonStr = string(updated)
		if forceStringType {
			parentPath := trimSuffix(p, ".enum")
			updated, _ = sjson.SetBytes([]byte(jsonStr), joinPath(parentPath, "type"), "string")
			jsonStr = string(updated)
		}
	}
	return jsonStr
}

func addEnumHints(jsonStr string) string {
	for _, p := range findPaths(jsonStr, "enum") {
		arr := gjson.Get(jsonStr, p)
		if !arr.IsArray() {
			continue
		}
		items := arr.Array()
		if len(items) <= 1 || len(items) > 10 {
			continue
		}

		var vals []string
		for _, item := range items {
			vals = append(vals, item.String())
		}
		jsonStr = appendHint(jsonStr, trimSuffix(p, ".enum"), "Allowed: "+strings.Join(vals, ", "))
	}
	return jsonStr
}

// Antigravity does not enforce enum on function arguments and ignores boolean response enums.
// Preserve the advisory values in description, but do not leave an unenforced constraint in the
// schema contract. Response enums for string, number, and integer remain native constraints.
func dropIgnoredEnumsToHints(jsonStr string, options jsonSchemaCleanOptions) string {
	for _, path := range findPaths(jsonStr, "enum") {
		parentPath := trimSuffix(path, ".enum")
		shouldDrop := options.dropAllEnums || (options.dropBooleanEnums && gjson.Get(jsonStr, joinPath(parentPath, "type")).String() == "boolean")
		if !shouldDrop {
			continue
		}
		enum := gjson.Get(jsonStr, path)
		if enum.IsArray() && len(enum.Array()) == 1 {
			jsonStr = appendHint(jsonStr, parentPath, "Allowed: "+enum.Array()[0].String())
		}
		jsonStr, _ = sjson.Delete(jsonStr, path)
	}
	return jsonStr
}

func addAdditionalPropertiesHints(jsonStr string) string {
	for _, p := range findPaths(jsonStr, "additionalProperties") {
		if gjson.Get(jsonStr, p).Type == gjson.False {
			jsonStr = appendHint(jsonStr, trimSuffix(p, ".additionalProperties"), "No extra properties allowed")
		}
	}
	return jsonStr
}

var unsupportedConstraints = []string{
	"minLength", "maxLength", "exclusiveMinimum", "exclusiveMaximum",
	"pattern", "minItems", "maxItems", "uniqueItems", "format",
	"default", "examples", // Claude rejects these in VALIDATED mode
}

func constraintKeywords(options jsonSchemaCleanOptions) []string {
	keywords := append([]string(nil), unsupportedConstraints...)
	if options.antigravitySemantics {
		keywords = append(keywords, "minimum", "maximum", "multipleOf")
	}
	return keywords
}

func moveConstraintsToDescription(jsonStr string, options jsonSchemaCleanOptions) string {
	constraints := constraintKeywords(options)
	pathsByField := findPathsByFields(jsonStr, constraints)
	for _, key := range constraints {
		for _, p := range pathsByField[key] {
			val := gjson.Get(jsonStr, p)
			if !val.Exists() || val.IsObject() || val.IsArray() {
				continue
			}
			parentPath := trimSuffix(p, "."+key)
			if isPropertyDefinition(parentPath) {
				continue
			}
			jsonStr = appendHint(jsonStr, parentPath, fmt.Sprintf("%s: %s", key, val.String()))
		}
	}
	return jsonStr
}

func moveNotToDescription(jsonStr string) string {
	for _, path := range findPaths(jsonStr, "not") {
		value := gjson.Get(jsonStr, path)
		if !value.Exists() || isPropertyDefinition(trimSuffix(path, ".not")) {
			continue
		}
		jsonStr = appendHint(jsonStr, trimSuffix(path, ".not"), "not: "+value.Raw)
	}
	return jsonStr
}

func mergeConditionals(jsonStr string) string {
	pathsByField := findPathsByFields(jsonStr, []string{"then", "else"})
	var paths []string
	for _, key := range []string{"then", "else"} {
		for _, p := range pathsByField[key] {
			parentPath := trimSuffix(p, "."+key)
			if isPropertyDefinition(parentPath) {
				continue
			}
			paths = append(paths, p)
		}
	}
	sortByDepth(paths)

	for _, p := range paths {
		props := gjson.Get(jsonStr, joinPath(p, "properties"))
		if !props.IsObject() {
			continue
		}
		var parentPath string
		if strings.HasSuffix(p, ".then") {
			parentPath = trimSuffix(p, ".then")
		} else if strings.HasSuffix(p, ".else") {
			parentPath = trimSuffix(p, ".else")
		} else if p == "then" || p == "else" {
			parentPath = ""
		} else {
			continue
		}

		props.ForEach(func(key, value gjson.Result) bool {
			destPath := joinPath(parentPath, "properties."+escapeGJSONPathKey(key.String()))
			if !gjson.Get(jsonStr, destPath).Exists() {
				updated, _ := sjson.SetRawBytes([]byte(jsonStr), destPath, []byte(value.Raw))
				jsonStr = string(updated)
			}
			return true
		})
	}
	return jsonStr
}

func mergeAllOf(jsonStr string) string {
	paths := findPaths(jsonStr, "allOf")
	sortByDepth(paths)

	for _, p := range paths {
		allOf := gjson.Get(jsonStr, p)
		if !allOf.IsArray() {
			continue
		}
		parentPath := trimSuffix(p, ".allOf")

		for _, item := range allOf.Array() {
			if !item.IsObject() {
				continue
			}
			item.ForEach(func(key, value gjson.Result) bool {
				field := key.String()
				switch field {
				case "required":
					if !value.IsArray() {
						return true
					}
					reqPath := joinPath(parentPath, "required")
					current := getStrings(jsonStr, reqPath)
					for _, required := range value.Array() {
						if name := required.String(); !contains(current, name) {
							current = append(current, name)
						}
					}
					updated, _ := sjson.SetBytes([]byte(jsonStr), reqPath, current)
					jsonStr = string(updated)
				case "if", "then", "else", "allOf":
					// Conditional applicability cannot be represented by the upstream schema.
				case "description":
					destination := descriptionPath(parentPath)
					merged := mergeHint(gjson.Get(jsonStr, destination).String(), value.String())
					updated, _ := sjson.SetBytes([]byte(jsonStr), destination, merged)
					jsonStr = string(updated)
				default:
					destination := joinPath(parentPath, escapeGJSONPathKey(field))
					jsonStr = mergeMissingSchemaAtPath(jsonStr, destination, value)
				}
				return true
			})
		}
		jsonStr, _ = sjson.Delete(jsonStr, p)
	}
	return jsonStr
}

// mergeMissingSchemaAtPath recursively fills absent fields without replacing any existing
// definition. Conflicting allOf leaves remain visible as bounded conjunction hints.
func mergeMissingSchemaAtPath(jsonStr, destination string, incoming gjson.Result) string {
	existing := gjson.Get(jsonStr, destination)
	if !existing.Exists() {
		updated, _ := sjson.SetRawBytes([]byte(jsonStr), destination, []byte(incoming.Raw))
		return string(updated)
	}
	if !existing.IsObject() || !incoming.IsObject() {
		if existing.Raw != incoming.Raw {
			parts := splitGJSONPath(destination)
			if len(parts) > 0 {
				field := unescapeGJSONPathKey(parts[len(parts)-1])
				parentPath := strings.Join(parts[:len(parts)-1], ".")
				hint := "Conjunction " + boundedHintText(field, 64) + ": " + boundedGJSONValueHint(existing) + " AND " + boundedGJSONValueHint(incoming)
				jsonStr = appendHint(jsonStr, parentPath, hint)
			}
		}
		return jsonStr
	}
	incoming.ForEach(func(key, value gjson.Result) bool {
		child := joinPath(destination, escapeGJSONPathKey(key.String()))
		jsonStr = mergeMissingSchemaAtPath(jsonStr, child, value)
		return true
	})
	return jsonStr
}

func boundedGJSONValueHint(value gjson.Result) string {
	switch value.Type {
	case gjson.Null:
		return "null"
	case gjson.False:
		return "false"
	case gjson.True:
		return "true"
	case gjson.Number:
		return boundedHintText(value.Raw, 64)
	case gjson.String:
		return strconv.Quote(boundedHintText(value.String(), 64))
	case gjson.JSON:
		trimmed := strings.TrimSpace(value.Raw)
		if strings.HasPrefix(trimmed, "[") {
			return "array"
		}
		return "object"
	default:
		return "value"
	}
}

func flattenAnyOfOneOf(jsonStr string) string {
	for _, key := range []string{"anyOf", "oneOf"} {
		paths := findPaths(jsonStr, key)
		sortByDepth(paths)

		for _, p := range paths {
			arr := gjson.Get(jsonStr, p)
			if !arr.IsArray() || len(arr.Array()) == 0 {
				continue
			}

			parentPath := trimSuffix(p, "."+key)
			parentDesc := gjson.Get(jsonStr, descriptionPath(parentPath)).String()

			items := arr.Array()
			bestIdx, allTypes := selectBest(items)
			selected := items[bestIdx].Raw
			hasNull := false
			for _, item := range items {
				if item.Get("type").String() == "null" {
					hasNull = true
					break
				}
			}
			if hasNull && items[bestIdx].Get("type").String() != "null" {
				updated, _ := sjson.SetBytes([]byte(selected), "nullable", true)
				selected = string(updated)
			}

			if parentDesc != "" {
				selected = mergeDescriptionRaw(selected, parentDesc)
			}

			if len(allTypes) > 1 {
				hint := "Accepts: " + strings.Join(allTypes, " | ")
				selected = appendHintRaw(selected, hint)
			}

			jsonStr = setRawAt(jsonStr, parentPath, selected)
		}
	}
	return jsonStr
}

func selectBest(items []gjson.Result) (bestIdx int, types []string) {
	bestScore := -1
	for i, item := range items {
		t := item.Get("type").String()
		score := 0

		switch {
		case t == "object" || item.Get("properties").Exists():
			score, t = 3, orDefault(t, "object")
		case t == "array" || item.Get("items").Exists():
			score, t = 2, orDefault(t, "array")
		case t != "" && t != "null":
			score = 1
		default:
			t = orDefault(t, "null")
		}

		if t != "" {
			types = append(types, t)
		}
		if score > bestScore {
			bestScore, bestIdx = score, i
		}
	}
	return
}

func flattenTypeArrays(jsonStr string, preserveNativeNullable bool) string {
	paths := findPaths(jsonStr, "type")
	sortByDepth(paths)

	nullableFields := make(map[string][]string)

	for _, p := range paths {
		res := gjson.Get(jsonStr, p)
		if !res.IsArray() || len(res.Array()) == 0 {
			continue
		}

		hasNull := false
		var nonNullTypes []string
		for _, item := range res.Array() {
			s := item.String()
			if s == "null" {
				hasNull = true
			} else if s != "" {
				nonNullTypes = append(nonNullTypes, s)
			}
		}

		firstType := "string"
		if len(nonNullTypes) > 0 {
			firstType = nonNullTypes[0]
		}

		updated, _ := sjson.SetBytes([]byte(jsonStr), p, firstType)
		jsonStr = string(updated)

		parentPath := trimSuffix(p, ".type")
		if len(nonNullTypes) > 1 {
			hint := "Accepts: " + strings.Join(nonNullTypes, " | ")
			jsonStr = appendHint(jsonStr, parentPath, hint)
		}

		if hasNull {
			if preserveNativeNullable {
				updated, _ = sjson.SetBytes([]byte(jsonStr), joinPath(parentPath, "nullable"), true)
				jsonStr = string(updated)
				jsonStr = appendHint(jsonStr, parentPath, "(nullable)")
				continue
			}

			parts := splitGJSONPath(p)
			if len(parts) >= 3 && parts[len(parts)-3] == "properties" {
				fieldNameEscaped := parts[len(parts)-2]
				fieldName := unescapeGJSONPathKey(fieldNameEscaped)
				objectPath := strings.Join(parts[:len(parts)-3], ".")
				nullableFields[objectPath] = append(nullableFields[objectPath], fieldName)
				jsonStr = appendHint(jsonStr, joinPath(objectPath, "properties."+fieldNameEscaped), "(nullable)")
			}
		}
	}

	for objectPath, fields := range nullableFields {
		reqPath := joinPath(objectPath, "required")
		req := gjson.Get(jsonStr, reqPath)
		if !req.IsArray() {
			continue
		}

		var filtered []string
		for _, required := range req.Array() {
			if !contains(fields, required.String()) {
				filtered = append(filtered, required.String())
			}
		}
		if len(filtered) == 0 {
			jsonStr, _ = sjson.Delete(jsonStr, reqPath)
		} else {
			updated, _ := sjson.SetBytes([]byte(jsonStr), reqPath, filtered)
			jsonStr = string(updated)
		}
	}
	return jsonStr
}

func removeUnsupportedKeywords(jsonStr string, options jsonSchemaCleanOptions) string {
	keywords := append(constraintKeywords(options),
		"$schema", "$defs", "definitions", "const", "$ref", "$id", "additionalProperties",
		"propertyNames", "patternProperties", // Gemini doesn't support these schema keywords
		"if", "then", "else",
		"$comment", "enumDescriptions", "enumTitles", "prefill", "deprecated", // Schema metadata fields unsupported by Gemini
	)
	if options.antigravitySemantics {
		keywords = append(keywords, "not")
	}

	deletePaths := make([]string, 0)
	pathsByField := findPathsByFields(jsonStr, keywords)
	for _, key := range keywords {
		for _, p := range pathsByField[key] {
			if isPropertyDefinition(trimSuffix(p, "."+key)) {
				continue
			}
			if options.preserveAdditionalPropertiesFalse && key == "additionalProperties" {
				if gjson.Get(jsonStr, p).Type == gjson.False {
					continue
				}
			}
			deletePaths = append(deletePaths, p)
		}
	}
	sortByDepth(deletePaths)
	for _, p := range deletePaths {
		jsonStr, _ = sjson.Delete(jsonStr, p)
	}
	// Remove x-* extension fields (e.g., x-google-enum-descriptions) that are not supported by Gemini API
	jsonStr = removeExtensionFields(jsonStr)
	return jsonStr
}

// removeExtensionFields removes all x-* extension fields from the JSON schema.
// These are OpenAPI/JSON Schema extension fields that Google APIs don't recognize.
func removeExtensionFields(jsonStr string) string {
	var paths []string
	walkForExtensions(gjson.Parse(jsonStr), "", &paths)
	// walkForExtensions returns paths in a way that deeper paths are added before their ancestors
	// when they are not deleted wholesale, but since we skip children of deleted x-* nodes,
	// any collected path is safe to delete. We still use DeleteBytes for efficiency.

	b := []byte(jsonStr)
	for _, p := range paths {
		b, _ = sjson.DeleteBytes(b, p)
	}
	return string(b)
}

func walkForExtensions(value gjson.Result, path string, paths *[]string) {
	if value.IsArray() {
		arr := value.Array()
		for i := len(arr) - 1; i >= 0; i-- {
			walkForExtensions(arr[i], joinPath(path, strconv.Itoa(i)), paths)
		}
		return
	}

	if value.IsObject() {
		value.ForEach(func(key, val gjson.Result) bool {
			keyStr := key.String()
			safeKey := escapeGJSONPathKey(keyStr)
			childPath := joinPath(path, safeKey)

			// If it's an extension field, we delete it and don't need to look at its children.
			if strings.HasPrefix(keyStr, "x-") && !isPropertyDefinition(path) {
				*paths = append(*paths, childPath)
				return true
			}

			walkForExtensions(val, childPath, paths)
			return true
		})
	}
}

func cleanupRequiredFields(jsonStr string) string {
	for _, p := range findPaths(jsonStr, "required") {
		parentPath := trimSuffix(p, ".required")
		propsPath := joinPath(parentPath, "properties")

		req := gjson.Get(jsonStr, p)
		props := gjson.Get(jsonStr, propsPath)
		if !req.IsArray() || !props.IsObject() {
			continue
		}

		var valid []string
		for _, r := range req.Array() {
			key := r.String()
			if props.Get(escapeGJSONPathKey(key)).Exists() {
				valid = append(valid, key)
			}
		}

		if len(valid) != len(req.Array()) {
			if len(valid) == 0 {
				jsonStr, _ = sjson.Delete(jsonStr, p)
			} else {
				updated, _ := sjson.SetBytes([]byte(jsonStr), p, valid)
				jsonStr = string(updated)
			}
		}
	}
	return jsonStr
}

// addEmptySchemaPlaceholder adds a placeholder "reason" property to empty object schemas.
// Claude VALIDATED mode requires at least one required property in tool schemas.
func addEmptySchemaPlaceholder(jsonStr string) string {
	// Find all "type" fields
	paths := findPaths(jsonStr, "type")

	// Process from deepest to shallowest (to handle nested objects properly)
	sortByDepth(paths)

	for _, p := range paths {
		typeVal := gjson.Get(jsonStr, p)
		if typeVal.String() != "object" {
			continue
		}

		// Get the parent path (the object containing "type")
		parentPath := trimSuffix(p, ".type")

		// Check if properties exists and is empty or missing
		propsPath := joinPath(parentPath, "properties")
		propsVal := gjson.Get(jsonStr, propsPath)
		reqPath := joinPath(parentPath, "required")
		reqVal := gjson.Get(jsonStr, reqPath)
		hasRequiredProperties := reqVal.IsArray() && len(reqVal.Array()) > 0

		needsPlaceholder := false
		if !propsVal.Exists() {
			// No properties field at all
			needsPlaceholder = true
		} else if propsVal.IsObject() && len(propsVal.Map()) == 0 {
			// Empty properties object
			needsPlaceholder = true
		}

		if needsPlaceholder {
			// Add placeholder "reason" property
			reasonPath := joinPath(propsPath, "reason")
			updated, _ := sjson.SetBytes([]byte(jsonStr), reasonPath+".type", "string")
			jsonStr = string(updated)
			updated, _ = sjson.SetBytes([]byte(jsonStr), reasonPath+".description", placeholderReasonDescription)
			jsonStr = string(updated)

			// Add to required array
			updated, _ = sjson.SetBytes([]byte(jsonStr), reqPath, []string{"reason"})
			jsonStr = string(updated)
			continue
		}

		// If schema has properties but none are required, add a minimal placeholder.
		if propsVal.IsObject() && !hasRequiredProperties {
			// DO NOT add placeholder if it's a top-level schema (parentPath is empty)
			// or if we've already added a placeholder reason above.
			if parentPath == "" {
				continue
			}
			placeholderPath := joinPath(propsPath, "_")
			if !gjson.Get(jsonStr, placeholderPath).Exists() {
				updated, _ := sjson.SetBytes([]byte(jsonStr), placeholderPath+".type", "boolean")
				jsonStr = string(updated)
			}
			updated, _ := sjson.SetBytes([]byte(jsonStr), reqPath, []string{"_"})
			jsonStr = string(updated)
		}
	}

	return jsonStr
}

// --- Helpers ---

func findPaths(jsonStr, field string) []string {
	var paths []string
	Walk(gjson.Parse(jsonStr), "", field, &paths)
	return paths
}

func findPathsByFields(jsonStr string, fields []string) map[string][]string {
	set := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		set[field] = struct{}{}
	}
	paths := make(map[string][]string, len(set))
	walkForFields(gjson.Parse(jsonStr), "", set, paths)
	return paths
}

func walkForFields(value gjson.Result, path string, fields map[string]struct{}, paths map[string][]string) {
	switch value.Type {
	case gjson.JSON:
		value.ForEach(func(key, val gjson.Result) bool {
			keyStr := key.String()
			safeKey := escapeGJSONPathKey(keyStr)

			var childPath string
			if path == "" {
				childPath = safeKey
			} else {
				childPath = path + "." + safeKey
			}

			if _, ok := fields[keyStr]; ok {
				paths[keyStr] = append(paths[keyStr], childPath)
			}

			walkForFields(val, childPath, fields, paths)
			return true
		})
	case gjson.String, gjson.Number, gjson.True, gjson.False, gjson.Null:
		// Terminal types - no further traversal needed
	}
}

func sortByDepth(paths []string) {
	sort.SliceStable(paths, func(i, j int) bool {
		return len(splitGJSONPath(paths[i])) > len(splitGJSONPath(paths[j]))
	})
}

func trimSuffix(path, suffix string) string {
	if path == strings.TrimPrefix(suffix, ".") {
		return ""
	}
	return strings.TrimSuffix(path, suffix)
}

func joinPath(base, suffix string) string {
	if base == "" {
		return suffix
	}
	return base + "." + suffix
}

func setRawAt(jsonStr, path, value string) string {
	if path == "" {
		return value
	}
	result, _ := sjson.SetRawBytes([]byte(jsonStr), path, []byte(value))
	return string(result)
}

// schemaNameMapKeywords are the schema keywords whose value maps author-chosen names to
// subschemas. A key directly under one of them is a name, never a schema keyword.
var schemaNameMapKeywords = map[string]struct{}{
	"properties":        {},
	"patternProperties": {},
	"dependentSchemas":  {},
	"$defs":             {},
	"definitions":       {},
}

// isPropertyDefinition reports whether path points at a map whose keys are names chosen by the
// tool author, so a key spelled like a schema keyword there must be preserved.
//
// A trailing ".properties" is not enough to tell: a tool may declare a property named
// "properties", and the schema for that property then sits at a path ending in ".properties" while
// being an ordinary schema node. Classifying it as a name map skipped every cleaning pass inside
// it, so unsupported keywords such as "propertyNames" reached the private Gemini backend, which
// rejects unknown fields with a 400.
//
// Each name-map keyword at the end of the path therefore flips the answer, because the node it
// names is a map only when its own parent is a schema: "properties" is a map,
// "properties.properties" the schema of a property named "properties", and
// "properties.properties.properties" that schema's own map. Only the trailing run matters, so any
// prefix the caller nests the schema under is ignored.
func isPropertyDefinition(path string) bool {
	segments := splitGJSONPath(path)
	trailing := 0
	for i := len(segments) - 1; i >= 0; i-- {
		if _, ok := schemaNameMapKeywords[unescapeGJSONPathKey(segments[i])]; !ok {
			break
		}
		trailing++
	}
	return trailing%2 == 1
}

func descriptionPath(parentPath string) string {
	if parentPath == "" || parentPath == "@this" {
		return "description"
	}
	return parentPath + ".description"
}

// mergeHint combines an existing description with a hint. Cleaning is not always a single pass:
// a schema may be cleaned by a translator and again by an executor, so an already-present hint is
// kept as-is instead of being appended a second time.
func mergeHint(existing, hint string) string {
	if existing == "" {
		return hint
	}
	// A hint added to an empty description is stored bare and later hints are appended after it, so
	// the bare form may sit alone, lead the description, or appear parenthesised further along.
	if existing == hint ||
		strings.HasPrefix(existing, hint+" (") ||
		strings.Contains(existing, fmt.Sprintf("(%s)", hint)) {
		return existing
	}
	return fmt.Sprintf("%s (%s)", existing, hint)
}

func appendHint(jsonStr, parentPath, hint string) string {
	descPath := parentPath + ".description"
	if parentPath == "" || parentPath == "@this" {
		descPath = "description"
	}
	merged := mergeHint(gjson.Get(jsonStr, descPath).String(), hint)
	updated, _ := sjson.SetBytes([]byte(jsonStr), descPath, merged)
	jsonStr = string(updated)
	return jsonStr
}

func appendHintRaw(jsonRaw, hint string) string {
	merged := mergeHint(gjson.Get(jsonRaw, "description").String(), hint)
	updated, _ := sjson.SetBytes([]byte(jsonRaw), "description", merged)
	jsonRaw = string(updated)
	return jsonRaw
}

func getStrings(jsonStr, path string) []string {
	var result []string
	if arr := gjson.Get(jsonStr, path); arr.IsArray() {
		for _, r := range arr.Array() {
			result = append(result, r.String())
		}
	}
	return result
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func orDefault(val, def string) string {
	if val == "" {
		return def
	}
	return val
}

func escapeGJSONPathKey(key string) string {
	if strings.IndexAny(key, ".*?") == -1 {
		return key
	}
	return gjsonPathKeyReplacer.Replace(key)
}

func unescapeGJSONPathKey(key string) string {
	if !strings.Contains(key, "\\") {
		return key
	}
	var b strings.Builder
	b.Grow(len(key))
	for i := 0; i < len(key); i++ {
		if key[i] == '\\' && i+1 < len(key) {
			i++
			b.WriteByte(key[i])
			continue
		}
		b.WriteByte(key[i])
	}
	return b.String()
}

func splitGJSONPath(path string) []string {
	if path == "" {
		return nil
	}

	parts := make([]string, 0, strings.Count(path, ".")+1)
	var b strings.Builder
	b.Grow(len(path))

	for i := 0; i < len(path); i++ {
		c := path[i]
		if c == '\\' && i+1 < len(path) {
			b.WriteByte('\\')
			i++
			b.WriteByte(path[i])
			continue
		}
		if c == '.' {
			parts = append(parts, b.String())
			b.Reset()
			continue
		}
		b.WriteByte(c)
	}
	parts = append(parts, b.String())
	return parts
}

func mergeDescriptionRaw(schemaRaw, parentDesc string) string {
	childDesc := gjson.Get(schemaRaw, "description").String()
	switch {
	case childDesc == "":
		updated, _ := sjson.SetBytes([]byte(schemaRaw), "description", parentDesc)
		return string(updated)
	case childDesc == parentDesc:
		return schemaRaw
	default:
		combined := fmt.Sprintf("%s (%s)", parentDesc, childDesc)
		updated, _ := sjson.SetBytes([]byte(schemaRaw), "description", combined)
		return string(updated)
	}
}
