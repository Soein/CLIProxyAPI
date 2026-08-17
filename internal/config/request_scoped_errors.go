package config

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	requestScopedActionStop                = "stop"
	requestScopedActionStopAndCooldown     = "stop-and-cooldown"
	requestScopedActionContinue            = "continue"
	requestScopedActionContinueAndCooldown = "continue-and-cooldown"
)

// ValidateRequestScopedErrorRules validates request-scoped upstream error rules.
func ValidateRequestScopedErrorRules(rules []RequestScopedErrorRule) error {
	return validateRequestScopedErrorRules("request-scoped-errors", rules)
}

// ValidateRequestScopedErrorRules validates request-scoped error rules for every credential family.
func (cfg *Config) ValidateRequestScopedErrorRules() error {
	if cfg == nil {
		return nil
	}
	for index := range cfg.GeminiKey {
		if errValidate := validateRequestScopedErrorRules(fmt.Sprintf("gemini-api-key[%d].request-scoped-errors", index), cfg.GeminiKey[index].RequestScopedErrors); errValidate != nil {
			return errValidate
		}
	}
	for index := range cfg.InteractionsKey {
		if errValidate := validateRequestScopedErrorRules(fmt.Sprintf("interactions-api-key[%d].request-scoped-errors", index), cfg.InteractionsKey[index].RequestScopedErrors); errValidate != nil {
			return errValidate
		}
	}
	for index := range cfg.ClaudeKey {
		if errValidate := validateRequestScopedErrorRules(fmt.Sprintf("claude-api-key[%d].request-scoped-errors", index), cfg.ClaudeKey[index].RequestScopedErrors); errValidate != nil {
			return errValidate
		}
	}
	for index := range cfg.CodexKey {
		if errValidate := validateRequestScopedErrorRules(fmt.Sprintf("codex-api-key[%d].request-scoped-errors", index), cfg.CodexKey[index].RequestScopedErrors); errValidate != nil {
			return errValidate
		}
	}
	for index := range cfg.XAIKey {
		if errValidate := validateRequestScopedErrorRules(fmt.Sprintf("xai-api-key[%d].request-scoped-errors", index), cfg.XAIKey[index].RequestScopedErrors); errValidate != nil {
			return errValidate
		}
	}
	for index := range cfg.OpenAICompatibility {
		if errValidate := validateRequestScopedErrorRules(fmt.Sprintf("openai-compatibility[%d].request-scoped-errors", index), cfg.OpenAICompatibility[index].RequestScopedErrors); errValidate != nil {
			return errValidate
		}
	}
	return nil
}

func validateRequestScopedErrorRules(field string, rules []RequestScopedErrorRule) error {
	for ruleIndex := range rules {
		rule := rules[ruleIndex]
		ruleField := fmt.Sprintf("%s[%d]", field, ruleIndex)
		if rule.Status < 100 || rule.Status > 599 {
			return fmt.Errorf("%s.status must be between 100 and 599", ruleField)
		}

		switch strings.ToLower(strings.TrimSpace(rule.Action)) {
		case requestScopedActionStop,
			requestScopedActionStopAndCooldown,
			requestScopedActionContinue,
			requestScopedActionContinueAndCooldown:
		default:
			return fmt.Errorf("%s.action must be one of stop, stop-and-cooldown, continue, continue-and-cooldown", ruleField)
		}

		hasMatcher := false
		for _, match := range rule.Match {
			if strings.TrimSpace(match) != "" {
				hasMatcher = true
			}
		}
		for regexIndex, pattern := range rule.MatchRegexr {
			if strings.TrimSpace(pattern) == "" {
				continue
			}
			hasMatcher = true
			if _, errCompile := regexp.Compile(pattern); errCompile != nil {
				return fmt.Errorf("%s.match-regexr[%d] must be a valid regular expression", ruleField, regexIndex)
			}
		}
		if !hasMatcher {
			return fmt.Errorf("%s: at least one non-empty matcher is required", ruleField)
		}
	}
	return nil
}
