package claude

import (
	"encoding/json"
	"fmt"
)

// MergeAdjacentSameRole 将连续相同 role 的消息合并为一条。
// Kiro 上游拒绝两条连续 role 相同的消息，因此需要合并。
//
// 合并规则：
//   - 两者均为字符串内容 → "<a>\n<b>"
//   - 两者均为数组内容  → 数组拼接
//   - 混合情况         → 先将双方升级为数组再拼接
func MergeAdjacentSameRole(in []AnthropicMessage) []AnthropicMessage {
	if len(in) <= 1 {
		return in
	}
	out := []AnthropicMessage{in[0]}
	for i := 1; i < len(in); i++ {
		last := &out[len(out)-1]
		cur := in[i]
		if last.Role != cur.Role {
			out = append(out, cur)
			continue
		}
		merged, err := mergeContent(last.Content, cur.Content)
		if err != nil {
			out = append(out, cur)
			continue
		}
		last.Content = merged
	}
	return out
}

func mergeContent(a, b json.RawMessage) (json.RawMessage, error) {
	aIsArr := isJSONArray(a)
	bIsArr := isJSONArray(b)
	switch {
	case !aIsArr && !bIsArr:
		var as, bs string
		if err := json.Unmarshal(a, &as); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(b, &bs); err != nil {
			return nil, err
		}
		return json.Marshal(as + "\n" + bs)
	case aIsArr && bIsArr:
		var arrA, arrB []any
		if err := json.Unmarshal(a, &arrA); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(b, &arrB); err != nil {
			return nil, err
		}
		return json.Marshal(append(arrA, arrB...))
	default:
		arrA, err := promoteToArray(a)
		if err != nil {
			return nil, err
		}
		arrB, err := promoteToArray(b)
		if err != nil {
			return nil, err
		}
		return json.Marshal(append(arrA, arrB...))
	}
}

func isJSONArray(raw json.RawMessage) bool {
	for _, c := range raw {
		switch c {
		case ' ', '\t', '\n', '\r':
			continue
		case '[':
			return true
		default:
			return false
		}
	}
	return false
}

func promoteToArray(raw json.RawMessage) ([]any, error) {
	if isJSONArray(raw) {
		var arr []any
		if err := json.Unmarshal(raw, &arr); err != nil {
			return nil, err
		}
		return arr, nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, fmt.Errorf("kiro/claude: promote: %w", err)
	}
	return []any{map[string]any{"type": "text", "text": s}}, nil
}
