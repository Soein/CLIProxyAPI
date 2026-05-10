package claude

// MaxToolDescriptionLength 是 Kiro 对工具描述的长度上限（与 AIClient2API 保持一致）。
const MaxToolDescriptionLength = 9216

// TruncateToolDescription 将描述截断到 MaxToolDescriptionLength 个字符，
// 发生截断时追加 "..."，否则原样返回。
func TruncateToolDescription(desc string) string {
	if len(desc) <= MaxToolDescriptionLength {
		return desc
	}
	return desc[:MaxToolDescriptionLength] + "..."
}
