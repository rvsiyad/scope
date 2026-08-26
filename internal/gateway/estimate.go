package gateway

// Cost estimation for admission control. The estimate only has to be good
// enough that a greedy tenant can't blow far past its budget before settle
// trues everything up against the provider's real count — precision is
// settle's job, not admission's.
const (
	// estCharsPerToken is the usual English-text rule of thumb: ~4 characters
	// per token. A real tokenizer would be per-model and per-provider; the
	// error it saves doesn't matter here because settle corrects it.
	estCharsPerToken = 4
	// estMessageOverhead covers per-message framing tokens (role markers,
	// separators) that content length doesn't see.
	estMessageOverhead = 4
	// estDefaultCompletion is assumed when the client doesn't set max_tokens.
	// Admission needs *some* prediction of output length; clients that set
	// max_tokens get charged (up front) exactly what they asked to be allowed.
	estDefaultCompletion = 256
)

// estimateTokens predicts a request's total cost — prompt plus completion —
// before the provider is called.
func estimateTokens(req ChatRequest) int {
	est := estimatePromptTokens(req)
	if req.MaxTokens != nil && *req.MaxTokens > 0 {
		est += *req.MaxTokens
	} else {
		est += estDefaultCompletion
	}
	return est
}

func estimatePromptTokens(req ChatRequest) int {
	n := 0
	for _, m := range req.Messages {
		n += len(m.Content)/estCharsPerToken + estMessageOverhead
	}
	return n
}

// meterText approximates tokens for text that streamed without ever getting
// a real usage count (client disconnected before the final chunk).
func meterText(chars int) int { return chars / estCharsPerToken }
