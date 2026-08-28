package query

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/rvsiyad/scope/internal/tsdb"
)

// A tiny PromQL grammar — just enough for a human at a curl prompt and
// the phase-C dashboards to speak the engine's whole vocabulary:
//
//	expr      = aggregate | fncall | selector
//	aggregate = op "by" "(" names ")" "(" expr ")"
//	          | op "(" expr ")"
//	fncall    = name "(" [number ","] selector "[" duration "]" ")"
//	selector  = name [ "{" matcher { "," matcher } "}" ]
//	matcher   = name ( "=" | "!=" ) quoted-string
//
// e.g. `sum by (tenant) (rate(gateway_tokens_total{tenant!="test"}[5m]))`
// or `quantile_over_time(0.99, gateway_ttft_ms[5m])`.
//
// Hand-rolled recursive descent over a hand-rolled lexer: the grammar is
// small enough that a generator would cost more than it saves, and the
// parser IS the documentation of what the engine speaks. Durations use
// Go's syntax ("90s", "1.5h"), which overlaps PromQL's for the common
// cases.

// Parse turns an expression string into an evaluable Expr.
func Parse(input string) (Expr, error) {
	p := &parser{lex: lex(input)}
	expr, err := p.parseExpr()
	if err != nil {
		return nil, fmt.Errorf("query: parse %q: %w", input, err)
	}
	if tok := p.next(); tok.kind != tokEOF {
		return nil, fmt.Errorf("query: parse %q: unexpected %q after expression", input, tok.text)
	}
	return expr, nil
}

// MustParse is Parse for tests and fixtures; it panics on error.
func MustParse(input string) Expr {
	expr, err := Parse(input)
	if err != nil {
		panic(err)
	}
	return expr
}

type tokKind int

const (
	tokEOF tokKind = iota
	tokIdent
	tokNumber
	tokString  // quoted, already unescaped
	tokLParen  // (
	tokRParen  // )
	tokLBrace  // {
	tokRBrace  // }
	tokLBrack  // [
	tokRBrack  // ]
	tokComma   // ,
	tokEq      // =
	tokNeq     // !=
	tokInvalid // anything else; text carries the offender
)

type token struct {
	kind tokKind
	text string
}

// lex tokenizes the whole input up front — expressions are one line, so
// there is nothing streaming about them.
func lex(input string) []token {
	var toks []token
	i := 0
	for i < len(input) {
		c := input[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n':
			i++
		case c == '(':
			toks, i = append(toks, token{tokLParen, "("}), i+1
		case c == ')':
			toks, i = append(toks, token{tokRParen, ")"}), i+1
		case c == '{':
			toks, i = append(toks, token{tokLBrace, "{"}), i+1
		case c == '}':
			toks, i = append(toks, token{tokRBrace, "}"}), i+1
		case c == '[':
			toks, i = append(toks, token{tokLBrack, "["}), i+1
		case c == ']':
			toks, i = append(toks, token{tokRBrack, "]"}), i+1
		case c == ',':
			toks, i = append(toks, token{tokComma, ","}), i+1
		case c == '=':
			toks, i = append(toks, token{tokEq, "="}), i+1
		case c == '!' && i+1 < len(input) && input[i+1] == '=':
			toks, i = append(toks, token{tokNeq, "!="}), i+2
		case c == '"':
			j := i + 1
			for j < len(input) && input[j] != '"' {
				if input[j] == '\\' {
					j++
				}
				j++
			}
			if j >= len(input) {
				return append(toks, token{tokInvalid, input[i:]})
			}
			unquoted, err := strconv.Unquote(input[i : j+1])
			if err != nil {
				return append(toks, token{tokInvalid, input[i : j+1]})
			}
			toks, i = append(toks, token{tokString, unquoted}), j+1
		case c >= '0' && c <= '9' || c == '.':
			j := i
			for j < len(input) && (input[j] >= '0' && input[j] <= '9' || input[j] == '.') {
				j++
			}
			toks, i = append(toks, token{tokNumber, input[i:j]}), j
		case unicode.IsLetter(rune(c)) || c == '_':
			j := i
			for j < len(input) && (unicode.IsLetter(rune(input[j])) || unicode.IsDigit(rune(input[j])) || input[j] == '_' || input[j] == ':') {
				j++
			}
			toks, i = append(toks, token{tokIdent, input[i:j]}), j
		default:
			return append(toks, token{tokInvalid, string(c)})
		}
	}
	return append(toks, token{tokEOF, ""})
}

type parser struct {
	lex []token
	pos int
}

func (p *parser) next() token {
	t := p.lex[p.pos]
	if t.kind != tokEOF {
		p.pos++
	}
	return t
}

func (p *parser) peek() token { return p.lex[p.pos] }

func (p *parser) expect(kind tokKind, what string) (token, error) {
	t := p.next()
	if t.kind != kind {
		return t, fmt.Errorf("expected %s, got %q", what, t.text)
	}
	return t, nil
}

func (p *parser) parseExpr() (Expr, error) {
	ident, err := p.expect(tokIdent, "a name")
	if err != nil {
		return nil, err
	}
	switch {
	case aggFuncs[ident.text] != nil && p.peek().kind != tokLBrace && p.peek().kind != tokLBrack && p.peek().kind != tokEOF:
		return p.parseAggregate(ident.text)
	case windowFuncs[ident.text] != nil && p.peek().kind == tokLParen:
		return p.parseCall(ident.text)
	default:
		return p.parseSelector(ident.text)
	}
}

// parseAggregate: after `sum`, either `by (a, b) (expr)` or `(expr)`.
// A metric that happens to be NAMED sum{...} still parses as a selector —
// parseExpr only routes here when no brace/bracket follows the name.
func (p *parser) parseAggregate(op string) (Expr, error) {
	agg := Aggregate{Op: op}
	if t := p.peek(); t.kind == tokIdent && t.text == "by" {
		p.next()
		if _, err := p.expect(tokLParen, `"(" after by`); err != nil {
			return nil, err
		}
		for {
			name, err := p.expect(tokIdent, "a label name")
			if err != nil {
				return nil, err
			}
			agg.By = append(agg.By, name.text)
			t := p.next()
			if t.kind == tokRParen {
				break
			}
			if t.kind != tokComma {
				return nil, fmt.Errorf(`expected "," or ")" in by-clause, got %q`, t.text)
			}
		}
	}
	if _, err := p.expect(tokLParen, `"(" opening the aggregated expression`); err != nil {
		return nil, err
	}
	inner, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tokRParen, `")" closing the aggregation`); err != nil {
		return nil, err
	}
	agg.Expr = inner
	return agg, nil
}

// parseCall: after `rate`, `( selector [dur] )` — with an optional
// leading number for quantile_over_time.
func (p *parser) parseCall(fn string) (Expr, error) {
	c := Call{Func: fn}
	if _, err := p.expect(tokLParen, `"(" after function name`); err != nil {
		return nil, err
	}
	hasParam := p.peek().kind == tokNumber
	if hasParam {
		num := p.next()
		v, err := strconv.ParseFloat(num.text, 64)
		if err != nil {
			return nil, fmt.Errorf("bad number %q: %v", num.text, err)
		}
		c.Param = v
		if _, err := p.expect(tokComma, `"," after the parameter`); err != nil {
			return nil, err
		}
	}
	if needsParam := fn == "quantile_over_time"; needsParam != hasParam {
		if needsParam {
			return nil, fmt.Errorf("%s needs a quantile, e.g. %s(0.99, m[5m])", fn, fn)
		}
		return nil, fmt.Errorf("%s takes no leading number", fn)
	}
	name, err := p.expect(tokIdent, "a metric name")
	if err != nil {
		return nil, err
	}
	selExpr, err := p.parseSelector(name.text)
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tokLBrack, `"[" opening the range`); err != nil {
		return nil, err
	}
	// A duration like "5m" or "1h30m" lexes as alternating number/ident
	// tokens; reassemble the text and let Go's duration syntax judge it.
	var dur strings.Builder
	for p.peek().kind == tokNumber || p.peek().kind == tokIdent {
		dur.WriteString(p.next().text)
	}
	if dur.Len() == 0 {
		return nil, fmt.Errorf("expected a duration inside %q brackets", fn)
	}
	d, err := parseDuration(dur.String())
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tokRBrack, `"]" closing the range`); err != nil {
		return nil, err
	}
	if _, err := p.expect(tokRParen, `")" closing the call`); err != nil {
		return nil, err
	}
	c.Arg = MatrixSelector{Sel: selExpr.(VectorSelector), Range: d}
	return c, nil
}

// parseSelector: after a metric name, an optional matcher block.
func (p *parser) parseSelector(name string) (Expr, error) {
	sel := VectorSelector{Matchers: []tsdb.Matcher{tsdb.Eq(tsdb.MetricName, name)}}
	if p.peek().kind != tokLBrace {
		return sel, nil
	}
	p.next()
	if p.peek().kind == tokRBrace { // empty matcher block: m{}
		p.next()
		return sel, nil
	}
	for {
		label, err := p.expect(tokIdent, "a label name")
		if err != nil {
			return nil, err
		}
		op := p.next()
		if op.kind != tokEq && op.kind != tokNeq {
			return nil, fmt.Errorf(`expected "=" or "!=" after %q, got %q`, label.text, op.text)
		}
		val, err := p.expect(tokString, "a quoted label value")
		if err != nil {
			return nil, err
		}
		if op.kind == tokEq {
			sel.Matchers = append(sel.Matchers, tsdb.Eq(label.text, val.text))
		} else {
			sel.Matchers = append(sel.Matchers, tsdb.Neq(label.text, val.text))
		}
		t := p.next()
		if t.kind == tokRBrace {
			return sel, nil
		}
		if t.kind != tokComma {
			return nil, fmt.Errorf(`expected "," or "}" in matchers, got %q`, t.text)
		}
	}
}

// parseDuration accepts Go durations ("30s", "5m", "1.5h"), which
// overlap PromQL's syntax for the common cases.
func parseDuration(text string) (time.Duration, error) {
	d, err := time.ParseDuration(text)
	if err != nil {
		return 0, fmt.Errorf("bad duration %q", text)
	}
	if d <= 0 {
		return 0, fmt.Errorf("duration %q must be positive", text)
	}
	return d, nil
}
