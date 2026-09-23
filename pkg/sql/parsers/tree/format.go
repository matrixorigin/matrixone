// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

// FmtCtx contains formatted text of the node.
type FmtCtx struct {
	*strings.Builder
	dialectType dialect.DialectType
	// quoteString string
	quoteString       bool
	singleQuoteString bool
	quoteIdentifier   bool
	// noBackslashEscape mirrors the NO_BACKSLASH_ESCAPES sql_mode: when set, string
	// literals are emitted without backslash escaping (backslash is a literal char),
	// so a value deparsed here re-parses to the same string under that mode. Nodes that
	// re-escape a stored (already-unescaped) literal — e.g. FullTextMatchExpr's pattern
	// — must consult this to keep format->parse idempotent under NO_BACKSLASH_ESCAPES.
	noBackslashEscape bool
	// modeIndependentStringLiterals emits string values containing backslashes
	// as hex-to-varchar casts so the SQL reparses identically with or without
	// NO_BACKSLASH_ESCAPES.
	modeIndependentStringLiterals bool
	paramExprOffset               bool
	canonicalUserVariableNames    bool
	detectDateTimeFormat          bool
	sawDateTimeFormat             bool
	stringLiteralPositions        *[]StringLiteralPosition
	maxOutputBytes                int
	outputLimitExceeded           bool
}

// formatOutputLimitAbort unwinds an AST traversal when a bounded formatter
// would emit more than its configured output limit. It is recovered only by
// FmtCtx.FormatNode; ordinary formatter contexts never raise it.
type formatOutputLimitAbort struct{}

// StringLiteralPosition identifies the bytes occupied by one string literal
// in the formatted output. Positions are recorded only when requested by the
// caller and are relative to the FmtCtx builder.
type StringLiteralPosition struct {
	Start int
	End   int
}

func NewFmtCtx(dialectType dialect.DialectType, opts ...FmtCtxOption) *FmtCtx {
	ctx := &FmtCtx{
		Builder:     new(strings.Builder),
		dialectType: dialectType,
	}
	for _, opt := range opts {
		opt.Apply(ctx)
	}
	return ctx
}

type FmtCtxOption func(*FmtCtx)

func (f FmtCtxOption) Apply(ctx *FmtCtx) {
	f(ctx)
}

func WithQuoteString(quote bool) FmtCtxOption {
	return FmtCtxOption(func(ctx *FmtCtx) {
		ctx.quoteString = quote
	})
}

func WithSingleQuoteString() FmtCtxOption {
	return FmtCtxOption(func(ctx *FmtCtx) {
		ctx.singleQuoteString = true
	})
}

func WithQuoteIdentifier() FmtCtxOption {
	return FmtCtxOption(func(ctx *FmtCtx) {
		ctx.quoteIdentifier = true
	})
}

// WithNoBackslashEscape makes string-literal formatting match the
// NO_BACKSLASH_ESCAPES sql_mode: a deparse-then-reparse under that mode stays
// idempotent (a backslash is emitted literally, not doubled). Pass this when the
// output will be re-parsed with NO_BACKSLASH_ESCAPES active.
func WithNoBackslashEscape() FmtCtxOption {
	return FmtCtxOption(func(ctx *FmtCtx) {
		ctx.noBackslashEscape = true
	})
}

// NoBackslashEscape reports whether string literals should be formatted for the
// NO_BACKSLASH_ESCAPES sql_mode.
func (ctx *FmtCtx) NoBackslashEscape() bool { return ctx.noBackslashEscape }

func WithModeIndependentStringLiterals() FmtCtxOption {
	return FmtCtxOption(func(ctx *FmtCtx) {
		ctx.modeIndependentStringLiterals = true
	})
}

func (ctx *FmtCtx) ModeIndependentStringLiterals() bool {
	return ctx.modeIndependentStringLiterals
}

// WithParamExprOffset includes a parameter's parser-assigned offset in its
// formatted form. It is intended for internal semantic keys; SQL restored for
// users must keep the default placeholder-only representation.
func WithParamExprOffset() FmtCtxOption {
	return FmtCtxOption(func(ctx *FmtCtx) {
		ctx.paramExprOffset = true
	})
}

// WithCanonicalUserVariableNames lowercases user-defined variable names
// while formatting. MatrixOne resolves those names case-insensitively; this
// option lets semantic-key callers use the same identity without changing the
// AST or the default SQL rendering.
func WithCanonicalUserVariableNames() FmtCtxOption {
	return FmtCtxOption(func(ctx *FmtCtx) {
		ctx.canonicalUserVariableNames = true
	})
}

// WithDateTimeFormatDetection asks the formatter to report whether the
// expression tree contains a DATE_FORMAT or TIME_FORMAT call. Detection is
// performed by the formatter itself, so nested expressions and subqueries use
// the same complete traversal as normal SQL rendering.
func WithDateTimeFormatDetection() FmtCtxOption {
	return FmtCtxOption(func(ctx *FmtCtx) {
		ctx.detectDateTimeFormat = true
	})
}

// WithStringLiteralPositions records the output ranges of string literals.
// The caller normally combines this with WithSingleQuoteString so the ranges
// include their stable SQL quoting.
func WithStringLiteralPositions(positions *[]StringLiteralPosition) FmtCtxOption {
	return FmtCtxOption(func(ctx *FmtCtx) {
		ctx.stringLiteralPositions = positions
	})
}

// WithMaxOutputBytes bounds formatted output. If a write would exceed the
// limit, formatting is aborted; callers must use FmtCtx.FormatNode to recover
// that internal abort and observe OutputLimitExceeded. A non-positive limit
// leaves formatting unbounded, preserving the default behavior.
func WithMaxOutputBytes(maxBytes int) FmtCtxOption {
	return FmtCtxOption(func(ctx *FmtCtx) {
		ctx.maxOutputBytes = maxBytes
	})
}

// OutputLimitExceeded reports whether a write attempted to grow the formatted
// output beyond the configured maximum.
func (ctx *FmtCtx) OutputLimitExceeded() bool {
	return ctx.outputLimitExceeded
}

// FormatNode formats node and reports whether formatting completed. When an
// output limit is configured, the first write beyond it stops the whole AST
// traversal instead of continuing to visit nodes whose output will be
// discarded. Panics unrelated to the output limit are propagated unchanged.
func (ctx *FmtCtx) FormatNode(node NodeFormatter) (complete bool) {
	complete = true
	defer func() {
		if recovered := recover(); recovered != nil {
			if _, ok := recovered.(formatOutputLimitAbort); ok {
				complete = false
				return
			}
			panic(recovered)
		}
	}()
	if node != nil {
		node.Format(ctx)
	}
	return !ctx.outputLimitExceeded
}

func (ctx *FmtCtx) limitedWriteLength(n int) int {
	if ctx.maxOutputBytes <= 0 {
		return n
	}
	remaining := ctx.maxOutputBytes - ctx.Len()
	if remaining < 0 {
		remaining = 0
	}
	if n > remaining {
		ctx.outputLimitExceeded = true
		panic(formatOutputLimitAbort{})
	}
	return n
}

// Override the promoted strings.Builder writes so callers that opt into an
// output limit cannot allocate an arbitrarily large formatting buffer.
func (ctx *FmtCtx) Write(p []byte) (int, error) {
	n := ctx.limitedWriteLength(len(p))
	return ctx.Builder.Write(p[:n])
}

func (ctx *FmtCtx) WriteString(s string) (int, error) {
	n := ctx.limitedWriteLength(len(s))
	return ctx.Builder.WriteString(s[:n])
}

func (ctx *FmtCtx) WriteByte(b byte) error {
	if ctx.limitedWriteLength(1) == 0 {
		return nil
	}
	return ctx.Builder.WriteByte(b)
}

func (ctx *FmtCtx) WriteRune(r rune) (int, error) {
	width := utf8.RuneLen(r)
	if width < 0 {
		width = utf8.RuneLen(utf8.RuneError)
	}
	if ctx.limitedWriteLength(width) < width {
		return 0, nil
	}
	return ctx.Builder.WriteRune(r)
}

func (ctx *FmtCtx) Grow(n int) {
	if ctx.maxOutputBytes > 0 {
		remaining := ctx.maxOutputBytes - ctx.Len()
		if remaining < 0 {
			remaining = 0
		}
		if n > remaining {
			n = remaining
		}
	}
	ctx.Builder.Grow(n)
}

func (ctx *FmtCtx) Reset() {
	ctx.Builder.Reset()
	ctx.outputLimitExceeded = false
}

// HasDateTimeFormatFunction reports whether formatting visited a
// DATE_FORMAT/TIME_FORMAT call while detection was enabled.
func (ctx *FmtCtx) HasDateTimeFormatFunction() bool {
	return ctx.sawDateTimeFormat
}

// NodeFormatter for formatted output of the node.
type NodeFormatter interface {
	Format(ctx *FmtCtx)
}

// Visitor Design Pattern
// NodeChecker is abstract tree Node
type NodeChecker interface {
	// `Accept` method accepts Visitor to visit itself. Visitor checks the current node
	// The returned node should replace original node.
	// The node returned by Accpet should replace the original node.
	// If OK returns false, it stops accessing other child nodes.

	//	The general implementation logic of the `Accept` method is:
	//	First, call the Visitor.`Enter` method, and assign the returned `node` to the receiver of the `Accept` method,
	//	If the returnd `skipChildren` value is true, then it is necessary to stop accessing the receiver's child node
	//	Otherwise, recursively call the` Accept` of its children nodes,
	//	Finally, don't forget to call the Visitor's `Exit` method
	Accept(v Visitor) (node Expr, ok bool)
}

// Visitor Design Pattern
// Visitor visits the ast node or sub ast nodes
type Visitor interface {
	// Call the 'Enter' method before visiting the children nodes.
	// The node type returned by the `Enter` method must be the same as the input node type
	// SkipChildren returning true means that access to child nodes should be skipped.
	Enter(n Expr) (node Expr, skipChildren bool)

	//`Exit` is called after all children nodes are visited.
	//The returned node of the `Exit` method is `Expr`, which is of the same type as the input node.
	//if `Exit` method returns OK as false ,means stop visiting.
	Exit(n Expr) (node Expr, ok bool)
}

func String(node NodeFormatter, dialectType dialect.DialectType) string {
	if node == nil {
		return "<nil>"
	}

	ctx := NewFmtCtx(dialectType)
	node.Format(ctx)
	return ctx.String()
}

// StringWithOpts Restore SQL and provide string formatting restore options
func StringWithOpts(node NodeFormatter, dialectType dialect.DialectType, opts ...FmtCtxOption) string {
	if node == nil {
		return "<nil>"
	}

	ctx := NewFmtCtx(dialectType, opts...)
	node.Format(ctx)
	return ctx.String()
}

func (ctx *FmtCtx) PrintExpr(currentExpr Expr, expr Expr, left bool) {
	if precedenceFor(currentExpr) == Syntactic {
		expr.Format(ctx)
	} else {
		needParens := needParens(currentExpr, expr, left)
		if needParens {
			ctx.WriteByte('(')
		}
		expr.Format(ctx)
		if needParens {
			ctx.WriteByte(')')
		}
	}
}

func (ctx *FmtCtx) WriteValue(t P_TYPE, v string) (int, error) {
	start := ctx.Len()
	var n int
	var err error
	if ctx.quoteString {
		switch t {
		case P_char:
			n, err = ctx.WriteString(fmt.Sprintf("%q", v))
		default:
			n, err = ctx.WriteString(v)
		}
	} else if ctx.singleQuoteString && (t == P_char || t == P_ScoreBinary) {
		if t == P_ScoreBinary {
			_, err = ctx.WriteString("_binary ")
		}
		if err == nil {
			err = ctx.WriteByte('\'')
		}
		if err == nil {
			ctx.writeDoubled(v, '\'')
			err = ctx.WriteByte('\'')
		}
	} else {
		n, err = ctx.WriteString(v)
	}
	if ctx.singleQuoteString && (t == P_char || t == P_ScoreBinary) && err == nil {
		n = ctx.Len() - start
	}
	if err == nil && (t == P_char || t == P_ScoreBinary) && ctx.stringLiteralPositions != nil {
		*ctx.stringLiteralPositions = append(*ctx.stringLiteralPositions, StringLiteralPosition{
			Start: start,
			End:   ctx.Len(),
		})
	}
	return n, err
}

// writeFormattedStringValue streams the same escaping as FormatString followed
// by WriteValue. It is used only by bounded formatting, where materializing an
// escaped copy before the output limit is checked would defeat the bound.
func (ctx *FmtCtx) writeFormattedStringValue(t P_TYPE, value string) {
	start := ctx.Len()
	quoteLiteral := ctx.singleQuoteString && (t == P_char || t == P_ScoreBinary)
	if quoteLiteral && t == P_ScoreBinary {
		ctx.WriteString("_binary ")
	}
	if quoteLiteral {
		ctx.WriteByte('\'')
	}
	ctx.writeFormattedString(value, quoteLiteral)
	if quoteLiteral {
		ctx.WriteByte('\'')
	}
	if (t == P_char || t == P_ScoreBinary) && ctx.stringLiteralPositions != nil {
		*ctx.stringLiteralPositions = append(*ctx.stringLiteralPositions, StringLiteralPosition{
			Start: start,
			End:   ctx.Len(),
		})
	}
}

// writeFormattedString mirrors FormatString without allocating its expanded
// result. quoteLiteral applies the SQL single-quote doubling done by WriteValue.
func (ctx *FmtCtx) writeFormattedString(value string, quoteLiteral bool) {
	writeRune := func(r rune) {
		if quoteLiteral && r == '\'' {
			ctx.WriteByte('\'')
		}
		ctx.WriteRune(r)
	}
	for i, r := range value {
		switch r {
		case '\n':
			ctx.WriteString(`\n`)
		case '\x00':
			ctx.WriteString(`\0`)
		case '\r':
			ctx.WriteString(`\r`)
		case '\\':
			if i+1 < len(value) && (value[i+1] == '_' || value[i+1] == '%') {
				writeRune('\\')
				continue
			}
			ctx.WriteString(`\\`)
		case '\b':
			ctx.WriteString(`\b`)
		case '\x1a':
			ctx.WriteString(`\Z`)
		case '\t':
			ctx.WriteString(`\t`)
		default:
			writeRune(r)
		}
	}
}

// writeDoubled writes value while doubling each occurrence of quote. It writes
// substrings directly to the context so bounded formatting does not first
// allocate a second copy of a potentially large literal or identifier.
func (ctx *FmtCtx) writeDoubled(value string, quote byte) {
	for from := 0; from < len(value); {
		to := len(value)
		if ctx.maxOutputBytes > 0 {
			remaining := ctx.maxOutputBytes - ctx.Len()
			if remaining < 0 {
				remaining = 0
			}
			// Inspect no more input bytes than can fit plus one. If no quote is
			// found in that prefix, WriteString will abort before copying it.
			if remaining < to-from-1 {
				to = from + remaining + 1
			}
		}
		rel := strings.IndexByte(value[from:to], quote)
		if rel < 0 {
			ctx.WriteString(value[from:to])
			from = to
			continue
		}
		at := from + rel
		ctx.WriteString(value[from:at])
		ctx.WriteByte(quote)
		ctx.WriteByte(quote)
		from = at + 1
	}
}

func (ctx *FmtCtx) WriteStringQuote(v string) (int, error) {
	if ctx.quoteString {
		return ctx.WriteString(fmt.Sprintf("%q", v))
	} else {
		return ctx.WriteString(v)
	}
}

// needParens says if we need a parenthesis
// op is the operator we are printing
// val is the value we are checking if we need parens around or not
// left let's us know if the value is on the lhs or rhs of the operator
func needParens(op, val Expr, left bool) bool {
	// Values are atomic and never need parens
	if IsValue(val) {
		return false
	}

	if areBothISExpr(op, val) {
		return true
	}

	opBinding := precedenceFor(op)
	valBinding := precedenceFor(val)

	if opBinding == Syntactic || valBinding == Syntactic {
		return false
	}

	if left {
		// for left associative operators, if the value is to the left of the operator,
		// we only need parens if the order is higher for the value expression
		return valBinding > opBinding
	}

	return valBinding >= opBinding
}

// IsValue returns true if the Expr is a string, integral or value arg.
// NULL is not considered to be a value.
func IsValue(node Expr) bool {
	switch node.(type) {
	case *NumVal, *StrVal:
		return true
	}
	return false
}

func areBothISExpr(op Expr, val Expr) bool {
	// when using IS on an IS op, we need special handling
	_, isOpIs := op.(*IsNullExpr)
	if isOpIs {
		_, isValIs := val.(*IsNullExpr)
		if isValIs {
			return true
		}
	}
	_, isOpIsNot := op.(*IsNullExpr)
	if isOpIsNot {
		_, isValIsNot := val.(*IsNullExpr)
		if isValIsNot {
			return true
		}
	}
	return false
}
