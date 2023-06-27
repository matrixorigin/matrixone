package tree

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"io"
	"strings"
)

func NewFmtCtx2(dialectType dialect.DialectType, flags RestoreFlags) *FmtCtx {
	return &FmtCtx{
		Builder:     new(strings.Builder),
		dialectType: dialectType,
		Flags:       flags,
	}
}

// WriteKeyWord writes the `keyWord` into writer.
// `keyWord` will be converted format(uppercase and lowercase for now) according to `RestoreFlags`.
func (ctx *FmtCtx) WriteKeyWord(keyWord string) {
	switch {
	case ctx.Flags.HasKeyWordUppercaseFlag():
		keyWord = strings.ToUpper(keyWord)
	case ctx.Flags.HasKeyWordLowercaseFlag():
		keyWord = strings.ToLower(keyWord)
	}
	fmt.Fprint(ctx.Builder, keyWord)

}

// WriteWithSpecialComments writes a string with a special comment wrapped.
func (ctx *FmtCtx) WriteWithSpecialComments(featureID string, fn func() error) error {
	if !ctx.Flags.HasSpecialCommentFlag() {
		return fn()
	}
	ctx.WritePlain("/*T!")
	if len(featureID) != 0 {
		ctx.WritePlainf("[%s]", featureID)
	}
	ctx.WritePlain(" ")
	if err := fn(); err != nil {
		return err
	}
	ctx.WritePlain(" */")
	return nil
}

// WriteString writes the string into writer
// `str` may be wrapped in quotes and escaped according to RestoreFlags.
func (ctx *FmtCtx) WriteStringValue(str string) (int, error) {
	if ctx.Flags.HasStringEscapeBackslashFlag() {
		str = strings.Replace(str, `\`, `\\`, -1)
	}
	quotes := ""
	switch {
	case ctx.Flags.HasStringSingleQuotesFlag():
		str = strings.Replace(str, `'`, `''`, -1)
		quotes = `'`
	case ctx.Flags.HasStringDoubleQuotesFlag():
		str = strings.Replace(str, `"`, `""`, -1)
		quotes = `"`
	}
	return fmt.Fprint(ctx.Builder, quotes, str, quotes)
}

// WriteName writes the name into writer
// `name` maybe wrapped in quotes and escaped according to RestoreFlags.
func (ctx *FmtCtx) WriteName(name string) {
	switch {
	case ctx.Flags.HasNameUppercaseFlag():
		name = strings.ToUpper(name)
	case ctx.Flags.HasNameLowercaseFlag():
		name = strings.ToLower(name)
	}
	quotes := ""
	switch {
	case ctx.Flags.HasNameDoubleQuotesFlag():
		name = strings.Replace(name, `"`, `""`, -1)
		quotes = `"`
	case ctx.Flags.HasNameBackQuotesFlag():
		name = strings.Replace(name, "`", "``", -1)
		quotes = "`"
	}
	fmt.Fprint(ctx.Builder, quotes, name, quotes)
}

// WritePlain writes the plain text into writer without any handling.
func (ctx *FmtCtx) WritePlain(plainText string) {
	fmt.Fprint(ctx.Builder, plainText)
}

// WritePlainf write the plain text into writer without any handling.
func (ctx *FmtCtx) WritePlainf(format string, a ...interface{}) {
	fmt.Fprintf(ctx.Builder, format, a...)
}

// WriteValue2 -> WriteValue() and WriteStringQuote()
func (ctx *FmtCtx) WriteValue2(t P_TYPE, v string) (int, error) {
	switch t {
	case P_char:
		return ctx.WriteStringValue(v)
	default:
		return ctx.WriteString(v)
	}
}

func (ctx *FmtCtx) ToString() string {
	return ctx.String()
}

// -------------------------------------------------------------------------------------
// RestoreFlags mark the Restore format
type RestoreFlags uint64

// Mutually exclusive group of `RestoreFlags`:
// [RestoreStringSingleQuotes, RestoreStringDoubleQuotes]
// [RestoreKeyWordUppercase, RestoreKeyWordLowercase]
// [RestoreNameUppercase, RestoreNameLowercase]
// [RestoreNameDoubleQuotes, RestoreNameBackQuotes]
// The flag with the left position in each group has a higher priority.
const (
	RestoreStringSingleQuotes RestoreFlags = 1 << iota
	RestoreStringDoubleQuotes
	RestoreStringEscapeBackslash

	RestoreKeyWordUppercase
	RestoreKeyWordLowercase

	RestoreNameUppercase
	RestoreNameLowercase
	RestoreNameDoubleQuotes
	RestoreNameBackQuotes

	RestoreSpacesAroundBinaryOperation
	RestoreBracketAroundBinaryOperation

	RestoreStringWithoutCharset
	RestoreStringWithoutDefaultCharset

	RestoreSpecialComment
	SkipPlacementRuleForRestore
)

const (
	// DefaultRestoreFlags is the default value of RestoreFlags.
	DefaultRestoreFlags = RestoreStringSingleQuotes | RestoreKeyWordUppercase | RestoreNameBackQuotes
)

func (rfg RestoreFlags) has(flag RestoreFlags) bool {
	return rfg&flag != 0
}

// HasStringSingleQuotesFlag returns a boolean indicating when `rf` has `RestoreStringSingleQuotes` flag.
func (rfg RestoreFlags) HasStringSingleQuotesFlag() bool {
	return rfg.has(RestoreStringSingleQuotes)
}

// HasStringDoubleQuotesFlag returns a boolean indicating whether `rf` has `RestoreStringDoubleQuotes` flag.
func (rfg RestoreFlags) HasStringDoubleQuotesFlag() bool {
	return rfg.has(RestoreStringDoubleQuotes)
}

// HasStringEscapeBackslashFlag returns a boolean indicating whether `rf` has `RestoreStringEscapeBackslash` flag.
func (rfg RestoreFlags) HasStringEscapeBackslashFlag() bool {
	return rfg.has(RestoreStringEscapeBackslash)
}

// HasKeyWordUppercaseFlag returns a boolean indicating whether `rf` has `RestoreKeyWordUppercase` flag.
func (rfg RestoreFlags) HasKeyWordUppercaseFlag() bool {
	return rfg.has(RestoreKeyWordUppercase)
}

// HasKeyWordLowercaseFlag returns a boolean indicating whether `rf` has `RestoreKeyWordLowercase` flag.
func (rfg RestoreFlags) HasKeyWordLowercaseFlag() bool {
	return rfg.has(RestoreKeyWordLowercase)
}

// HasNameUppercaseFlag returns a boolean indicating whether `rf` has `RestoreNameUppercase` flag.
func (rfg RestoreFlags) HasNameUppercaseFlag() bool {
	return rfg.has(RestoreNameUppercase)
}

// HasNameLowercaseFlag returns a boolean indicating whether `rf` has `RestoreNameLowercase` flag.
func (rfg RestoreFlags) HasNameLowercaseFlag() bool {
	return rfg.has(RestoreNameLowercase)
}

// HasNameDoubleQuotesFlag returns a boolean indicating whether `rf` has `RestoreNameDoubleQuotes` flag.
func (rfg RestoreFlags) HasNameDoubleQuotesFlag() bool {
	return rfg.has(RestoreNameDoubleQuotes)
}

// HasNameBackQuotesFlag returns a boolean indicating whether `rf` has `RestoreNameBackQuotes` flag.
func (rfg RestoreFlags) HasNameBackQuotesFlag() bool {
	return rfg.has(RestoreNameBackQuotes)
}

// HasSpacesAroundBinaryOperationFlag returns a boolean indicating whether `rf` has `RestoreSpacesAroundBinaryOperation` flag.
func (rfg RestoreFlags) HasSpacesAroundBinaryOperationFlag() bool {
	return rfg.has(RestoreSpacesAroundBinaryOperation)
}

// HasRestoreBracketAroundBinaryOperation returns a boolean indicating whether `rf` has `RestoreBracketAroundBinaryOperation` flag.
func (rfg RestoreFlags) HasRestoreBracketAroundBinaryOperation() bool {
	return rfg.has(RestoreBracketAroundBinaryOperation)
}

// HasStringWithoutDefaultCharset returns a boolean indicating whether `rf` has `RestoreStringWithoutDefaultCharset` flag.
func (rfg RestoreFlags) HasStringWithoutDefaultCharset() bool {
	return rfg.has(RestoreStringWithoutDefaultCharset)
}

// HasStringWithoutCharset returns a boolean indicating whether `rf` has `RestoreStringWithoutCharset` flag.
func (rfg RestoreFlags) HasStringWithoutCharset() bool {
	return rfg.has(RestoreStringWithoutCharset)
}

// HasTiDBSpecialCommentFlag returns a boolean indicating whether `rf` has `RestoreTiDBSpecialComment` flag.
func (rfg RestoreFlags) HasSpecialCommentFlag() bool {
	return rfg.has(RestoreSpecialComment)
}

// HasSkipPlacementRuleForRestoreFlag returns a boolean indicating whether `rf` has `SkipPlacementRuleForRestore` flag.
func (rfg RestoreFlags) HasSkipPlacementRuleForRestoreFlag() bool {
	return rfg.has(SkipPlacementRuleForRestore)
}

// RestoreCtx is `Restore` context to hold flags and writer.
type RestoreCtx struct {
	Flags     RestoreFlags
	Wtr       io.Writer
	DefaultDB string
}

// NewRestoreCtx returns a new `RestoreCtx`.
func NewRestoreCtx(flags RestoreFlags, wtr io.Writer) *RestoreCtx {
	return &RestoreCtx{
		Flags:     flags,
		Wtr:       wtr,
		DefaultDB: "",
	}
}

// WriteKeyWord writes the `keyWord` into writer.
// `keyWord` will be converted format(uppercase and lowercase for now) according to `RestoreFlags`.
func (ctx *RestoreCtx) WriteKeyWord(keyWord string) {
	switch {
	case ctx.Flags.HasKeyWordUppercaseFlag():
		keyWord = strings.ToUpper(keyWord)
	case ctx.Flags.HasKeyWordLowercaseFlag():
		keyWord = strings.ToLower(keyWord)
	}
	fmt.Fprint(ctx.Wtr, keyWord)
}

// WriteWithSpecialComments writes a string with a special comment wrapped.
func (ctx *RestoreCtx) WriteWithSpecialComments(featureID string, fn func() error) error {
	if !ctx.Flags.HasSpecialCommentFlag() {
		return fn()
	}
	ctx.WritePlain("/*T!")
	if len(featureID) != 0 {
		ctx.WritePlainf("[%s]", featureID)
	}
	ctx.WritePlain(" ")
	if err := fn(); err != nil {
		return err
	}
	ctx.WritePlain(" */")
	return nil
}

// WriteString writes the string into writer
// `str` may be wrapped in quotes and escaped according to RestoreFlags.
func (ctx *RestoreCtx) WriteString(str string) {
	if ctx.Flags.HasStringEscapeBackslashFlag() {
		str = strings.Replace(str, `\`, `\\`, -1)
	}
	quotes := ""
	switch {
	case ctx.Flags.HasStringSingleQuotesFlag():
		str = strings.Replace(str, `'`, `''`, -1)
		quotes = `'`
	case ctx.Flags.HasStringDoubleQuotesFlag():
		str = strings.Replace(str, `"`, `""`, -1)
		quotes = `"`
	}
	fmt.Fprint(ctx.Wtr, quotes, str, quotes)
}

// WriteName writes the name into writer
// `name` maybe wrapped in quotes and escaped according to RestoreFlags.
func (ctx *RestoreCtx) WriteName(name string) {
	switch {
	case ctx.Flags.HasNameUppercaseFlag():
		name = strings.ToUpper(name)
	case ctx.Flags.HasNameLowercaseFlag():
		name = strings.ToLower(name)
	}
	quotes := ""
	switch {
	case ctx.Flags.HasNameDoubleQuotesFlag():
		name = strings.Replace(name, `"`, `""`, -1)
		quotes = `"`
	case ctx.Flags.HasNameBackQuotesFlag():
		name = strings.Replace(name, "`", "``", -1)
		quotes = "`"
	}
	fmt.Fprint(ctx.Wtr, quotes, name, quotes)
}

// WritePlain writes the plain text into writer without any handling.
func (ctx *RestoreCtx) WritePlain(plainText string) {
	fmt.Fprint(ctx.Wtr, plainText)
}

// WritePlainf write the plain text into writer without any handling.
func (ctx *RestoreCtx) WritePlainf(format string, a ...interface{}) {
	fmt.Fprintf(ctx.Wtr, format, a...)
}
