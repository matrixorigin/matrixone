// Copyright 2023 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"strings"
)

func NewFmtCtxWithFlag(dialectType dialect.DialectType, flags RestoreFlags) *FmtCtx {
	return &FmtCtx{
		Builder:     new(strings.Builder),
		dialectType: dialectType,
		flags:       flags,
	}
}

// WriteKeyWord writes the SQL keywords into writer.
// SQL KeyWord will be converted to uppercase or lowercase based on RestoreFlags
func (ctx *FmtCtx) WriteKeyWord(keyword string) {
	switch {
	case ctx.flags.HasKeyWordUppercaseFlag():
		keyword = strings.ToUpper(keyword)
	case ctx.flags.HasKeyWordLowercaseFlag():
		keyword = strings.ToLower(keyword)
	}
	fmt.Fprint(ctx.Builder, keyword)

}

// WriteStringValue writes the string into writer
// 'str' parameter may be enclosed in quotation marks and escaped based on FormatFlags.
func (ctx *FmtCtx) WriteStringValue(str string) (int, error) {
	if ctx.flags.HasStringEscapeBackslashFlag() {
		str = strings.Replace(str, `\`, `\\`, -1)
	}
	quotes := ""
	switch {
	case ctx.flags.HasStringSingleQuotesFlag():
		str = strings.Replace(str, `'`, `''`, -1)
		quotes = `'`
	case ctx.flags.HasStringDoubleQuotesFlag():
		str = strings.Replace(str, `"`, `""`, -1)
		quotes = `"`
	}
	return fmt.Fprint(ctx.Builder, quotes, str, quotes)
}

// WriteName Write the database name identifier to the writer, such as 'DbName', 'TableName', 'ColumnName' and so on,
// the name identifier may be wrapped in quotation marks according to RestoreFlags.
func (ctx *FmtCtx) WriteName(name string) {
	switch {
	case ctx.flags.HasNameUppercaseFlag():
		name = strings.ToUpper(name)
	case ctx.flags.HasNameLowercaseFlag():
		name = strings.ToLower(name)
	}
	quotes := ""
	switch {
	case ctx.flags.HasNameDoubleQuotesFlag():
		name = strings.Replace(name, `"`, `""`, -1)
		quotes = `"`
	case ctx.flags.HasNameBackQuotesFlag():
		name = strings.Replace(name, "`", "``", -1)
		quotes = "`"
	}
	fmt.Fprint(ctx.Builder, quotes, name, quotes)
}

// WritePlainText Write plain text into FmtCtx writer
func (ctx *FmtCtx) WritePlainText(plainText string) {
	fmt.Fprint(ctx.Builder, plainText)
}

func (ctx *FmtCtx) ToString() string {
	return ctx.String()
}

// RestoreFlags mark Lexical token display format
type RestoreFlags uint32

// The following are the 'RestoreFlags' groups with mutually exclusive relationships:
// (`RestoreStringSingleQuotes`, `RestoreStringDoubleQuotes`)
// (`RestoreKeyWordUppercase`, RestoreKeyWordLowercase`)
// (`RestoreNameUppercase`, `RestoreNameLowercase`)
// (`RestoreNameDoubleQuotes`, `RestoreNameBackQuotes`)
// Attention when using
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
)

// HasStringSingleQuotesFlag returns a boolean value indicating whether `r` has the "RestoreStringSingleQuotes" flag.
func (r RestoreFlags) HasStringSingleQuotesFlag() bool {
	return r&RestoreStringSingleQuotes != 0
}

// HasStringDoubleQuotesFlag returns a boolean indicating whether `r` has `RestoreStringDoubleQuotes` flag.
func (r RestoreFlags) HasStringDoubleQuotesFlag() bool {
	return r&RestoreStringDoubleQuotes != 0
}

// HasStringEscapeBackslashFlag returns a boolean indicating whether `r` has `RestoreStringEscapeBackslash` flag.
func (r RestoreFlags) HasStringEscapeBackslashFlag() bool {
	return r&RestoreStringEscapeBackslash != 0
}

// HasKeyWordUppercaseFlag returns a boolean indicating whether `r` has `RestoreKeyWordUppercase` flag.
func (r RestoreFlags) HasKeyWordUppercaseFlag() bool {
	return r&RestoreKeyWordUppercase != 0
}

// HasKeyWordLowercaseFlag returns a boolean indicating whether `r` has `RestoreKeyWordLowercase` flag.
func (r RestoreFlags) HasKeyWordLowercaseFlag() bool {
	return r&RestoreKeyWordLowercase != 0
}

// HasNameUppercaseFlag returns a boolean value which indicating whether `r` has `RestoreNameUppercase` flag.
func (r RestoreFlags) HasNameUppercaseFlag() bool {
	return r&RestoreNameUppercase != 0
}

// HasNameLowercaseFlag returns a boolean value which indicating whether `r` has `RestoreNameLowercase` flag.
func (r RestoreFlags) HasNameLowercaseFlag() bool {
	return r&RestoreNameLowercase != 0
}

// HasNameDoubleQuotesFlag returns a boolean value which indicating whether `r` has `RestoreNameDoubleQuotes` flag.
func (r RestoreFlags) HasNameDoubleQuotesFlag() bool {
	return r&RestoreNameDoubleQuotes != 0
}

// HasNameBackQuotesFlag returns a boolean value which indicating whether `r` has `RestoreNameBackQuotes` flag.
func (r RestoreFlags) HasNameBackQuotesFlag() bool {
	return r&RestoreNameBackQuotes != 0
}
