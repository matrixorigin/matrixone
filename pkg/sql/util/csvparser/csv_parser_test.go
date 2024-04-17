// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package csvparser

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TODO: rewrite test case

func NewStringReader(str string) io.Reader {
	return strings.NewReader(str)
}

func newStringField(val string, isNull bool) Field {
	return Field{
		Val:    val,
		IsNull: isNull,
	}
}
func assertPosEqual(t *testing.T, parser *CSVParser, pos int64) {
	require.Equal(t, parser.Pos(), pos)
}
func tpchDatums() [][]Field {
	datums := make([][]Field, 0, 3)
	datums = append(datums, []Field{
		newStringField("1", false),
		newStringField("goldenrod lavender spring chocolate lace", false),
		newStringField("Manufacturer#1", false),
		newStringField("Brand#13", false),
		newStringField("PROMO BURNISHED COPPER", false),
		newStringField("7", false),
		newStringField("JUMBO PKG", false),
		newStringField("901.00", false),
		newStringField("ly. slyly ironi", false),
	})
	datums = append(datums, []Field{
		newStringField("2", false),
		newStringField("blush thistle blue yellow saddle", false),
		newStringField("Manufacturer#1", false),
		newStringField("Brand#13", false),
		newStringField("LARGE BRUSHED BRASS", false),
		newStringField("1", false),
		newStringField("LG CASE", false),
		newStringField("902.00", false),
		newStringField("lar accounts amo", false),
	})
	datums = append(datums, []Field{
		newStringField("3", false),
		newStringField("spring green yellow purple cornsilk", false),
		newStringField("Manufacturer#4", false),
		newStringField("Brand#42", false),
		newStringField("STANDARD POLISHED BRASS", false),
		newStringField("21", false),
		newStringField("WRAP CASE", false),
		newStringField("903.00", false),
		newStringField("egular deposits hag", false),
	})

	return datums
}

func datumsToString(datums [][]Field, delimitor string, quote string, lastSep bool) string {
	var b strings.Builder
	doubleQuote := quote + quote
	for _, ds := range datums {
		for i, d := range ds {
			text := d.Val
			if len(quote) > 0 {
				b.WriteString(quote)
				b.WriteString(strings.ReplaceAll(text, quote, doubleQuote))
				b.WriteString(quote)
			} else {
				b.WriteString(text)
			}
			if lastSep || i < len(ds)-1 {
				b.WriteString(delimitor)
			}
		}
		b.WriteString("\r\n")
	}
	return b.String()
}

func TestTPCH(t *testing.T) {
	datums := tpchDatums()
	input := datumsToString(datums, "|", "", true)
	reader := strings.NewReader(input)

	cfg := CSVConfig{
		FieldsTerminatedBy: "|",
		FieldsEnclosedBy:   "",
		TrimLastSep:        true,
	}

	parser, err := NewCSVParser(&cfg, reader, int64(ReadBlockSize), false, false)
	require.NoError(t, err)

	var row []Field

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, datums[0], row)
	require.Equal(t, parser.Pos(), int64(126))
	assertPosEqual(t, parser, 126)

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, datums[1], row)
	assertPosEqual(t, parser, 241)

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, datums[2], row)
	assertPosEqual(t, parser, 369)

}

func TestTPCHMultiBytes(t *testing.T) {
	datums := tpchDatums()
	sepsAndQuotes := [][2]string{
		{",", ""},
		{"\000", ""},
		{"，", ""},
		{"🤔", ""},
		{"，", "。"},
		{"||", ""},
		{"|+|", ""},
		{"##", ""},
		{"，", "'"},
		{"，", `"`},
		{"🤔", `''`},
		{"🤔", `"'`},
		{"🤔", `"'`},
		{"🤔", "🌚"}, // this two emoji have same prefix bytes
		{"##", "#-"},
		{"\\s", "\\q"},
		{",", "1"},
		{",", "ac"},
	}
	for _, SepAndQuote := range sepsAndQuotes {
		inputStr := datumsToString(datums, SepAndQuote[0], SepAndQuote[1], false)

		// extract all index in the middle of '\r\n' from the inputStr.
		// they indicate where the parser stops after reading one row.
		// should be equals to the number of datums.
		var allExpectedParserPos []int
		for {
			last := 0
			if len(allExpectedParserPos) > 0 {
				last = allExpectedParserPos[len(allExpectedParserPos)-1]
			}
			pos := strings.IndexByte(inputStr[last:], '\r')
			if pos < 0 {
				break
			}
			allExpectedParserPos = append(allExpectedParserPos, last+pos+1)
		}
		require.Len(t, allExpectedParserPos, len(datums))

		cfg := CSVConfig{
			FieldsTerminatedBy: SepAndQuote[0],
			FieldsEnclosedBy:   SepAndQuote[1],
			TrimLastSep:        false,
		}

		reader := NewStringReader(inputStr)
		parser, err := NewCSVParser(&cfg, reader, int64(ReadBlockSize), false, false)
		if fmt.Sprint(err) == "invalid input: invalid field or comment delimiter" {
			continue
		}
		require.NoError(t, err)

		for i, expectedParserPos := range allExpectedParserPos {
			row, err := parser.Read()
			require.Nil(t, err)
			require.Equal(t, datums[i], row)
			assertPosEqual(t, parser, int64(expectedParserPos))
		}

	}
}

func TestLinesTerminatedBy(t *testing.T) {
	datums := tpchDatums()
	input := datumsToString(datums, "|", "", true)
	reader := strings.NewReader(input)

	cfg := CSVConfig{
		FieldsTerminatedBy: "|",
		FieldsEnclosedBy:   "",
		LinesTerminatedBy:  "\r\n",
		TrimLastSep:        true,
	}

	parser, err := NewCSVParser(&cfg, reader, int64(ReadBlockSize), false, false)
	require.NoError(t, err)

	var row []Field

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, datums[0], row)
	require.Equal(t, parser.Pos(), int64(127))
	assertPosEqual(t, parser, 127)

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, datums[1], row)
	assertPosEqual(t, parser, 242)

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, datums[2], row)
	assertPosEqual(t, parser, 370)

}

func TestRFC4180(t *testing.T) {
	cfg := CSVConfig{
		FieldsTerminatedBy: ",",
		FieldsEnclosedBy:   `"`,
	}

	// example 1, trailing new lines

	parser, err := NewCSVParser(&cfg, NewStringReader("aaa,bbb,ccc\nzzz,yyy,xxx\n"), int64(ReadBlockSize), false, false)
	require.NoError(t, err)

	var row []Field

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, []Field{
		newStringField("aaa", false),
		newStringField("bbb", false),
		newStringField("ccc", false),
	}, row)
	assertPosEqual(t, parser, 12)

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, []Field{
		newStringField("zzz", false),
		newStringField("yyy", false),
		newStringField("xxx", false),
	}, row)
	assertPosEqual(t, parser, 24)

	// example 2, no trailing new lines

	parser, err = NewCSVParser(&cfg, NewStringReader("aaa,bbb,ccc\nzzz,yyy,xxx"), int64(ReadBlockSize), false, false)
	require.NoError(t, err)

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, []Field{
		newStringField("aaa", false),
		newStringField("bbb", false),
		newStringField("ccc", false),
	}, row)
	assertPosEqual(t, parser, 12)

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, []Field{
		newStringField("zzz", false),
		newStringField("yyy", false),
		newStringField("xxx", false),
	}, row)
	assertPosEqual(t, parser, 23)

	// example 5, quoted fields

	parser, err = NewCSVParser(&cfg, NewStringReader(`"aaa","bbb","ccc"`+"\nzzz,yyy,xxx"), int64(ReadBlockSize), false, false)
	require.NoError(t, err)

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, []Field{
		newStringField("aaa", false),
		newStringField("bbb", false),
		newStringField("ccc", false),
	}, row)
	assertPosEqual(t, parser, 18)

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, []Field{
		newStringField("zzz", false),
		newStringField("yyy", false),
		newStringField("xxx", false),
	}, row)
	assertPosEqual(t, parser, 29)

	// example 6, line breaks within fields

	parser, err = NewCSVParser(&cfg, NewStringReader(`"aaa","b
bb","ccc"
zzz,yyy,xxx`), int64(ReadBlockSize), false, false)
	require.NoError(t, err)

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, []Field{
		newStringField("aaa", false),
		newStringField("b\nbb", false),
		newStringField("ccc", false),
	}, row)
	assertPosEqual(t, parser, 19)

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, []Field{
		newStringField("zzz", false),
		newStringField("yyy", false),
		newStringField("xxx", false),
	}, row)
	assertPosEqual(t, parser, 30)

	// example 7, quote escaping

	parser, err = NewCSVParser(&cfg, NewStringReader(`"aaa","b""bb","ccc"`), int64(ReadBlockSize), false, false)
	require.NoError(t, err)

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, []Field{
		newStringField("aaa", false),
		newStringField("b\"bb", false),
		newStringField("ccc", false),
	}, row)
	assertPosEqual(t, parser, 19)

}

func TestMySQL(t *testing.T) {
	cfg := CSVConfig{
		FieldsTerminatedBy: ",",
		FieldsEnclosedBy:   `"`,
		LinesTerminatedBy:  "\n",
		FieldsEscapedBy:    `\`,
		NotNull:            false,
		Null:               []string{`\N`},
	}

	parser, err := NewCSVParser(&cfg, NewStringReader(`"\"","\\","\?"
"\
",\N,\\N`), int64(ReadBlockSize), false, false)
	require.NoError(t, err)

	var row []Field

	row, err = parser.Read()
	require.NoError(t, err)
	require.Equal(t, []Field{
		newStringField(`"`, false),
		newStringField(`\`, false),
		newStringField("?", false),
	}, row)

	assertPosEqual(t, parser, 15)

	row, err = parser.Read()
	require.NoError(t, err)

	require.Equal(t, []Field{
		newStringField("\n", false),
		newStringField("\\N", true),
		newStringField(`\N`, false),
	}, row)

	assertPosEqual(t, parser, 26)

	parser, err = NewCSVParser(
		&cfg,
		NewStringReader(`"\0\b\n\r\t\Z\\\  \c\'\""`),
		int64(ReadBlockSize), false, false)
	require.NoError(t, err)

	row, err = parser.Read()
	require.NoError(t, err)
	require.Equal(t, []Field{
		newStringField(string([]byte{0, '\b', '\n', '\r', '\t', 26, '\\', ' ', ' ', 'c', '\'', '"'}), false),
	}, row)

	cfg.UnescapedQuote = true
	parser, err = NewCSVParser(
		&cfg,
		NewStringReader(`3,"a string containing a " quote",102.20
`),
		int64(ReadBlockSize), false, false)
	require.NoError(t, err)

	row, err = parser.Read()
	require.NoError(t, err)
	require.Equal(t, []Field{
		newStringField("3", false),
		newStringField(`a string containing a " quote`, false),
		newStringField("102.20", false),
	}, row)

	parser, err = NewCSVParser(
		&cfg,
		NewStringReader(`3,"a string containing a " quote","102.20"`),
		int64(ReadBlockSize), false, false)
	require.NoError(t, err)

	row, err = parser.Read()
	require.NoError(t, err)
	require.Equal(t, []Field{
		newStringField("3", false),
		newStringField(`a string containing a " quote`, false),
		newStringField("102.20", false),
	}, row)

	parser, err = NewCSVParser(
		&cfg,
		NewStringReader(`"a"b",c"d"e`),
		int64(ReadBlockSize), false, false)
	require.NoError(t, err)

	row, err = parser.Read()
	require.NoError(t, err)
	require.Equal(t, []Field{
		newStringField(`a"b`, false),
		newStringField(`c"d"e`, false),
	}, row)
}

func TestCustomEscapeChar(t *testing.T) {
	cfg := CSVConfig{
		FieldsTerminatedBy: ",",
		FieldsEnclosedBy:   `"`,
		FieldsEscapedBy:    `!`,
		NotNull:            false,
		Null:               []string{`!N`},
	}

	parser, err := NewCSVParser(&cfg, NewStringReader(`"!"","!!","!\"
"!
",!N,!!N`), int64(ReadBlockSize), false, false)
	require.NoError(t, err)

	var row []Field

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, []Field{
		newStringField(`"`, false),
		newStringField(`!`, false),
		newStringField(`\`, false),
	}, row)
	assertPosEqual(t, parser, 15)

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, []Field{
		newStringField("\n", false),
		newStringField(`!N`, true),
		newStringField(`!N`, false),
	}, row)
	assertPosEqual(t, parser, 26)

	cfg = CSVConfig{
		FieldsTerminatedBy: ",",
		FieldsEnclosedBy:   `"`,
		FieldsEscapedBy:    ``,
		NotNull:            false,
		Null:               []string{`NULL`},
	}

	parser, err = NewCSVParser(
		&cfg,
		NewStringReader(`"{""itemRangeType"":0,""itemContainType"":0,""shopRangeType"":1,""shopJson"":""[{\""id\"":\""A1234\"",\""shopName\"":\""AAAAAA\""}]""}"`),
		int64(ReadBlockSize), false, false)
	require.NoError(t, err)

	row, err = parser.Read()
	require.Nil(t, err)
	require.Equal(t, []Field{
		newStringField(`{"itemRangeType":0,"itemContainType":0,"shopRangeType":1,"shopJson":"[{\"id\":\"A1234\",\"shopName\":\"AAAAAA\"}]"}`, false),
	}, row)
}
