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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/stretchr/testify/require"
)

// TODO: rewrite test case

func NewStringReader(str string) io.Reader {
	return strings.NewReader(str)
}

func newStringField(val string, isNull bool, hasStringQuote bool) Field {
	return Field{
		Val:            val,
		IsNull:         isNull,
		HasStringQuote: hasStringQuote,
	}
}
func assertPosEqual(t *testing.T, parser *CSVParser, pos int64) {
	require.Equal(t, parser.Pos(), pos)
}
func tpchDatums() [][]Field {
	datums := make([][]Field, 0, 3)
	datums = append(datums, []Field{
		newStringField("1", false, false),
		newStringField("goldenrod lavender spring chocolate lace", false, false),
		newStringField("Manufacturer#1", false, false),
		newStringField("Brand#13", false, false),
		newStringField("PROMO BURNISHED COPPER", false, false),
		newStringField("7", false, false),
		newStringField("JUMBO PKG", false, false),
		newStringField("901.00", false, false),
		newStringField("ly. slyly ironi", false, false),
	})
	datums = append(datums, []Field{
		newStringField("2", false, false),
		newStringField("blush thistle blue yellow saddle", false, false),
		newStringField("Manufacturer#1", false, false),
		newStringField("Brand#13", false, false),
		newStringField("LARGE BRUSHED BRASS", false, false),
		newStringField("1", false, false),
		newStringField("LG CASE", false, false),
		newStringField("902.00", false, false),
		newStringField("lar accounts amo", false, false),
	})
	datums = append(datums, []Field{
		newStringField("3", false, false),
		newStringField("spring green yellow purple cornsilk", false, false),
		newStringField("Manufacturer#4", false, false),
		newStringField("Brand#42", false, false),
		newStringField("STANDARD POLISHED BRASS", false, false),
		newStringField("21", false, false),
		newStringField("WRAP CASE", false, false),
		newStringField("903.00", false, false),
		newStringField("egular deposits hag", false, false),
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

	parser, err := NewCSVParser(&cfg, reader, int64(ReadBlockSize), false)
	require.NoError(t, err)

	var row []Field

	row, err = parser.Read(nil)
	require.Nil(t, err)
	require.Equal(t, datums[0], row)
	require.Equal(t, parser.Pos(), int64(126))
	assertPosEqual(t, parser, 126)

	row, err = parser.Read(nil)
	require.Nil(t, err)
	require.Equal(t, datums[1], row)
	assertPosEqual(t, parser, 241)

	row, err = parser.Read(nil)
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
		parser, err := NewCSVParser(&cfg, reader, int64(ReadBlockSize), false)
		if fmt.Sprint(err) == "invalid input: invalid field delimiter" {
			continue
		}
		require.NoError(t, err)

		for i, expectedParserPos := range allExpectedParserPos {
			row, err := parser.Read(nil)
			require.Nil(t, err)
			require.Equal(t, len(datums[i]), len(row))
			for j := range row {
				require.Equal(t, datums[i][j].Val, row[j].Val)
				require.Equal(t, datums[i][j].IsNull, row[j].IsNull)
			}
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

	parser, err := NewCSVParser(&cfg, reader, int64(ReadBlockSize), false)
	require.NoError(t, err)

	var row []Field

	row, err = parser.Read(nil)
	require.Nil(t, err)
	require.Equal(t, datums[0], row)
	require.Equal(t, parser.Pos(), int64(127))
	assertPosEqual(t, parser, 127)

	row, err = parser.Read(nil)
	require.Nil(t, err)
	require.Equal(t, datums[1], row)
	assertPosEqual(t, parser, 242)

	row, err = parser.Read(nil)
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

	parser, err := NewCSVParser(&cfg, NewStringReader("aaa,bbb,ccc\nzzz,yyy,xxx\n"), int64(ReadBlockSize), false)
	require.NoError(t, err)

	var row []Field

	row, err = parser.Read(nil)
	require.Nil(t, err)
	require.Equal(t, []Field{
		newStringField("aaa", false, false),
		newStringField("bbb", false, false),
		newStringField("ccc", false, false),
	}, row)
	assertPosEqual(t, parser, 12)

	row, err = parser.Read(nil)
	require.Nil(t, err)
	require.Equal(t, []Field{
		newStringField("zzz", false, false),
		newStringField("yyy", false, false),
		newStringField("xxx", false, false),
	}, row)
	assertPosEqual(t, parser, 24)

	// example 2, no trailing new lines

	parser, err = NewCSVParser(&cfg, NewStringReader("aaa,bbb,ccc\nzzz,yyy,xxx"), int64(ReadBlockSize), false)
	require.NoError(t, err)

	row, err = parser.Read(nil)
	require.Nil(t, err)
	require.Equal(t, []Field{
		newStringField("aaa", false, false),
		newStringField("bbb", false, false),
		newStringField("ccc", false, false),
	}, row)
	assertPosEqual(t, parser, 12)

	row, err = parser.Read(nil)
	require.Nil(t, err)
	require.Equal(t, []Field{
		newStringField("zzz", false, false),
		newStringField("yyy", false, false),
		newStringField("xxx", false, false),
	}, row)
	assertPosEqual(t, parser, 23)

	// example 5, quoted fields

	parser, err = NewCSVParser(&cfg, NewStringReader(`"aaa","bbb","ccc"`+"\nzzz,yyy,xxx"), int64(ReadBlockSize), false)
	require.NoError(t, err)

	row, err = parser.Read(nil)
	require.Nil(t, err)
	require.Equal(t, []Field{
		newStringField("aaa", false, true),
		newStringField("bbb", false, true),
		newStringField("ccc", false, true),
	}, row)
	assertPosEqual(t, parser, 18)

	row, err = parser.Read(nil)
	require.Nil(t, err)
	require.Equal(t, []Field{
		newStringField("zzz", false, false),
		newStringField("yyy", false, false),
		newStringField("xxx", false, false),
	}, row)
	assertPosEqual(t, parser, 29)

	// example 6, line breaks within fields

	parser, err = NewCSVParser(&cfg, NewStringReader(`"aaa","b
bb","ccc"
zzz,yyy,xxx`), int64(ReadBlockSize), false)
	require.NoError(t, err)

	row, err = parser.Read(nil)
	require.Nil(t, err)
	require.Equal(t, []Field{
		newStringField("aaa", false, true),
		newStringField("b\nbb", false, true),
		newStringField("ccc", false, true),
	}, row)
	assertPosEqual(t, parser, 19)

	row, err = parser.Read(nil)
	require.Nil(t, err)
	require.Equal(t, []Field{
		newStringField("zzz", false, false),
		newStringField("yyy", false, false),
		newStringField("xxx", false, false),
	}, row)
	assertPosEqual(t, parser, 30)

	// example 7, quote escaping

	parser, err = NewCSVParser(&cfg, NewStringReader(`"aaa","b""bb","ccc"`), int64(ReadBlockSize), false)
	require.NoError(t, err)

	row, err = parser.Read(nil)
	require.Nil(t, err)
	require.Equal(t, []Field{
		newStringField("aaa", false, true),
		newStringField("b\"bb", false, true),
		newStringField("ccc", false, true),
	}, row)
	assertPosEqual(t, parser, 19)

	//  example 8, read head columns
	parser, err = NewCSVParser(&cfg, NewStringReader(`"aaa","bbb","ccc"`+"\nzzz,yyy,xxx"), int64(ReadBlockSize), true)
	require.NoError(t, err)

	row, err = parser.Read(nil)
	require.Nil(t, err)
	require.Equal(t, 0, len(parser.columns))
	require.Equal(t, []Field{
		newStringField("zzz", false, false),
		newStringField("yyy", false, false),
		newStringField("xxx", false, false),
	}, row)

	cfg.HeaderSchemaMatch = true
	parser, err = NewCSVParser(&cfg, NewStringReader(`"aaa","bbb","ccc"`+"\nzzz,yyy,xxx"), int64(ReadBlockSize), true)
	require.NoError(t, err)

	row, err = parser.Read(nil)
	require.Nil(t, err)
	require.Equal(t, []string{"aaa", "bbb", "ccc"}, parser.columns)
	require.Equal(t, []Field{
		newStringField("zzz", false, false),
		newStringField("yyy", false, false),
		newStringField("xxx", false, false),
	}, row)

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
",\N,\\N`), int64(ReadBlockSize), false)
	require.NoError(t, err)

	var row []Field

	row, err = parser.Read(nil)
	require.NoError(t, err)
	require.Equal(t, []Field{
		newStringField(`"`, false, true),
		newStringField(`\`, false, true),
		newStringField("?", false, true),
	}, row)

	assertPosEqual(t, parser, 15)

	row, err = parser.Read(nil)
	require.NoError(t, err)

	require.Equal(t, []Field{
		newStringField("\n", false, true),
		newStringField("\\N", true, false),
		newStringField(`\N`, false, false),
	}, row)

	assertPosEqual(t, parser, 26)

	parser, err = NewCSVParser(
		&cfg,
		NewStringReader(`"\0\b\n\r\t\Z\\\  \c\'\""`),
		int64(ReadBlockSize), false)
	require.NoError(t, err)

	row, err = parser.Read(nil)
	require.NoError(t, err)
	require.Equal(t, []Field{
		newStringField(string([]byte{0, '\b', '\n', '\r', '\t', 26, '\\', ' ', ' ', 'c', '\'', '"'}), false, true),
	}, row)

	cfg.UnescapedQuote = true
	parser, err = NewCSVParser(
		&cfg,
		NewStringReader(`3,"a string containing a " quote",102.20
`),
		int64(ReadBlockSize), false)
	require.NoError(t, err)

	row, err = parser.Read(nil)
	require.NoError(t, err)
	require.Equal(t, []Field{
		newStringField("3", false, false),
		newStringField(`a string containing a " quote`, false, true),
		newStringField("102.20", false, false),
	}, row)

	parser, err = NewCSVParser(
		&cfg,
		NewStringReader(`3,"a string containing a " quote","102.20"`),
		int64(ReadBlockSize), false)
	require.NoError(t, err)

	row, err = parser.Read(nil)
	require.NoError(t, err)
	require.Equal(t, []Field{
		newStringField("3", false, false),
		newStringField(`a string containing a " quote`, false, true),
		newStringField("102.20", false, true),
	}, row)

	parser, err = NewCSVParser(
		&cfg,
		NewStringReader(`"a"b",c"d"e`),
		int64(ReadBlockSize), false)
	require.NoError(t, err)

	row, err = parser.Read(nil)
	require.NoError(t, err)
	require.Equal(t, []Field{
		newStringField(`a"b`, false, true),
		newStringField(`c"d"e`, false, false),
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
",!N,!!N`), int64(ReadBlockSize), false)
	require.NoError(t, err)

	var row []Field

	row, err = parser.Read(nil)
	require.Nil(t, err)
	require.Equal(t, []Field{
		newStringField(`"`, false, true),
		newStringField(`!`, false, true),
		newStringField(`\`, false, true),
	}, row)
	assertPosEqual(t, parser, 15)

	row, err = parser.Read(nil)
	require.Nil(t, err)
	require.Equal(t, []Field{
		newStringField("\n", false, true),
		newStringField(`!N`, true, false),
		newStringField(`!N`, false, false),
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
		int64(ReadBlockSize), false)
	require.NoError(t, err)

	row, err = parser.Read(nil)
	require.Nil(t, err)
	require.Equal(t, []Field{
		newStringField(`{"itemRangeType":0,"itemContainType":0,"shopRangeType":1,"shopJson":"[{\"id\":\"A1234\",\"shopName\":\"AAAAAA\"}]"}`, false, true),
	}, row)
}

// TestCommentEmptyRecordNoPanic: an all-empty-string row (e.g. `""\n` or
// `"",""\n`) leaves recordBuffer empty; the comment check must not index
// recordBuffer[0] out of range.
func TestCommentEmptyRecordNoPanic(t *testing.T) {
	cfg := CSVConfig{
		FieldsTerminatedBy: ",",
		FieldsEnclosedBy:   `"`,
		Comment:            "#",
	}
	parser, err := NewCSVParser(&cfg, NewStringReader("\"\",\"\"\n1,2\n"), int64(ReadBlockSize), false)
	require.NoError(t, err)

	row, err := parser.Read(nil)
	require.NoError(t, err)
	require.Len(t, row, 2)
	require.Equal(t, "", row[0].Val)
	require.Equal(t, "", row[1].Val)

	row, err = parser.Read(nil)
	require.NoError(t, err)
	require.Equal(t, "1", row[0].Val)
	require.Equal(t, "2", row[1].Val)
}

// TestCommentDisabled: with no comment marker (empty Comment, the default), a
// first field starting with '#' is data, not a skipped comment.
func TestCommentDisabled(t *testing.T) {
	cfg := CSVConfig{FieldsTerminatedBy: ",", FieldsEnclosedBy: `"`} // Comment == ""
	parser, err := NewCSVParser(&cfg, NewStringReader("#lost,1\nkept,2\n"), int64(ReadBlockSize), false)
	require.NoError(t, err)

	row, err := parser.Read(nil)
	require.NoError(t, err)
	require.Equal(t, "#lost", row[0].Val)
	require.Equal(t, "1", row[1].Val)
}

// TestCommentRawFirstByte: the comment marker is matched on the line's first
// RAW byte (before unquoting). An enclosed "#x" value begins with the quote
// char, so it is data; only an unquoted line literally starting with '#' is a
// skipped comment.
func TestCommentRawFirstByte(t *testing.T) {
	cfg := CSVConfig{FieldsTerminatedBy: ",", FieldsEnclosedBy: `"`, Comment: "#"}
	parser, err := NewCSVParser(&cfg,
		NewStringReader("\"#x\",1\n# a real comment\nkept,2\n"),
		int64(ReadBlockSize), false)
	require.NoError(t, err)

	// quoted "#x" is data, not a comment
	row, err := parser.Read(nil)
	require.NoError(t, err)
	require.Equal(t, "#x", row[0].Val)
	require.Equal(t, "1", row[1].Val)

	// the unquoted '# a real comment' line is skipped; the next data row follows
	row, err = parser.Read(nil)
	require.NoError(t, err)
	require.Equal(t, "kept", row[0].Val)
	require.Equal(t, "2", row[1].Val)

	_, err = parser.Read(nil)
	require.ErrorIs(t, err, io.EOF)
}

// TestParserReset: one parser reused across independent inputs must show no
// cross-input bleed — including after a partial/failed read — and must keep
// its configuration.
func TestParserReset(t *testing.T) {
	cfg := &CSVConfig{
		FieldsTerminatedBy: ",",
		FieldsEnclosedBy:   `"`,
		LinesTerminatedBy:  "\n",
	}
	parser, err := NewCSVParser(cfg, strings.NewReader(""), ReadBlockSize, false)
	require.NoError(t, err)

	readOne := func(input string) ([]Field, error) {
		parser.Reset(strings.NewReader(input))
		return parser.Read(nil)
	}

	row, err := readOne("a,b,c")
	require.NoError(t, err)
	require.Len(t, row, 3)
	require.Equal(t, "a", row[0].Val)

	// an unterminated quoted field fails; the NEXT input must be clean
	_, err = readOne(`"unterminated`)
	require.Error(t, err)
	row, err = readOne("x,y")
	require.NoError(t, err)
	require.Len(t, row, 2)
	require.Equal(t, "x", row[0].Val)
	require.Equal(t, "y", row[1].Val)

	// leftover unread content of a previous input never bleeds into the next
	row, err = readOne("1,2\n3,4\n5,6")
	require.NoError(t, err)
	require.Equal(t, "1", row[0].Val)
	row, err = readOne("7,8")
	require.NoError(t, err)
	require.Equal(t, "7", row[0].Val)
	require.Equal(t, "8", row[1].Val)
	_, err = parser.Read(nil)
	require.ErrorIs(t, err, io.EOF)
}

// RecordLine reports the physical line a record starts on, which is what an
// external scan surfaces as __mo_file_line. A record whose quoted field spans
// several lines must report its FIRST line, and the counter must survive
// records that are read across buffer refills.
func TestRecordLine(t *testing.T) {
	cfg := CSVConfig{
		FieldsTerminatedBy: ",",
		FieldsEnclosedBy:   `"`,
	}

	t.Run("one record per line", func(t *testing.T) {
		parser, err := NewCSVParser(&cfg, NewStringReader("a,1\nb,2\nc,3\n"), int64(ReadBlockSize), false)
		require.NoError(t, err)
		for _, want := range []int64{1, 2, 3} {
			_, err = parser.Read(nil)
			require.NoError(t, err)
			require.Equal(t, want, parser.RecordLine())
		}
	})

	t.Run("multi-line quoted field reports its first line", func(t *testing.T) {
		// line 1: a,"x            line 4: c,3
		// lines 2-3 are inside the quoted field
		parser, err := NewCSVParser(&cfg, NewStringReader("a,\"x\ny\nz\"\nc,3\n"), int64(ReadBlockSize), false)
		require.NoError(t, err)

		_, err = parser.Read(nil)
		require.NoError(t, err)
		require.Equal(t, int64(1), parser.RecordLine(), "record starting on line 1 spans lines 1-3")

		_, err = parser.Read(nil)
		require.NoError(t, err)
		require.Equal(t, int64(4), parser.RecordLine(), "the next record starts on line 4")
	})

	t.Run("skipped blank lines still count", func(t *testing.T) {
		// line 1: a,1   lines 2-3 blank   line 4: b,2   line 5 whitespace
		// line 6: c,3. Whitespace is field data; only lines 2-3 are skipped.
		parser, err := NewCSVParser(&cfg, NewStringReader("a,1\n\n\nb,2\n   \nc,3\n"), int64(ReadBlockSize), false)
		require.NoError(t, err)
		for _, want := range []int64{1, 4, 5, 6} {
			row, err := parser.Read(nil)
			require.NoError(t, err)
			require.Equal(t, want, parser.RecordLine())
			if want == 5 {
				require.Equal(t, []Field{newStringField("   ", false, false)}, row)
			}
		}
	})

	// A quoted field spanning more bytes than one read block drains several
	// blocks into the record. Counting only the last block's prefix loses every
	// newline in the blocks before it -- which is exactly the record shape
	// __mo_file_line exists to report.
	t.Run("newlines are counted in every drained block", func(t *testing.T) {
		parser, err := NewCSVParser(&cfg,
			NewStringReader("a,\"xxxxxxxx\nxxxxxxxx\nxxxxxxxx\"\nb,2\n"), 4, false)
		require.NoError(t, err)

		_, err = parser.Read(nil)
		require.NoError(t, err)
		require.Equal(t, int64(1), parser.RecordLine(), "the quoted record starts on line 1")

		_, err = parser.Read(nil)
		require.NoError(t, err)
		require.Equal(t, int64(4), parser.RecordLine(),
			"the quoted field spans lines 1-3, so the next record is on line 4")
	})

	// LINES STARTING BY reads ahead a whole line, then pushes the unmatched
	// remainder back and rewinds pos. The counter has to rewind with it, or the
	// newline consumed by the lookahead is counted twice.
	t.Run("lines starting by does not double count", func(t *testing.T) {
		startCfg := cfg
		startCfg.LinesStartingBy = "xxx"
		startCfg.LinesTerminatedBy = "\n"
		parser, err := NewCSVParser(&startCfg, NewStringReader("xxxa,1\nxxxb,2\n"), int64(ReadBlockSize), false)
		require.NoError(t, err)
		for _, want := range []int64{1, 2} {
			_, err = parser.Read(nil)
			require.NoError(t, err)
			require.Equal(t, want, parser.RecordLine())
		}
	})

	// Each skip path must move the candidate record start forward, while a
	// multi-line quoted record must keep the line where its first field began.
	for _, tc := range []struct {
		name           string
		input          string
		comment        string
		startingBy     string
		skipWhitespace bool
		lines          []int64
		rows           [][]string
	}{
		{
			name:  "comments before and between records",
			input: "#h\na,1\n#c\nb,2\n", comment: "#",
			lines: []int64{2, 4}, rows: [][]string{{"a", "1"}, {"b", "2"}},
		},
		{
			name:  "consecutive comments and unterminated trailing comment",
			input: "REMh\nREMh2\na,1\nREMtail", comment: "REM",
			lines: []int64{3}, rows: [][]string{{"a", "1"}},
		},
		{
			name:  "starting by skips nonmatching lines",
			input: "junk1\njunk2\nxxxa,1\njunk3\nxxxb,2", startingBy: "xxx",
			lines: []int64{3, 5}, rows: [][]string{{"a", "1"}, {"b", "2"}},
		},
		{
			name:  "comments and blanks before multiline record",
			input: "#h\n\n   \n#c\na,\"x\ny\"\n#d\nb,2", comment: "#",
			lines: []int64{3, 5, 8}, rows: [][]string{{"   "}, {"a", "x\ny"}, {"b", "2"}},
		},
		{
			name:  "opt-in whitespace skipping before multiline record",
			input: "#h\n\n   \n#c\na,\"x\ny\"\n#d\nb,2", comment: "#", skipWhitespace: true,
			lines: []int64{5, 8}, rows: [][]string{{"a", "x\ny"}, {"b", "2"}},
		},
		{
			name:    "comments and starting by before multiline record",
			input:   "#h\njunk\nxxxa,\"x\n#inside\"\n#c\njunk2\nxxxb,2\njunk tail",
			comment: "#", startingBy: "xxx",
			lines: []int64{3, 7}, rows: [][]string{{"a", "x\n#inside"}, {"b", "2"}},
		},
		{
			name:  "CRLF comments and starting by",
			input: "#h\r\njunk\r\nxxxa,1\r\n#c\r\nxxxb,2\r\n", comment: "#", startingBy: "xxx",
			lines: []int64{3, 5}, rows: [][]string{{"a", "1"}, {"b", "2"}},
		},
		{
			name:  "quoted comment marker is data",
			input: "#h\n\"#data\",1\nb,2\n", comment: "#",
			lines: []int64{2, 3}, rows: [][]string{{"#data", "1"}, {"b", "2"}},
		},
		{
			name:  "all input skipped",
			input: "#h\njunk\n#tail", comment: "#", startingBy: "xxx",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, blockSize := range []int64{4, ReadBlockSize} {
				t.Run(fmt.Sprintf("block=%d", blockSize), func(t *testing.T) {
					skipCfg := cfg
					skipCfg.Comment = tc.comment
					skipCfg.LinesStartingBy = tc.startingBy
					skipCfg.LinesTerminatedBy = "\n"
					skipCfg.SkipWhitespaceRecords = tc.skipWhitespace
					parser, err := NewCSVParser(&skipCfg, NewStringReader(tc.input), blockSize, false)
					require.NoError(t, err)
					for i, want := range tc.lines {
						row, err := parser.Read(nil)
						require.NoError(t, err)
						require.Equal(t, want, parser.RecordLine())
						require.Len(t, row, len(tc.rows[i]))
						for j, value := range tc.rows[i] {
							require.Equal(t, value, row[j].Val)
						}
					}
					_, err = parser.Read(nil)
					require.ErrorIs(t, err, io.EOF)
				})
			}
		})
	}

	t.Run("reset after skipped input", func(t *testing.T) {
		skipCfg := cfg
		skipCfg.Comment = "#"
		skipCfg.LinesStartingBy = "xxx"
		skipCfg.LinesTerminatedBy = "\n"
		parser, err := NewCSVParser(&skipCfg, NewStringReader("#h\njunk\nxxxa,1\n"), 4, false)
		require.NoError(t, err)
		_, err = parser.Read(nil)
		require.NoError(t, err)
		require.Equal(t, int64(3), parser.RecordLine())

		parser.Reset(NewStringReader("xxxb,2\n"))
		row, err := parser.Read(nil)
		require.NoError(t, err)
		require.Equal(t, int64(1), parser.RecordLine())
		require.Equal(t, []Field{newStringField("b", false, false), newStringField("2", false, false)}, row)
		_, err = parser.Read(nil)
		require.ErrorIs(t, err, io.EOF)
	})

	t.Run("counter is exact across a small read block", func(t *testing.T) {
		// a tiny block size forces the refill paths (readUntil's loop) that
		// advance position in bulk rather than byte by byte
		var sb strings.Builder
		for i := 0; i < 50; i++ {
			sb.WriteString("aaaaaaaaaa,bbbbbbbbbb\n")
		}
		parser, err := NewCSVParser(&cfg, NewStringReader(sb.String()), 8, false)
		require.NoError(t, err)
		for i := int64(1); i <= 50; i++ {
			_, err = parser.Read(nil)
			require.NoError(t, err)
			require.Equal(t, i, parser.RecordLine(), "record %d", i)
		}
	})
}

func TestIsSyntaxError(t *testing.T) {
	for _, err := range []error{errUnterminatedQuotedField, errDanglingBackslash, errUnexpectedQuoteField} {
		require.True(t, IsSyntaxError(err))
		require.True(t, IsSyntaxError(fmt.Errorf("wrapped: %w", err)))
	}
	for _, err := range []error{nil, io.EOF, io.ErrUnexpectedEOF,
		moerr.NewInvalidInputNoCtx("storage failure"),
		moerr.NewInternalErrorNoCtx("row size limit"),
	} {
		require.False(t, IsSyntaxError(err))
	}
}

// crStripIssueConfig returns the LOAD DATA configuration from issue #30462:
// an alternate field terminator plus ENCLOSED BY/escape/UnescapedQuote, LF line
// terminator. withQuote=false removes the enclosure for the quote-independent
// variants.
func crStripIssueConfig(withQuote bool) *CSVConfig {
	cfg := &CSVConfig{
		FieldsTerminatedBy: "0",
		LinesTerminatedBy:  "\n",
		FieldsEscapedBy:    `\`,
		Null:               []string{`\N`},
	}
	if withQuote {
		cfg.FieldsEnclosedBy = "~"
		cfg.UnescapedQuote = true
	}
	return cfg
}

// TestTrailingCRBelongsToSealedField covers issue #30462: the trailing-CR
// strip at end of record must only remove a CR that belongs to the current
// (not yet bounded) field. A CR before a field separator is content of an
// earlier field whose boundary has already been recorded; dropping it used to
// push that boundary past the end of the record buffer and panic in readRow.
func TestTrailingCRBelongsToSealedField(t *testing.T) {
	for _, tc := range []struct {
		name      string
		withQuote bool
		input     string
		row       []Field
	}{
		// The original repro: ~ is literal mid-field text, the CR before the
		// separator stays in field 1, the trailing empty field is preserved.
		{"issue repro", true, "a~\r0", []Field{
			newStringField("a~\r", false, false),
			newStringField("", false, false),
		}},
		// Without an enclosure the same byte pattern must behave identically.
		{"no quote, EOF end", false, "a\r0", []Field{
			newStringField("a\r", false, false),
			newStringField("", false, false),
		}},
		{"no quote, LF end", false, "a\r0\n", []Field{
			newStringField("a\r", false, false),
			newStringField("", false, false),
		}},
		// Repeated boundaries: CR kept in field 1, both empty fields kept.
		{"two empty trailing fields", false, "a\r00\n", []Field{
			newStringField("a\r", false, false),
			newStringField("", false, false),
			newStringField("", false, false),
		}},
		// Current-field CR (CRLF physical line) is still stripped; sealed-field
		// CR is not.
		{"CRLF with full last field", false, "a0b\r\n", []Field{
			newStringField("a", false, false),
			newStringField("b", false, false),
		}},
		{"CRLF with empty last field", false, "a0\r\n", []Field{
			newStringField("a", false, false),
			newStringField("", false, false),
		}},
		{"CRLF, sealed CR and current CR", false, "a\r0\r\n", []Field{
			newStringField("a\r", false, false),
			newStringField("", false, false),
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parser, err := NewCSVParser(crStripIssueConfig(tc.withQuote), NewStringReader(tc.input), int64(ReadBlockSize), false)
			require.NoError(t, err)
			row, err := parser.Read(nil)
			require.NoError(t, err)
			require.Equal(t, tc.row, row)
			_, err = parser.Read(nil)
			require.ErrorIs(t, err, io.EOF)
		})
	}

	t.Run("TrimLastSep drops only the trailing empty field", func(t *testing.T) {
		cfg := crStripIssueConfig(true)
		cfg.TrimLastSep = true
		parser, err := NewCSVParser(cfg, NewStringReader("a~\r0"), int64(ReadBlockSize), false)
		require.NoError(t, err)
		row, err := parser.Read(nil)
		require.NoError(t, err)
		require.Equal(t, []Field{
			newStringField("a~\r", false, false),
		}, row)
		_, err = parser.Read(nil)
		require.ErrorIs(t, err, io.EOF)
	})

	t.Run("header consumer reads sealed CR", func(t *testing.T) {
		cfg := crStripIssueConfig(true)
		cfg.HeaderSchemaMatch = true
		parser, err := NewCSVParser(cfg, NewStringReader("a~\r0\nx0y\n"), int64(ReadBlockSize), true)
		require.NoError(t, err)
		row, err := parser.Read(nil)
		require.NoError(t, err)
		require.Equal(t, []Field{
			newStringField("x", false, false),
			newStringField("y", false, false),
		}, row)
		require.Equal(t, []string{"a~\r", ""}, parser.Columns())
		_, err = parser.Read(nil)
		require.ErrorIs(t, err, io.EOF)
	})

	t.Run("a quote that opens a field still reports unterminated", func(t *testing.T) {
		parser, err := NewCSVParser(crStripIssueConfig(true), NewStringReader("~\r0"), int64(ReadBlockSize), false)
		require.NoError(t, err)
		_, err = parser.Read(nil)
		require.ErrorIs(t, err, errUnterminatedQuotedField)
		require.True(t, IsSyntaxError(err))
	})

	t.Run("strip and reuse across a small read block", func(t *testing.T) {
		// A 1-block buffer (5 bytes) ends at the first CR. Refilling seals
		// that field; the next record then exercises ordinary CRLF stripping.
		parser, err := NewCSVParser(crStripIssueConfig(true), NewStringReader("abc~\r0\ny0z\r\n"), 1, false)
		require.NoError(t, err)
		for _, want := range [][]Field{
			{newStringField("abc~\r", false, false), newStringField("", false, false)},
			{newStringField("y", false, false), newStringField("z", false, false)},
		} {
			row, err := parser.Read(nil)
			require.NoError(t, err)
			require.Equal(t, want, row)
		}
		_, err = parser.Read(nil)
		require.ErrorIs(t, err, io.EOF)
	})
}

func TestWhitespaceOnlyRecord(t *testing.T) {
	for _, tc := range []struct {
		name, input, terminator, comment, startingBy string
		allowEmpty                                   bool
		rows                                         [][]Field
		lines                                        []int64
	}{
		{name: "lf", input: "\n \n\t\n \t \nx\n", rows: [][]Field{
			{newStringField(" ", false, false)}, {newStringField("\t", false, false)},
			{newStringField(" \t ", false, false)}, {newStringField("x", false, false)}}, lines: []int64{2, 3, 4, 5}},
		{name: "explicit crlf", terminator: "\r\n", input: "\r\n \r\n\t\r\nx\r\n", rows: [][]Field{
			{newStringField(" ", false, false)}, {newStringField("\t", false, false)}, {newStringField("x", false, false)}}, lines: []int64{2, 3, 4}},
		{name: "eof", input: " \nx\n\t", rows: [][]Field{
			{newStringField(" ", false, false)}, {newStringField("x", false, false)}, {newStringField("\t", false, false)}}, lines: []int64{1, 2, 3}},
		{name: "unicode whitespace", input: "\u00a0\n\u2003\n", rows: [][]Field{
			{newStringField("\u00a0", false, false)}, {newStringField("\u2003", false, false)}}, lines: []int64{1, 2}},
		{name: "quoted controls", terminator: "\n", input: "\"\"\n\" \"\n\"\r\"\n", rows: [][]Field{
			{newStringField("", false, true)}, {newStringField(" ", false, true)}, {newStringField("\r", false, true)}}, lines: []int64{1, 2, 3}},
		{name: "allow empty", allowEmpty: true, input: "\n \n", rows: [][]Field{
			{newStringField("", false, false)}, {newStringField(" ", false, false)}}, lines: []int64{1, 2}},
		{name: "allow empty crlf", allowEmpty: true, terminator: "\r\n", input: "\r\n \r\n", rows: [][]Field{
			{newStringField("", false, false)}, {newStringField(" ", false, false)}}, lines: []int64{1, 2}},
		{name: "comments", comment: "#", input: "# skip\n \n# skip\nx\n", rows: [][]Field{
			{newStringField(" ", false, false)}, {newStringField("x", false, false)}}},
		{name: "starting by", startingBy: "p:", terminator: "\n", input: "skip\np: \np:x\n", rows: [][]Field{
			{newStringField(" ", false, false)}, {newStringField("x", false, false)}}},
		{name: "multiple columns", input: " ,\n,\n", rows: [][]Field{
			{newStringField(" ", false, false), newStringField("", false, false)},
			{newStringField("", false, false), newStringField("", false, false)}}, lines: []int64{1, 2}},
		{name: "cr at eof", terminator: "\n", input: "\r", rows: [][]Field{{newStringField("\r", false, false)}}, lines: []int64{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := CSVConfig{FieldsTerminatedBy: ",", FieldsEnclosedBy: `"`, LinesTerminatedBy: tc.terminator,
				Comment: tc.comment, LinesStartingBy: tc.startingBy, AllowEmptyLine: tc.allowEmpty}
			parser, err := NewCSVParser(&cfg, strings.NewReader(tc.input), 1, false)
			require.NoError(t, err)
			var row []Field
			for i, want := range tc.rows {
				row, err = parser.Read(row)
				require.NoError(t, err)
				require.Equal(t, want, row)
				if tc.lines != nil {
					require.Equal(t, tc.lines[i], parser.RecordLine())
				}
			}
			_, err = parser.Read(row)
			require.ErrorIs(t, err, io.EOF)
		})
	}
}

// maxRecordTestCfg returns the LOAD DATA configuration with an explicit
// per-record size bound, so boundary tests run on a few hundred bytes instead
// of MiB-scale inputs.
func maxRecordTestCfg(limit int64) *CSVConfig {
	return &CSVConfig{
		FieldsTerminatedBy: ",",
		FieldsEnclosedBy:   `"`,
		FieldsEscapedBy:    `\`,
		LinesTerminatedBy:  "\n",
		Null:               []string{`\N`},
		UnescapedQuote:     true,
		MaxRecordSize:      limit,
	}
}

// TestMaxRecordSizeBound pins the single-record raw-size bound: it applies to
// the whole record identically no matter how quotes, escapes, field
// separators or read blocks fragment the row — which the former per-fragment
// check in readUntil did not. Tests use an injected small bound; blockBufSize
// 16 gives 16*BufferSizeScale=80-byte read blocks so every case also crosses
// refill boundaries.
func TestMaxRecordSizeBound(t *testing.T) {
	const (
		limit     int64 = 96
		blockSize int64 = 16
	)

	readRow := func(t *testing.T, cfg *CSVConfig, input string) []Field {
		t.Helper()
		parser, err := NewCSVParser(cfg, NewStringReader(input), blockSize, false)
		require.NoError(t, err)
		row, err := parser.Read(nil)
		require.NoError(t, err)
		return row
	}
	assertLimitError := func(t *testing.T, cfg *CSVConfig, input string) {
		t.Helper()
		parser, err := NewCSVParser(cfg, NewStringReader(input), blockSize, false)
		require.NoError(t, err)
		_, err = parser.Read(nil)
		require.Error(t, err)
		require.ErrorContains(t, err, "largest-entry-limit")
		require.NotContains(t, err.Error(), "txn-entry-size-limit",
			"the bound is the parser's own record bound, it never was the txn entry limit")
		require.False(t, IsSyntaxError(err),
			"oversized records must fail loading, not be skipped as tolerated syntax errors")
	}

	// Under / equal / above the record bound on a plain unquoted field: a
	// single-field row weighs content bytes + 1 (its only field frame), so
	// the inclusive boundary sits at limit-1 content bytes.
	t.Run("unquoted field below at above bound", func(t *testing.T) {
		row := readRow(t, maxRecordTestCfg(limit), strings.Repeat("x", int(limit)-2)+"\n")
		require.Len(t, row, 1)
		require.Len(t, row[0].Val, int(limit)-2)

		row = readRow(t, maxRecordTestCfg(limit), strings.Repeat("x", int(limit)-1)+"\n")
		require.Len(t, row[0].Val, int(limit)-1, "content plus its one field frame is inclusive")

		// Content alone exceeds the bound: the fold-in check rejects the record.
		assertLimitError(t, maxRecordTestCfg(limit), strings.Repeat("x", int(limit)+1)+"\n")
		// Over by many blocks: readUntil's growth check fails fast mid-record.
		assertLimitError(t, maxRecordTestCfg(limit), strings.Repeat("x", int(5*limit))+"\n")
	})

	// A quoted field made only of escape pairs never gives readUntil a large
	// fragment; the bound must still apply through the per-token path.
	t.Run("quoted field of escape tokens", func(t *testing.T) {
		below := `"` + strings.Repeat(`\"`, 40) + `"` + "\n" // 80 raw bytes
		row := readRow(t, maxRecordTestCfg(limit), below)
		require.Len(t, row, 1)
		require.Equal(t, strings.Repeat(`"`, 40), row[0].Val)
		require.True(t, row[0].HasStringQuote)

		assertLimitError(t, maxRecordTestCfg(limit), `"`+strings.Repeat(`\"`, 60)+`"`+"\n")
	})

	// Quoted content spanning read blocks is bounded like unquoted content.
	t.Run("quoted field across read blocks", func(t *testing.T) {
		row := readRow(t, maxRecordTestCfg(limit), `"`+strings.Repeat("x", 90)+`"`+"\n")
		require.Len(t, row[0].Val, 90)

		// the fragment completes past the bound: fold-in check
		assertLimitError(t, maxRecordTestCfg(limit), `"`+strings.Repeat("x", int(limit)+24)+`"`+"\n")
		// the fragment itself outgrows the bound: readUntil fails fast
		assertLimitError(t, maxRecordTestCfg(limit), `"`+strings.Repeat("x", int(5*limit))+`"`+"\n")
	})

	// The bound is cumulative over the record: three mid-size fields exceed it
	// together even though each fits on its own.
	t.Run("composite multi-field row", func(t *testing.T) {
		row := readRow(t, maxRecordTestCfg(limit),
			strings.Repeat("x", 30)+","+strings.Repeat("y", 30)+","+strings.Repeat("z", 30)+"\n")
		require.Len(t, row, 3)

		assertLimitError(t, maxRecordTestCfg(limit),
			strings.Repeat("x", 40)+","+strings.Repeat("y", 40)+","+strings.Repeat("z", 40)+"\n")
	})

	// Framing counts toward the record bound too: a row of only separators
	// must not grow fieldIndexes/fieldIsQuoted (and downstream the decoded
	// row slice) without limit while recordBuffer stays empty. Every block
	// size must reach the same accept/reject decision, otherwise the bound
	// would depend on read segmentation.
	t.Run("framing counts toward the record bound", func(t *testing.T) {
		const frameLimit int64 = 8
		cases := []struct {
			name       string
			input      string
			wantFields int // > 0: row parses with this many fields; 0: rejected
			wantQuoted bool
			wantEmpty  bool
		}{
			{"commas at bound", ",,,,,,,\n", 8, false, true},
			{"commas over bound", ",,,,,,,,\n", 0, false, false},
			{"empty quoted at bound", `""` + strings.Repeat(`,""`, 7) + "\n", 8, true, true},
			{"empty quoted over bound", `""` + strings.Repeat(`,""`, 8) + "\n", 0, true, false},
			{"mixed content and framing at bound", "abc,def\n", 2, false, false},
			{"mixed content over bound", "abcd,defg\n", 0, false, false},
		}
		for _, tc := range cases {
			for _, bs := range []int64{1, 4, blockSize, ReadBlockSize} {
				t.Run(fmt.Sprintf("%s/block=%d", tc.name, bs), func(t *testing.T) {
					parser, err := NewCSVParser(maxRecordTestCfg(frameLimit), NewStringReader(tc.input), bs, false)
					require.NoError(t, err)
					row, err := parser.Read(nil)
					if tc.wantFields == 0 {
						require.Error(t, err)
						require.ErrorContains(t, err, "largest-entry-limit")
						require.False(t, IsSyntaxError(err))
						return
					}
					require.NoError(t, err)
					require.Len(t, row, tc.wantFields)
					if tc.wantEmpty {
						want := newStringField("", false, tc.wantQuoted)
						for i, f := range row {
							require.Equal(t, want, f, "field %d decoded from empty", i)
						}
					}
				})
			}
		}
	})

	// The bound resets for every record: a file of many individually-small
	// records parses regardless of total file size.
	t.Run("bound is per record not per file", func(t *testing.T) {
		parser, err := NewCSVParser(maxRecordTestCfg(8),
			NewStringReader(strings.Repeat("abc,de\n", 100)), 4, false)
		require.NoError(t, err)
		for i := 0; i < 100; i++ {
			row, err := parser.Read(nil)
			require.NoError(t, err)
			require.Len(t, row, 2, "record %d", i)
		}
		_, err = parser.Read(nil)
		require.ErrorIs(t, err, io.EOF)
	})

	// Lines the parser skips outright (comments) share the same bound, both
	// when one fragment outgrows it and when a multi-byte terminator keeps
	// every fragment tiny so only the accumulation check can fire.
	t.Run("skipped lines share the bound", func(t *testing.T) {
		cfg := maxRecordTestCfg(limit)
		cfg.Comment = "#"
		row := readRow(t, cfg, "#"+strings.Repeat("c", 80)+"\na,b\n")
		require.Len(t, row, 2)

		assertLimitError(t, cfg, "#"+strings.Repeat("c", 200)+"\na,b\n")

		cfg = maxRecordTestCfg(limit)
		cfg.Comment = "#"
		cfg.LinesTerminatedBy = "::"
		assertLimitError(t, cfg, "#"+strings.Repeat("c:", 60)+"::a,b::")
	})

	// Reset keeps the parser (and its bound) reusable: a clean row parses and
	// the next oversized row is rejected again, with no cross-input bleed.
	t.Run("reset reuses the same bound", func(t *testing.T) {
		parser, err := NewCSVParser(maxRecordTestCfg(16), NewStringReader(strings.Repeat("x", 20)+"\n"), 4, false)
		require.NoError(t, err)
		_, err = parser.Read(nil)
		require.Error(t, err)

		parser.Reset(NewStringReader("a,b\n" + strings.Repeat("y", 20) + "\n"))
		row, err := parser.Read(nil)
		require.NoError(t, err)
		require.Len(t, row, 2)
		_, err = parser.Read(nil)
		require.ErrorContains(t, err, "largest-entry-limit")
	})

	// An explicit MaxRecordSize always wins over the server default.
	t.Run("explicit bound overrides default", func(t *testing.T) {
		assertLimitError(t, maxRecordTestCfg(16), strings.Repeat("x", 20)+"\n")
		require.Equal(t, int64(config.LargestEntryLimit), (&CSVParser{cfg: &CSVConfig{}}).maxRecordSize())
	})
}

// TestDoubledQuoteChargesRawBytes pins the pre-unescaping whole-record weight:
// a doubled quote "" is two consumed input bytes folding into one decoded
// quote, and both consumed bytes are charged — identically at every read-block
// size. Four pairs therefore weigh 8 content bytes plus 1 field frame, so
// MaxRecordSize=5 must reject the record the fold-then-charge accounting
// previously accepted. The escaped equivalent \" also prices two consumed
// bytes per decoded quote and yields the same decoded value under the same
// weight, so neither encoding can bypass the bound; a within-budget control at
// the exact threshold must still parse with its decoded value unchanged.
func TestDoubledQuoteChargesRawBytes(t *testing.T) {
	readAt := func(t *testing.T, input string, limit, blockSize int64) ([]Field, error) {
		t.Helper()
		parser, err := NewCSVParser(maxRecordTestCfg(limit), NewStringReader(input), blockSize, false)
		require.NoError(t, err)
		return parser.Read(nil)
	}
	requireLimitError := func(t *testing.T, input string, limit, blockSize int64) {
		t.Helper()
		_, err := readAt(t, input, limit, blockSize)
		require.Error(t, err)
		require.ErrorContains(t, err, "largest-entry-limit")
	}

	const wantVal = `""""`
	doubled := `"` + strings.Repeat(`""`, 4) + `"` + "\n"
	escaped := `"` + strings.Repeat(`\"`, 4) + `"` + "\n"

	// Exact threshold with one pair: weight = 2 content + 1 field == 3.
	t.Run("exact threshold with one pair", func(t *testing.T) {
		pair := `""""` + "\n"
		row, err := readAt(t, pair, 3, 4)
		require.NoError(t, err)
		require.Len(t, row, 1)
		require.Equal(t, `"`, row[0].Val)
		requireLimitError(t, pair, 2, 4)
	})

	for _, blockSize := range []int64{1, 4, 65536} {
		t.Run(fmt.Sprintf("block=%d", blockSize), func(t *testing.T) {
			// 4 pairs cost 8 raw content bytes; with the field frame the
			// inclusive weight is 9.
			requireLimitError(t, doubled, 5, blockSize)
			requireLimitError(t, doubled, 8, blockSize)

			row, err := readAt(t, doubled, 9, blockSize)
			require.NoError(t, err)
			require.Len(t, row, 1)
			require.Equal(t, wantVal, row[0].Val,
				"decoded value must not change with raw-byte accounting")

			// Identical decoded bytes must price identically across encodings.
			requireLimitError(t, escaped, 5, blockSize)
			erow, err := readAt(t, escaped, 9, blockSize)
			require.NoError(t, err)
			require.Len(t, erow, 1)
			require.Equal(t, wantVal, erow[0].Val)
		})
	}

	// The folded-quote charge is per-record state: back-to-back records, each
	// exactly in budget (weight 2 content + 1 field == 3), must all parse on
	// one parser — the bound applies per record, never to the whole file.
	t.Run("charge resets per record", func(t *testing.T) {
		parser, err := NewCSVParser(maxRecordTestCfg(3), NewStringReader(`""""`+"\n"+`""""`+"\n"), 4, false)
		require.NoError(t, err)
		for i := 0; i < 2; i++ {
			row, err := parser.Read(nil)
			require.NoError(t, err, "record %d succeeds on its own exact budget", i+1)
			require.Len(t, row, 1)
			require.Equal(t, `"`, row[0].Val, "record %d decoded value", i+1)
		}
		_, err = parser.Read(nil)
		require.ErrorIs(t, err, io.EOF)
	})

	// Several doubled-quote fields inside ONE record share the whole-record
	// budget: `a""b` consumes 4 raw bytes per field (decoded buffer 3+3,
	// folded delta 1+1) and the two field frames add 2 — weight is 10 exactly.
	// At the exact limit it passes; one less must reject.
	t.Run("multiple doubled-quote fields share the record budget", func(t *testing.T) {
		multi := `"a""b","c""d"` + "\n"
		parser, err := NewCSVParser(maxRecordTestCfg(10), NewStringReader(multi), 4, false)
		require.NoError(t, err)
		row, err := parser.Read(nil)
		require.NoError(t, err)
		require.Len(t, row, 2)
		require.Equal(t, `a"b`, row[0].Val)
		require.Equal(t, `c"d`, row[1].Val)

		requireLimitError(t, multi, 9, 4)
	})

	// Reset reuses the same parser (and bound) after folded rows: the next
	// input starts from a clean record with no cross-input bleed, and its
	// decoded value is unchanged.
	t.Run("reset reuses the parser after folded rows", func(t *testing.T) {
		parser, err := NewCSVParser(maxRecordTestCfg(3), NewStringReader(`""""`+"\n"), 4, false)
		require.NoError(t, err)
		row, err := parser.Read(nil)
		require.NoError(t, err)
		require.Equal(t, `"`, row[0].Val)

		parser.Reset(NewStringReader(`""""` + "\n"))
		row, err = parser.Read(nil)
		require.NoError(t, err)
		require.Equal(t, `"`, row[0].Val)
		_, err = parser.Read(nil)
		require.ErrorIs(t, err, io.EOF)
	})
}

// TestLargeRowsByDefault covers issue #30640 at its reproducing scale with
// the default bound: rows that are legal to INSERT must also parse when
// LOADed from a CSV dump. Inputs are generated, no fixture files.
func TestLargeRowsByDefault(t *testing.T) {
	const MiB = 1024 * 1024
	require.GreaterOrEqual(t, config.LargestEntryLimit, 40*MiB,
		"the default bound must cover the 32MiB composite issue row with headroom")

	cfg := maxRecordTestCfg(0) // 0 => default bound
	readOne := func(t *testing.T, input string) []Field {
		t.Helper()
		parser, err := NewCSVParser(cfg, NewStringReader(input), ReadBlockSize, false)
		require.NoError(t, err)
		row, err := parser.Read(nil)
		require.NoError(t, err)
		return row
	}

	// The former hidden 10MiB fragment cap is gone: just under, equal, just
	// above it and the issue's 12MiB LONGTEXT all parse identically.
	for _, size := range []int{10*MiB - 1, 10 * MiB, 10*MiB + 1, 12 * MiB} {
		t.Run(fmt.Sprintf("plain field of %d bytes", size), func(t *testing.T) {
			row := readOne(t, strings.Repeat("x", size)+"\n")
			require.Len(t, row, 1)
			require.Len(t, row[0].Val, size, "REPEAT('x',%d) must round trip through CSV", size)
		})
	}

	// The 32MiB composite MEDIUMTEXT/LONGTEXT row from the issue, including a
	// quoted field holding escaped quotes.
	t.Run("32MiB composite row", func(t *testing.T) {
		var quoted strings.Builder
		for i := 0; i < 8; i++ {
			quoted.WriteString(strings.Repeat("x", MiB-2))
			quoted.WriteString(`\"`) // escaped quotes keep this field on the token path too
		}
		require.Equal(t, 8*MiB, quoted.Len())
		input := `"` + quoted.String() + `",` +
			`"` + strings.Repeat("y", 8*MiB) + `",` +
			strings.Repeat("z", 8*MiB) + "," +
			strings.Repeat("w", 8*MiB) + "\n"
		row := readOne(t, input)
		require.Len(t, row, 4)
		require.Len(t, row[0].Val, 8*MiB-8, "each \" pair decodes to one quote")
		require.True(t, row[0].HasStringQuote)
		require.Len(t, row[1].Val, 8*MiB)
		require.True(t, row[1].HasStringQuote)
		require.Len(t, row[2].Val, 8*MiB)
		require.False(t, row[2].HasStringQuote)
		require.Len(t, row[3].Val, 8*MiB)
	})

	// Past the default bound the row is still rejected, with an actionable
	// error that points at the real knob.
	t.Run("over default bound is rejected", func(t *testing.T) {
		input := strings.Repeat("x", config.LargestEntryLimit+1) + "\n"
		parser, err := NewCSVParser(cfg, NewStringReader(input), ReadBlockSize, false)
		require.NoError(t, err)
		_, err = parser.Read(nil)
		require.Error(t, err)
		require.ErrorContains(t, err, "largest-entry-limit")
		require.False(t, IsSyntaxError(err))
	})
}
