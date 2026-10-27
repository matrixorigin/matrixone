// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/geo"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
)

// selectIntoOutfileConfig builds an ExportConfig the way the INTO OUTFILE
// grammar now populates it, with explicit FIELDS/LINES clauses.
func selectIntoOutfileConfig(terminated string, enclosed, escaped byte, startingBy, lineTerm string) *ExportConfig {
	fields := tree.NewFields(terminated, false, enclosed, escaped)
	lines := tree.NewLines(startingBy, lineTerm)
	ep := &ExportConfig{
		userConfig: &tree.ExportParam{Fields: fields, Lines: lines},
		Symbol:     [][]byte{[]byte(terminated), []byte(lineTerm)},
		ColumnFlag: []bool{false, false},
		mrs:        &MysqlResultSet{},
	}
	id, name := &MysqlColumn{}, &MysqlColumn{}
	id.SetName("id")
	id.SetColumnType(defines.MYSQL_TYPE_LONG)
	name.SetName("s")
	name.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	ep.mrs.AddColumn(id)
	ep.mrs.AddColumn(name)
	return ep
}

func newIdNameBatch(t *testing.T, mp *mpool.MPool, rows ...any) *batch.Batch {
	t.Helper()
	bat := batch.NewOffHeapWithSize(2)
	bat.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int32.ToType())
	bat.Vecs[1] = vector.NewOffHeapVecWithType(types.New(types.T_varchar, 65535, 0))
	for i, row := range rows {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(i+1), false, mp))
		if row == nil {
			require.NoError(t, vector.AppendBytes(bat.Vecs[1], nil, true, mp))
			continue
		}
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte(row.(string)), false, mp))
	}
	bat.SetRowCount(len(rows))
	return bat
}

// readPrefixedCSV parses writer output with the LOAD parser configured with
// the matching clause set, returning the decoded rows.
func readPrefixedCSV(t *testing.T, data []byte, startingBy, lineTerm string) [][]string {
	t.Helper()
	parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
		FieldsTerminatedBy: ",", FieldsEnclosedBy: `"`,
		FieldsEscapedBy: "\\", LinesTerminatedBy: lineTerm, LinesStartingBy: startingBy,
		Null: []string{`\N`}, UnescapedQuote: true,
	}, bytes.NewReader(data), csvparser.ReadBlockSize, false)
	require.NoError(t, err)
	var rows [][]string
	for {
		row, err := parser.Read(nil)
		if err == io.EOF {
			return rows
		}
		require.NoError(t, err)
		vals := make([]string, len(row))
		for i, f := range row {
			// Field values alias the parser's record buffer and are
			// overwritten by the next Read; clone for cross-row assertions.
			vals[i] = strings.Clone(f.Val)
		}
		rows = append(rows, vals)
	}
}

// TestOutfileLinesStartingByPrefixedOnDataRecords verifies both CSV row
// writers (batch-conversion constructByte and the result-set writer used by
// MODUMP) emit LINES STARTING BY before every data record, and that the
// output read back through LOAD's parser (LinesStartingBy) yields the
// original values.
func TestOutfileLinesStartingByPrefixedOnDataRecords(t *testing.T) {
	rows := []string{"alpha", "be>ta", ">>nested"}

	t.Run("batch writer constructByte", func(t *testing.T) {
		ep := selectIntoOutfileConfig(",", '"', '\\', ">>", "\n")
		mp := mpool.MustNewZero()
		defer func() { require.Zero(t, mp.CurrNB()) }()
		anyRows := make([]any, len(rows))
		for i, r := range rows {
			anyRows[i] = r
		}
		bat := newIdNameBatch(t, mp, anyRows...)
		defer bat.Clean(mp)
		ch := make(chan *BatchByte, 1)
		constructByte(context.Background(), &backSession{feSessionImpl: feSessionImpl{pool: mp}}, bat, 0, ch, ep)
		result := <-ch
		require.NoError(t, result.err)

		// Every record starts with the prefix.
		for _, line := range bytes.Split(bytes.TrimSuffix(result.writeByte, []byte("\n")), []byte("\n")) {
			require.True(t, bytes.HasPrefix(line, []byte(">>")), "record %q must carry the prefix", line)
		}

		got := readPrefixedCSV(t, result.writeByte, ">>", "\n")
		require.Len(t, got, len(rows))
		for i, r := range rows {
			require.Equal(t, strconv.Itoa(i+1), got[i][0])
			require.Equal(t, r, got[i][1], "row %d", i)
		}
	})

	t.Run("result-set row writer", func(t *testing.T) {
		ep := selectIntoOutfileConfig(",", '"', '\\', ">>", "\n")
		var out bytes.Buffer
		old := Write
		Write = func(_ *ExportConfig, b []byte) (int, error) { return out.Write(b) }
		defer func() { Write = old }()

		for i, row := range rows {
			ep.mrs.Data = [][]any{{int32(i + 1), row}}
			require.NoError(t, exportDataFromResultSetToCSVFile(ep))
		}

		got := readPrefixedCSV(t, out.Bytes(), ">>", "\n")
		require.Len(t, got, len(rows))
		for i, r := range rows {
			require.Equal(t, strconv.Itoa(i+1), got[i][0])
			require.Equal(t, r, got[i][1], "row %d", i)
		}
	})
}

// TestOutfileLinesStartingByHeaderUnprefixed verifies the CSV header line is
// NOT prefixed with LINES STARTING BY: LOAD's readRecord skips whole lines
// without the prefix, so the header disappears on reload instead of becoming
// a data row. Data records still carry the prefix. Full round-trip: reload
// with the matching clause set yields exactly the source rows.
func TestOutfileLinesStartingByHeaderUnprefixed(t *testing.T) {
	dir := t.TempDir()
	ep := selectIntoOutfileConfig(",", '"', '\\', ">>", "\n")
	ep.userConfig.Outfile = true
	ep.userConfig.Header = true
	ep.userConfig.FilePath = filepath.Join(dir, "out.csv")
	ep.proc = sysFileAccessTestProcess()
	ep.ctx = context.Background()
	ep.init()
	defer ep.Close()

	require.NoError(t, openNewFile(ep.ctx, ep, ep.mrs))

	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	bat := newIdNameBatch(t, mp, "alpha", "beta")
	defer bat.Clean(mp)
	ch := make(chan *BatchByte, 1)
	constructByte(ep.ctx, &backSession{feSessionImpl: feSessionImpl{pool: mp}}, bat, 0, ch, ep)
	result := <-ch
	require.NoError(t, result.err)
	require.NoError(t, writeToCSVFile(ep, result.writeByte))
	require.NoError(t, Close(ep))

	content, err := os.ReadFile(filepath.Join(dir, "out.csv"))
	require.NoError(t, err)
	lines := bytes.Split(bytes.TrimSuffix(content, []byte("\n")), []byte("\n"))
	require.Len(t, lines, 3)
	require.Equal(t, "id,s", string(lines[0]), "header must not carry the prefix")
	require.True(t, bytes.HasPrefix(lines[1], []byte(">>")), "data records must carry the prefix")
	require.True(t, bytes.HasPrefix(lines[2], []byte(">>")), "data records must carry the prefix")

	// The header line has no prefix, so LOAD skips it: only data rows return.
	got := readPrefixedCSV(t, content, ">>", "\n")
	require.Equal(t, [][]string{{"1", "alpha"}, {"2", "beta"}}, got)
}

// TestOutfileEscapedByHonored verifies FIELDS ESCAPED BY selection reaches
// the writer: a custom escape byte prefixes itself, and ESCAPED BY ”
// disables prefix escaping entirely (values written verbatim).
func TestOutfileEscapedByHonored(t *testing.T) {
	t.Run("custom escape byte used for bare values", func(t *testing.T) {
		ep := newOutfileExportConfig(tree.NewFields(",", false, 0, '#'), nil, "\n")
		require.Equal(t, []byte("a##b"), addEscapeToString(ep, []byte("a#b"), 0))
		require.Equal(t, []byte("a#,b"), addEscapeToString(ep, []byte("a,b"), 0))
		require.Equal(t, []byte("plain"), addEscapeToString(ep, []byte("plain"), 0))
	})
	t.Run("escaped-by-empty writes values verbatim", func(t *testing.T) {
		ep := newOutfileExportConfig(tree.NewFields(",", false, 0, 0), nil, "\n")
		require.Equal(t, []byte("a,b"), addEscapeToString(ep, []byte("a,b"), 0))
		require.Equal(t, []byte(`a\b`), addEscapeToString(ep, []byte(`a\b`), 0))
		require.Zero(t, ep.escapeChar())
	})
	t.Run("custom escape in enclosed mode is prefixed", func(t *testing.T) {
		ep := newOutfileExportConfig(tree.NewFields(",", false, '"', '!'), nil, "\n")
		require.Equal(t, []byte("a!!b"), addEscapeToString(ep, []byte("a!b"), '"'))
		require.Equal(t, []byte(`a""b`), addEscapeToString(ep, []byte(`a"b`), '"'))
	})
	t.Run("batch writer escapes bare text with custom byte", func(t *testing.T) {
		ep := selectIntoOutfileConfig(",", 0, '#', "", "\n")
		mp := mpool.MustNewZero()
		defer func() { require.Zero(t, mp.CurrNB()) }()
		bat := newIdNameBatch(t, mp, "a#b", "x,y")
		defer bat.Clean(mp)
		ch := make(chan *BatchByte, 1)
		constructByte(context.Background(), &backSession{feSessionImpl: feSessionImpl{pool: mp}}, bat, 0, ch, ep)
		result := <-ch
		require.NoError(t, result.err)
		require.Equal(t, "1,a##b\n2,x#,y\n", string(result.writeByte))
	})
}

// TestOutfileStartingByContainingTerminatorRejected mirrors the LOAD parser's
// config validation: a record prefix containing the record terminator makes
// boundaries unreadable, so the export is rejected before a file is opened.
func TestOutfileStartingByContainingTerminatorRejected(t *testing.T) {
	ep := newOutfileExportConfig(tree.NewFields(tree.DefaultFieldsTerminated, false, tree.DefaultFieldsEnclosedBy[0], '\\'), nil, "\n")
	ep.userConfig.Lines.StartingBy = ">>\n"
	err := ep.validateCSVFraming(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "STARTING BY '>>\n' cannot contain LINES TERMINATED BY '\n'")

	// CRLF is normalized the same way the LOAD parser does it.
	ep2 := newOutfileExportConfig(tree.NewFields(tree.DefaultFieldsTerminated, false, tree.DefaultFieldsEnclosedBy[0], '\\'), nil, "\r\n")
	ep2.userConfig.Lines.StartingBy = "x\ny"
	err = ep2.validateCSVFraming(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "STARTING BY 'x\ny' cannot contain LINES TERMINATED BY '\n'")

	// A prefix without the terminator is fine, including a bare CR under
	// CRLF termination (matches reader normalization).
	ep3 := newOutfileExportConfig(tree.NewFields(tree.DefaultFieldsTerminated, false, tree.DefaultFieldsEnclosedBy[0], '\\'), nil, "\r\n")
	ep3.userConfig.Lines.StartingBy = ">>\r"
	require.NoError(t, ep3.validateCSVFraming(context.Background()))

	// Non-CSV formats do not apply CSV framing validation.
	ep4 := newOutfileExportConfig(tree.NewFields(tree.DefaultFieldsTerminated, false, tree.DefaultFieldsEnclosedBy[0], '\\'), nil, "\n")
	ep4.userConfig.Lines.StartingBy = ">>\n"
	ep4.userConfig.ExportFormat = "parquet"
	require.NoError(t, ep4.validateCSVFraming(context.Background()))
}

// TestOutfileHeaderPrefixCollisionRejected: LOAD's startingBy filter skips a
// line only when the prefix is absent from it; an unprefixed header whose raw
// text contains the prefix would come back as a mangled data row (column
// `name` with STARTING BY 'n' loads an extra "ame" row). The export must
// reject the configuration before a byte reaches the file.
func TestOutfileHeaderPrefixCollisionRejected(t *testing.T) {
	newCfg := func(startingBy string, header bool) *ExportConfig {
		ep := selectIntoOutfileConfig(",", '"', '\\', startingBy, "\n")
		ep.userConfig.Outfile = true
		ep.userConfig.Header = header
		ep.proc = sysFileAccessTestProcess()
		ep.ctx = context.Background()
		return ep
	}
	const wantErr = "LINES STARTING BY 'n' occurs in the CSV header"

	t.Run("single column name containing the prefix", func(t *testing.T) {
		ep := newCfg("n", true)
		mrs := &MysqlResultSet{}
		col := &MysqlColumn{}
		col.SetName("name")
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		mrs.AddColumn(col)
		ep.userConfig.FilePath = filepath.Join(t.TempDir(), "collide.csv")
		ep.init()
		defer ep.Close()
		err := openNewFile(ep.ctx, ep, mrs)
		require.Error(t, err)
		require.Contains(t, err.Error(), wantErr)
		require.NoFileExists(t, ep.userConfig.FilePath, "no file may be opened on framing rejection")
	})

	t.Run("prefix spanning the field terminator", func(t *testing.T) {
		// The reader scans the joined line bytes ("idn't,age" contains "n't,");
		// only checking column names individually would miss this.
		ep := newCfg("d,s", true)
		mrs := &MysqlResultSet{}
		for _, name := range []string{"id", "s"} {
			col := &MysqlColumn{}
			col.SetName(name)
			col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
			mrs.AddColumn(col)
		}
		ep.userConfig.FilePath = filepath.Join(t.TempDir(), "collide.csv")
		ep.init()
		defer ep.Close()
		err := openNewFile(ep.ctx, ep, mrs)
		require.Error(t, err)
		require.Contains(t, err.Error(), "LINES STARTING BY 'd,s' occurs in the CSV header")
	})

	t.Run("header false is the documented escape hatch", func(t *testing.T) {
		ep := newCfg("n", false)
		mrs := &MysqlResultSet{}
		col := &MysqlColumn{}
		col.SetName("name")
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		mrs.AddColumn(col)
		ep.userConfig.FilePath = filepath.Join(t.TempDir(), "ok.csv")
		ep.init()
		defer ep.Close()
		require.NoError(t, openNewFile(ep.ctx, ep, mrs))
		require.NoError(t, Close(ep))
		content, err := os.ReadFile(ep.userConfig.FilePath)
		require.NoError(t, err)
		require.Empty(t, content, "header disabled: only data records will follow")
	})

	t.Run("prefix absent from the header still exports", func(t *testing.T) {
		ep := newCfg(">>", true)
		mrs := &MysqlResultSet{}
		col := &MysqlColumn{}
		col.SetName("name")
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		mrs.AddColumn(col)
		ep.userConfig.FilePath = filepath.Join(t.TempDir(), "ok.csv")
		ep.init()
		defer ep.Close()
		require.NoError(t, openNewFile(ep.ctx, ep, mrs))
		require.NoError(t, Close(ep))
		content, err := os.ReadFile(ep.userConfig.FilePath)
		require.NoError(t, err)
		require.Equal(t, "name\n", string(content), "unprefixed header keeps the reader-side skip contract")
	})

	t.Run("non-csv formats ignore the csv header rule", func(t *testing.T) {
		ep := newCfg("n", true)
		ep.userConfig.ExportFormat = "parquet"
		require.NoError(t, validateOutfileHeaderPrefix(ep.ctx, ep, &MysqlResultSet{}))
	})

	t.Run("empty column list is a no-op", func(t *testing.T) {
		ep := newCfg("n", true)
		require.NoError(t, validateOutfileHeaderPrefix(ep.ctx, ep, &MysqlResultSet{}))
		require.NoError(t, validateOutfileHeaderPrefix(ep.ctx, ep, nil))
	})

	t.Run("prefix occurring only in the header's terminator", func(t *testing.T) {
		// LOAD scans each raw record including the terminator, so a column
		// 'X' under TERMINATED BY '||' emits header 'X||', which prefix 'X|'
		// matches even though no column name contains it.
		ep := selectIntoOutfileConfig(",", '"', '\\', "X|", "||")
		ep.userConfig.Header = true
		ep.ctx = context.Background()
		mrs := &MysqlResultSet{}
		col := &MysqlColumn{}
		col.SetName("X")
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		mrs.AddColumn(col)
		err := validateOutfileHeaderPrefix(ep.ctx, ep, mrs)
		require.ErrorContains(t, err, "LINES STARTING BY 'X|' occurs in the CSV header")
	})

	t.Run("prefix absent from the complete header passes", func(t *testing.T) {
		ep := selectIntoOutfileConfig(",", '"', '\\', "Y|", "||")
		ep.userConfig.Header = true
		ep.ctx = context.Background()
		mrs := &MysqlResultSet{}
		col := &MysqlColumn{}
		col.SetName("X")
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		mrs.AddColumn(col)
		require.NoError(t, validateOutfileHeaderPrefix(ep.ctx, ep, mrs))
	})

	t.Run("document the reader corruption behind the check", func(t *testing.T) {
		// The reviewer's probe: file "X||X|\"one\"||X|\"two\"||" with prefix
		// 'X|' and terminator '||' must not parse as the two clean values;
		// the header record collides and the first value mangles.
		parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
			FieldsTerminatedBy: ",", FieldsEnclosedBy: `"`, FieldsEscapedBy: "\\",
			LinesTerminatedBy: "||", LinesStartingBy: "X|", Null: []string{`\N`},
		}, bytes.NewReader([]byte("X||X|\"one\"||X|\"two\"||")), csvparser.ReadBlockSize, false)
		require.NoError(t, err)
		var vals []string
		for len(vals) < 2 {
			row, err := parser.Read(nil)
			if err != nil {
				// The mangled record may surface as a parse error instead of
				// wrong values; either way the clean pair is gone.
				break
			}
			vals = append(vals, row[0].Val)
		}
		require.NotEqual(t, []string{"one", "two"}, vals)
	})
}

// TestOutfileExactBackslashNRejectedWithCustomEscape guards the LOAD-side
// NULL-token trap: LOAD always matches `\N` against its Null token list
// (even inside quotes, quotedNullIsText is false), so a literal `\N` string
// only round-trips when the default backslash escape rewrites it to `\\N`.
// Any other FIELDS ESCAPED BY setting must reject the value up front instead
// of silently importing it as SQL NULL. Genuine NULLs are unaffected: they
// are written by formatOutputNull and are meant to reload as NULL.
func TestOutfileExactBackslashNRejectedWithCustomEscape(t *testing.T) {
	const wantErr = "cannot round-trip unless FIELDS ESCAPED BY is the default"
	newGuardCfg := func(escaped, enclosed byte) *ExportConfig {
		ep := newOutfileExportConfig(tree.NewFields(",", false, enclosed, escaped), nil, "\n")
		ep.ctx = context.Background()
		return ep
	}

	t.Run("custom escape unenclosed", func(t *testing.T) {
		var buf bytes.Buffer
		err := formatOutputString(newGuardCfg('#', 0), []byte(`\N`), []byte("\n"), 0, false, true, &buf)
		require.ErrorContains(t, err, wantErr)
	})
	t.Run("custom escape enclosed still rejected", func(t *testing.T) {
		// LOAD matches the NULL token inside quotes too.
		var buf bytes.Buffer
		err := formatOutputString(newGuardCfg('#', '"'), []byte(`\N`), []byte("\n"), '"', true, true, &buf)
		require.ErrorContains(t, err, wantErr)
	})
	t.Run("escaping disabled rejected", func(t *testing.T) {
		var buf bytes.Buffer
		err := formatOutputString(newGuardCfg(0, 0), []byte(`\N`), []byte("\n"), 0, false, true, &buf)
		require.ErrorContains(t, err, wantErr)
	})
	t.Run("only the exact token is rejected", func(t *testing.T) {
		var buf bytes.Buffer
		require.NoError(t, formatOutputString(newGuardCfg('#', 0), []byte(`\Nx`), []byte("\n"), 0, false, true, &buf))
		require.NoError(t, formatOutputString(newGuardCfg('#', 0), []byte(`x\N`), []byte("\n"), 0, false, true, &buf))
		require.NoError(t, formatOutputString(newGuardCfg('#', 0), []byte(`\n`), []byte("\n"), 0, false, true, &buf))
	})

	t.Run("default escape rewrites the literal and reloads as text", func(t *testing.T) {
		ep := newGuardCfg('\\', 0)
		var buf bytes.Buffer
		require.NoError(t, formatOutputString(ep, []byte(`\N`), []byte("\n"), 0, false, true, &buf))
		require.Equal(t, `\\N`+"\n", buf.String())
		parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
			FieldsTerminatedBy: ",", FieldsEscapedBy: "\\", LinesTerminatedBy: "\n",
			Null: []string{`\N`},
		}, bytes.NewReader(buf.Bytes()), csvparser.ReadBlockSize, false)
		require.NoError(t, err)
		row, err := parser.Read(nil)
		require.NoError(t, err)
		require.Len(t, row, 1)
		require.False(t, row[0].IsNull, "escaped literal must not collapse into NULL")
		require.Equal(t, `\N`, row[0].Val)
	})

	t.Run("genuine NULLs export under a custom escape", func(t *testing.T) {
		// The NULL marker itself is never a data value, so the guard must not
		// block it; LOAD reads it back as SQL NULL as intended.
		ep := newGuardCfg('#', 0)
		var buf bytes.Buffer
		require.NoError(t, formatOutputNull(ep, []byte("\n"), 0, true, &buf))
		require.Equal(t, "\\N\n", buf.String())
		parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
			FieldsTerminatedBy: ",", FieldsEscapedBy: "#", LinesTerminatedBy: "\n",
			Null: []string{`\N`},
		}, bytes.NewReader(buf.Bytes()), csvparser.ReadBlockSize, false)
		require.NoError(t, err)
		row, err := parser.Read(nil)
		require.NoError(t, err)
		require.Len(t, row, 1)
		require.True(t, row[0].IsNull)
	})

	t.Run("batch writer rejects literal under custom escape", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer func() { require.Zero(t, mp.CurrNB()) }()
		bat := newIdNameBatch(t, mp, `\N`)
		defer bat.Clean(mp)
		ep := selectIntoOutfileConfig(",", 0, '#', "", "\n")
		ep.ctx = context.Background()
		ch := make(chan *BatchByte, 1)
		constructByte(ep.ctx, &backSession{feSessionImpl: feSessionImpl{pool: mp}}, bat, 0, ch, ep)
		result := <-ch
		require.ErrorContains(t, result.err, wantErr)
	})

	t.Run("batch writer default escape round-trips literal and NULL", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer func() { require.Zero(t, mp.CurrNB()) }()
		bat := newIdNameBatch(t, mp, `\N`, nil)
		defer bat.Clean(mp)
		ep := selectIntoOutfileConfig(",", 0, '\\', "", "\n")
		ep.ctx = context.Background()
		ch := make(chan *BatchByte, 1)
		constructByte(ep.ctx, &backSession{feSessionImpl: feSessionImpl{pool: mp}}, bat, 0, ch, ep)
		result := <-ch
		require.NoError(t, result.err)
		require.Equal(t, "1,\\\\N\n2,\\N\n", string(result.writeByte))
	})
}

// TestOutfileTrailingCRLastColumnRejectedWithoutEscape guards the reader-side
// CR strip: LOAD's parser removes a trailing CR from the last column's record
// under '\n' termination, and it does so on the record buffer AFTER quotes
// are removed — so with escaping disabled an enclosure does NOT protect a
// CR-final value either. The writer must reject it (the externalwrite writer
// has the same rule); any escape byte protects the value.
func TestOutfileTrailingCRLastColumnRejectedWithoutEscape(t *testing.T) {
	const wantErr = "a value ending in CR in the last column cannot round-trip"
	newGuardCfg := func(escaped, enclosed byte) *ExportConfig {
		ep := newOutfileExportConfig(tree.NewFields(",", false, enclosed, escaped), nil, "\n")
		ep.ctx = context.Background()
		return ep
	}

	t.Run("last column unenclosed without escape", func(t *testing.T) {
		var buf bytes.Buffer
		err := formatOutputString(newGuardCfg(0, 0), []byte("a\r"), []byte("\n"), 0, false, true, &buf)
		require.ErrorContains(t, err, wantErr)
	})
	t.Run("enclosure does not protect the CR", func(t *testing.T) {
		// The reader's strip runs after unquoting: even a quoted `"a\r"`
		// record loses its CR, so the writer must reject both spellings.
		var buf bytes.Buffer
		require.ErrorContains(t, formatOutputString(newGuardCfg(0, '"'), []byte("a\r"), []byte("\n"), '"', true, true, &buf), wantErr)
		require.ErrorContains(t, formatOutputString(newGuardCfg(0, '"'), []byte("a\r"), []byte("\n"), '"', false, true, &buf), wantErr)

		// Document the reader behavior that forces the writer's hand.
		parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
			FieldsTerminatedBy: ",", FieldsEnclosedBy: `"`, FieldsEscapedBy: "",
			LinesTerminatedBy: "\n", Null: []string{`\N`},
		}, bytes.NewReader([]byte("\"a\r\"\n")), csvparser.ReadBlockSize, false)
		require.NoError(t, err)
		row, err := parser.Read(nil)
		require.NoError(t, err)
		require.Equal(t, "a", row[0].Val, "LOAD strips the last column's trailing CR even inside quotes")
	})
	t.Run("non-last column keeps its CR", func(t *testing.T) {
		// The field terminator follows, so the reader's CR strip never sees it.
		var buf bytes.Buffer
		require.NoError(t, formatOutputString(newGuardCfg(0, 0), []byte("a\r"), []byte(","), 0, false, false, &buf))
		require.Equal(t, "a\r,", buf.String())
	})
	t.Run("non-newline terminator keeps the CR", func(t *testing.T) {
		// LOAD only strips the trailing CR when its normalized terminator is
		// '\n'; with LINES TERMINATED BY '|' the same value round-trips.
		ep := newOutfileExportConfig(tree.NewFields(",", false, 0, 0), nil, "|")
		ep.ctx = context.Background()
		var buf bytes.Buffer
		require.NoError(t, formatOutputString(ep, []byte("1"), []byte(","), 0, false, false, &buf))
		require.NoError(t, formatOutputString(ep, []byte("a\r"), []byte("|"), 0, false, true, &buf))
		require.Equal(t, "1,a\r|", buf.String())
		parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
			FieldsTerminatedBy: ",", FieldsEscapedBy: "", LinesTerminatedBy: "|",
			Null: []string{`\N`},
		}, bytes.NewReader(buf.Bytes()), csvparser.ReadBlockSize, false)
		require.NoError(t, err)
		row, err := parser.Read(nil)
		require.NoError(t, err)
		require.Equal(t, "a\r", row[1].Val)
	})
	t.Run("crlf terminator normalizes to newline and still strips", func(t *testing.T) {
		// LOAD maps TERMINATED BY '\r\n' to '\n' before deciding on the CR
		// strip, so the guard must apply there as well.
		ep := newOutfileExportConfig(tree.NewFields(",", false, 0, 0), nil, "\r\n")
		ep.ctx = context.Background()
		var buf bytes.Buffer
		require.ErrorContains(t, formatOutputString(ep, []byte("a\r"), []byte("\r\n"), 0, false, true, &buf), wantErr)
	})
	t.Run("multi-byte terminator starting with LF keeps the CR", func(t *testing.T) {
		// LOAD compares its complete normalized terminator with LF:
		// TERMINATED BY '\n|' disables the CR strip, so the guard must not
		// fire even though the terminator's first byte is LF.
		ep := newOutfileExportConfig(tree.NewFields(",", false, 0, 0), nil, "\n|")
		ep.ctx = context.Background()
		var buf bytes.Buffer
		require.NoError(t, formatOutputString(ep, []byte("1"), []byte(","), 0, false, false, &buf))
		require.NoError(t, formatOutputString(ep, []byte("a\r"), []byte("\n|"), 0, false, true, &buf))
		require.Equal(t, "1,a\r\n|", buf.String())
		parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
			FieldsTerminatedBy: ",", FieldsEscapedBy: "", LinesTerminatedBy: "\n|",
			Null: []string{`\N`},
		}, bytes.NewReader(buf.Bytes()), csvparser.ReadBlockSize, false)
		require.NoError(t, err)
		row, err := parser.Read(nil)
		require.NoError(t, err)
		require.Equal(t, "a\r", row[1].Val, "LOAD keeps the CR under a non-LF terminator")
	})
	t.Run("custom escape protects the CR", func(t *testing.T) {
		ep := newGuardCfg('#', 0)
		var buf bytes.Buffer
		require.NoError(t, formatOutputString(ep, []byte("a\r"), []byte("\n"), 0, false, true, &buf))
		require.Equal(t, "a#r\n", buf.String())
		parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
			FieldsTerminatedBy: ",", FieldsEscapedBy: "#", LinesTerminatedBy: "\n",
			Null: []string{`\N`},
		}, bytes.NewReader(buf.Bytes()), csvparser.ReadBlockSize, false)
		require.NoError(t, err)
		row, err := parser.Read(nil)
		require.NoError(t, err)
		require.Equal(t, "a\r", row[0].Val, "escaped CR must reload intact")
	})
	t.Run("default escape protects the CR", func(t *testing.T) {
		ep := newGuardCfg('\\', 0)
		var buf bytes.Buffer
		require.NoError(t, formatOutputString(ep, []byte("a\r"), []byte("\n"), 0, false, true, &buf))
		parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
			FieldsTerminatedBy: ",", FieldsEscapedBy: "\\", LinesTerminatedBy: "\n",
			Null: []string{`\N`},
		}, bytes.NewReader(buf.Bytes()), csvparser.ReadBlockSize, false)
		require.NoError(t, err)
		row, err := parser.Read(nil)
		require.NoError(t, err)
		require.Equal(t, "a\r", row[0].Val)
	})

	t.Run("batch writer rejects CR-final last column without escape", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer func() { require.Zero(t, mp.CurrNB()) }()
		bat := newIdNameBatch(t, mp, "a\r")
		defer bat.Clean(mp)
		ep := selectIntoOutfileConfig(",", 0, 0, "", "\n")
		ep.ctx = context.Background()
		ch := make(chan *BatchByte, 1)
		constructByte(ep.ctx, &backSession{feSessionImpl: feSessionImpl{pool: mp}}, bat, 0, ch, ep)
		result := <-ch
		require.ErrorContains(t, result.err, wantErr)
	})

	t.Run("result-set writer only flags the last column", func(t *testing.T) {
		ep := selectIntoOutfileConfig(",", 0, 0, "", "\n")
		ep.ctx = context.Background()
		mrs := &MysqlResultSet{}
		for _, name := range []string{"a", "b"} {
			col := &MysqlColumn{}
			col.SetName(name)
			col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
			mrs.AddColumn(col)
		}
		ep.mrs = mrs
		var out bytes.Buffer
		old := Write
		Write = func(_ *ExportConfig, b []byte) (int, error) { return out.Write(b) }
		defer func() { Write = old }()

		// CR in a non-last column followed by a plain last column: allowed.
		ep.mrs.Data = [][]any{{"a\r", "x"}}
		require.NoError(t, exportDataFromResultSetToCSVFile(ep))
		// The very same value in the last column must be rejected.
		ep.mrs.Data = [][]any{{"x", "a\r"}}
		require.ErrorContains(t, exportDataFromResultSetToCSVFile(ep), wantErr)
	})
}

// TestConstructByteVectorTypesErrorPlumbing sweeps every vector type the CSV
// batch writer can emit: the mechanical rewrite routing each arm through the
// first-error closure must keep writing rows unchanged, and a guard failure
// on any column must abort the batch with that error.
func TestConstructByteVectorTypesErrorPlumbing(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()

	t.Run("all vector types convert without error", func(t *testing.T) {
		jsonBytes, err := types.EncodeJson(bytejson.Null)
		require.NoError(t, err)
		fixed := func(typ types.Type, v any) *vector.Vector {
			vec := vector.NewOffHeapVecWithType(typ)
			switch val := v.(type) {
			case bool:
				require.NoError(t, vector.AppendFixed(vec, val, false, mp))
			case int8:
				require.NoError(t, vector.AppendFixed(vec, val, false, mp))
			case int16:
				require.NoError(t, vector.AppendFixed(vec, val, false, mp))
			case int32:
				require.NoError(t, vector.AppendFixed(vec, val, false, mp))
			case int64:
				require.NoError(t, vector.AppendFixed(vec, val, false, mp))
			case uint8:
				require.NoError(t, vector.AppendFixed(vec, val, false, mp))
			case uint16:
				require.NoError(t, vector.AppendFixed(vec, val, false, mp))
			case uint32:
				require.NoError(t, vector.AppendFixed(vec, val, false, mp))
			case uint64:
				require.NoError(t, vector.AppendFixed(vec, val, false, mp))
			case float32:
				require.NoError(t, vector.AppendFixed(vec, val, false, mp))
			case float64:
				require.NoError(t, vector.AppendFixed(vec, val, false, mp))
			case types.Date:
				require.NoError(t, vector.AppendFixed(vec, val, false, mp))
			case types.Datetime:
				require.NoError(t, vector.AppendFixed(vec, val, false, mp))
			case types.Time:
				require.NoError(t, vector.AppendFixed(vec, val, false, mp))
			case types.Timestamp:
				require.NoError(t, vector.AppendFixed(vec, val, false, mp))
			case types.MoYear:
				require.NoError(t, vector.AppendFixed(vec, val, false, mp))
			case types.Decimal64:
				require.NoError(t, vector.AppendFixed(vec, val, false, mp))
			case types.Decimal128:
				require.NoError(t, vector.AppendFixed(vec, val, false, mp))
			case types.Decimal256:
				require.NoError(t, vector.AppendFixed(vec, val, false, mp))
			case types.Uuid:
				require.NoError(t, vector.AppendFixed(vec, val, false, mp))
			case types.Rowid:
				require.NoError(t, vector.AppendFixed(vec, val, false, mp))
			case types.Blockid:
				require.NoError(t, vector.AppendFixed(vec, val, false, mp))
			case types.Enum:
				require.NoError(t, vector.AppendFixed(vec, val, false, mp))
			default:
				t.Fatalf("unhandled fixed type %T", v)
			}
			return vec
		}
		bytemap := func(typ types.Type, b []byte) *vector.Vector {
			vec := vector.NewOffHeapVecWithType(typ)
			require.NoError(t, vector.AppendBytes(vec, b, false, mp))
			return vec
		}
		geomBytes := geo.WriteWKB(geo.Point{X: 1.5, Y: -2.5})
		bat := &batch.Batch{}
		bat.Vecs = []*vector.Vector{
			fixed(types.New(types.T_bool, 0, 0), true),
			fixed(types.New(types.T_bool, 0, 0), false),
			fixed(types.New(types.T_int8, 0, 0), int8(-8)),
			fixed(types.New(types.T_int16, 0, 0), int16(-16)),
			fixed(types.New(types.T_int32, 0, 0), int32(-32)),
			fixed(types.New(types.T_int64, 0, 0), int64(-64)),
			fixed(types.New(types.T_uint8, 0, 0), uint8(8)),
			fixed(types.New(types.T_uint16, 0, 0), uint16(16)),
			fixed(types.New(types.T_uint32, 0, 0), uint32(32)),
			fixed(types.New(types.T_uint64, 0, 0), uint64(64)),
			// width 0 selects the general float format; a scale selects the fixed one
			fixed(types.New(types.T_float32, 0, 0), float32(1.5)),
			fixed(types.New(types.T_float32, 32, 2), float32(1.5)),
			fixed(types.New(types.T_float64, 0, 0), float64(1.5)),
			fixed(types.New(types.T_float64, 64, 2), float64(1.5)),
			fixed(types.New(types.T_bit, 16, 0), uint64(0x1234)),
			bytemap(types.New(types.T_varchar, 65535, 0), []byte("x,y")),
			bytemap(types.New(types.T_char, 8, 0), []byte("c")),
			bytemap(types.New(types.T_blob, 0, 0), []byte{0, 1, 2}),
			bytemap(types.T_json.ToType(), jsonBytes),
			bytemap(types.T_geometry.ToType(), geomBytes),
			bytemap(types.New(types.T_array_float32, 0, 0), types.ArrayToBytes([]float32{1, 2})),
			bytemap(types.New(types.T_array_float64, 0, 0), types.ArrayToBytes([]float64{1, 2})),
			bytemap(types.New(types.T_array_bf16, 0, 0), types.ArrayToBytes([]types.BF16{1})),
			bytemap(types.New(types.T_array_float16, 0, 0), types.ArrayToBytes([]types.Float16{1})),
			bytemap(types.New(types.T_array_int8, 0, 0), types.ArrayToBytes([]int8{1, 2})),
			bytemap(types.New(types.T_array_uint8, 0, 0), types.ArrayToBytes([]uint8{1, 2})),
			fixed(types.New(types.T_date, 0, 0), types.Date(744869)),
			fixed(types.New(types.T_datetime, 26, 2), types.Datetime(63442272000000)),
			fixed(types.New(types.T_time, 10, 0), types.Time(37230000000)),
			fixed(types.New(types.T_timestamp, 26, 0), types.Timestamp(1672531200000000)),
			fixed(types.New(types.T_year, 0, 0), types.MoYear(2024)),
			fixed(types.New(types.T_decimal64, 18, 2), types.Decimal64(12345)),
			fixed(types.New(types.T_decimal128, 38, 2), types.Decimal128{B0_63: 12345}),
			fixed(types.New(types.T_decimal256, 76, 2), types.Decimal256{B0_63: 1}),
			fixed(types.T_uuid.ToType(), types.Uuid{1}),
			fixed(types.T_Rowid.ToType(), types.Rowid{1}),
			fixed(types.T_Blockid.ToType(), types.Blockid{1}),
			fixed(types.New(types.T_enum, 0, 0), types.Enum(3)),
		}
		defer bat.Clean(mp)
		bat.SetRowCount(1)

		n := len(bat.Vecs)
		ep := &ExportConfig{
			userConfig: &tree.ExportParam{
				Fields: tree.NewFields(",", false, 0, '\\'),
				Lines:  tree.NewLines("", "\n"),
			},
			ctx:        context.Background(),
			Symbol:     make([][]byte, n),
			ColumnFlag: make([]bool, n),
		}
		for i := range ep.Symbol {
			ep.Symbol[i] = []byte{','}
		}
		ep.Symbol[n-1] = []byte{'\n'}

		ch := make(chan *BatchByte, 1)
		constructByte(ep.ctx, &backSession{feSessionImpl: feSessionImpl{pool: mp, timeZone: time.UTC}}, bat, 0, ch, ep)
		result := <-ch
		require.NoError(t, result.err)
		require.EqualValues(t, 1, result.rowCount)
		require.True(t, bytes.HasSuffix(result.writeByte, []byte("\n")))
		// Every column contributed bytes separated by the field terminator
		// (array-tostring output contains more commas, hence the lower bound).
		require.GreaterOrEqual(t, bytes.Count(result.writeByte, []byte{','}), n-1)
	})

	t.Run("guard failure on a non-last column aborts the batch", func(t *testing.T) {
		bat := &batch.Batch{}
		bat.Vecs = []*vector.Vector{
			vector.NewOffHeapVecWithType(types.New(types.T_int32, 0, 0)),
			vector.NewOffHeapVecWithType(types.New(types.T_varchar, 65535, 0)),
			vector.NewOffHeapVecWithType(types.New(types.T_varchar, 65535, 0)),
		}
		defer bat.Clean(mp)
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(1), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte(`\N`), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[2], []byte("x"), false, mp))
		bat.SetRowCount(1)

		ep := &ExportConfig{
			userConfig: &tree.ExportParam{
				Fields: tree.NewFields(",", false, 0, '#'),
				Lines:  tree.NewLines("", "\n"),
			},
			ctx:        context.Background(),
			Symbol:     [][]byte{{','}, {','}, {'\n'}},
			ColumnFlag: make([]bool, 3),
		}
		ch := make(chan *BatchByte, 1)
		constructByte(ep.ctx, &backSession{feSessionImpl: feSessionImpl{pool: mp}}, bat, 0, ch, ep)
		result := <-ch
		// The guard fires on the middle column; the closure must skip the
		// third column and the batch must surface the first error only.
		require.ErrorContains(t, result.err, "cannot round-trip unless FIELDS ESCAPED BY is the default")
		require.Empty(t, result.writeByte)
	})
	t.Run("result-set writer covers time, uuid and enum columns", func(t *testing.T) {
		ep := &ExportConfig{
			userConfig: &tree.ExportParam{
				Fields: tree.NewFields(",", false, 0, '\\'),
				Lines:  tree.NewLines("", "\n"),
			},
			ctx:        context.Background(),
			Symbol:     [][]byte{{','}, {','}, {'\n'}},
			ColumnFlag: make([]bool, 3),
			mrs:        &MysqlResultSet{},
		}
		for _, c := range []struct {
			name string
			typ  defines.MysqlType
		}{{"t", defines.MYSQL_TYPE_TIME}, {"u", defines.MYSQL_TYPE_UUID}, {"e", defines.MYSQL_TYPE_ENUM}} {
			col := &MysqlColumn{}
			col.SetName(c.name)
			col.SetColumnType(c.typ)
			ep.mrs.AddColumn(col)
		}
		ep.mrs.Data = [][]any{{types.Time(37230000000), "550e8400-e29b-41d4-a716-446655440000", "red"}}
		var out bytes.Buffer
		old := Write
		Write = func(_ *ExportConfig, b []byte) (int, error) { return out.Write(b) }
		defer func() { Write = old }()
		require.NoError(t, exportDataFromResultSetToCSVFile(ep))
		require.True(t, bytes.HasSuffix(out.Bytes(), []byte("\n")))
	})
}

// TestOutfileTranslatedEscapeByteValueRejected covers the escape-letter
// collision: LOAD's unescape translates the second byte of an escape pair
// from a fixed letter table ({0,b,n,r,t,Z} -> control character) whatever
// the configured escape byte is, so when the escape byte IS one of those
// letters a value containing it can never round-trip (the doubled byte
// decodes to a control character). The writer must reject it; values
// without the letter, NULLs, and letter values under a non-letter escape
// are unaffected.
func TestOutfileTranslatedEscapeByteValueRejected(t *testing.T) {
	newGuardCfg := func(escaped, enclosed byte) *ExportConfig {
		ep := newOutfileExportConfig(tree.NewFields(",", false, enclosed, escaped), nil, "\n")
		ep.ctx = context.Background()
		return ep
	}
	// The reviewer's probe: escape 'n' would export 'banana' as 'bannanna',
	// which LOAD decodes to "ba\na\na" (hex 62610A610A61). Document the
	// reader corruption the writer is now obliged to prevent.
	parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
		FieldsTerminatedBy: ",", FieldsEscapedBy: "n", LinesTerminatedBy: "\n",
		Null: []string{`\N`},
	}, bytes.NewReader([]byte("bannanna\n")), csvparser.ReadBlockSize, false)
	require.NoError(t, err)
	row, err := parser.Read(nil)
	require.NoError(t, err)
	require.Equal(t, "ba\na\na", row[0].Val, "LOAD translates every nn pair to LF")

	for _, letter := range []byte{'0', 'b', 'n', 'r', 't', 'Z'} {
		t.Run(fmt.Sprintf("letter %c", letter), func(t *testing.T) {
			var buf bytes.Buffer
			withLetter := []byte{'x', letter, 'y'}
			wantErr := fmt.Sprintf("cannot round-trip with FIELDS ESCAPED BY '%c'", letter)
			// Bare and enclosed values are rejected alike: escapes are
			// decoded inside quotes too.
			require.ErrorContains(t, formatOutputString(newGuardCfg(letter, 0), withLetter, []byte("\n"), 0, false, true, &buf), wantErr)
			buf.Reset()
			require.ErrorContains(t, formatOutputString(newGuardCfg(letter, '"'), withLetter, []byte("\n"), '"', true, true, &buf), wantErr)
			// A value free of the escape letter exports and round-trips.
			buf.Reset()
			require.NoError(t, formatOutputString(newGuardCfg(letter, 0), []byte("safe"), []byte("\n"), 0, false, true, &buf))
			require.Equal(t, "safe\n", buf.String())
		})
	}

	t.Run("NULL unaffected under a letter escape", func(t *testing.T) {
		// The \N marker contains no lowercase translated letter; only the
		// uppercase-N escape collides with it (covered separately).
		ep := newGuardCfg('n', 0)
		var buf bytes.Buffer
		require.NoError(t, formatOutputNull(ep, []byte("\n"), 0, true, &buf))
		require.Equal(t, "\\N\n", buf.String())
	})

	t.Run("batch writer aborts on a letter-valued row", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer func() { require.Zero(t, mp.CurrNB()) }()
		bat := newIdNameBatch(t, mp, "banana")
		defer bat.Clean(mp)
		ep := selectIntoOutfileConfig(",", 0, 'n', "", "\n")
		ep.ctx = context.Background()
		ch := make(chan *BatchByte, 1)
		constructByte(ep.ctx, &backSession{feSessionImpl: feSessionImpl{pool: mp}}, bat, 0, ch, ep)
		result := <-ch
		require.ErrorContains(t, result.err, "cannot round-trip with FIELDS ESCAPED BY 'n'")
	})
}

// TestOutfileNullWithEscapeNRejected: the NULL marker always ends in 'N'.
// With FIELDS ESCAPED BY 'N' LOAD's reader consumes the byte after the
// marker (the separator or terminator) as escape content, merging fields
// or records, so a NULL cannot round-trip under that clause and the writer
// must reject it. Values, including ones containing 'N', still round-trip:
// the doubled NN decodes back to a literal N.
func TestOutfileNullWithEscapeNRejected(t *testing.T) {
	newGuardCfg := func(escaped, enclosed byte) *ExportConfig {
		ep := newOutfileExportConfig(tree.NewFields(",", false, enclosed, escaped), nil, "\n")
		ep.ctx = context.Background()
		return ep
	}
	const wantErr = "a NULL value cannot round-trip with FIELDS ESCAPED BY 'N'"

	t.Run("NULL write rejected", func(t *testing.T) {
		var buf bytes.Buffer
		require.ErrorContains(t, formatOutputNull(newGuardCfg('N', 0), []byte("\n"), 0, true, &buf), wantErr)
	})

	t.Run("batch writer aborts on a NULL row", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer func() { require.Zero(t, mp.CurrNB()) }()
		bat := newIdNameBatch(t, mp, nil)
		defer bat.Clean(mp)
		ep := selectIntoOutfileConfig(",", 0, 'N', "", "\n")
		ep.ctx = context.Background()
		ch := make(chan *BatchByte, 1)
		constructByte(ep.ctx, &backSession{feSessionImpl: feSessionImpl{pool: mp}}, bat, 0, ch, ep)
		result := <-ch
		require.ErrorContains(t, result.err, wantErr)
	})

	t.Run("result-set writer aborts on a NULL row", func(t *testing.T) {
		ep := selectIntoOutfileConfig(",", 0, 'N', "", "\n")
		ep.ctx = context.Background()
		ep.mrs.Data = [][]any{{int32(1), nil}}
		require.ErrorContains(t, exportDataFromResultSetToCSVFile(ep), wantErr)
	})

	t.Run("values containing N still round-trip", func(t *testing.T) {
		ep := newGuardCfg('N', 0)
		var buf bytes.Buffer
		require.NoError(t, formatOutputString(ep, []byte("Nyx"), []byte("\n"), 0, false, true, &buf))
		require.Equal(t, "NNyx\n", buf.String())
		parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
			FieldsTerminatedBy: ",", FieldsEscapedBy: "N", LinesTerminatedBy: "\n",
			Null: []string{`\N`},
		}, bytes.NewReader(buf.Bytes()), csvparser.ReadBlockSize, false)
		require.NoError(t, err)
		row, err := parser.Read(nil)
		require.NoError(t, err)
		require.Equal(t, "Nyx", row[0].Val)
	})

	t.Run("document the reader merge the writer now prevents", func(t *testing.T) {
		// The reviewer's probe: a `1,\N` NULL record under escape 'N' merges
		// with the next record instead of reading as two columns.
		parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
			FieldsTerminatedBy: ",", FieldsEscapedBy: "N", LinesTerminatedBy: "\n",
			Null: []string{`\N`},
		}, bytes.NewReader([]byte("1,\\N\n2,tail\n")), csvparser.ReadBlockSize, false)
		require.NoError(t, err)
		row, err := parser.Read(nil)
		require.NoError(t, err)
		require.NotEqual(t, 2, len(row), "the NULL marker swallows the terminator and merges records")
	})
}

// TestOutfileBareNullLiteralEscapesLetterBytes pins the literal word NULL
// written bare (no enclosure): the writer prefixes the first N to stop LOAD
// reading SQL NULL and must also encode occurrences of the configured escape
// byte inside the word. Otherwise LOAD strips them away ('UNULL' reads as
// 'NLL', 'LNULL' as 'NUL').
func TestOutfileBareNullLiteralEscapesLetterBytes(t *testing.T) {
	cases := []struct {
		name     string
		escape   byte
		expected string
	}{
		{"escape U", 'U', "UNUULL"},
		{"escape L", 'L', "LNULLLL"},
		{"escape N", 'N', "NNNULL"},
		{"legacy backslash", '\\', `\NULL`},
		{"escape hash", '#', "#NULL"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ep := newOutfileExportConfig(tree.NewFields(",", false, 0, c.escape), nil, "\n")
			got := addEscapeToString(ep, []byte("NULL"), 0)
			require.Equal(t, c.expected, string(got))

			// The written bytes read back through LOAD's parser as the
			// literal word, not SQL NULL and not a shortened word.
			parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
				FieldsTerminatedBy: ",", FieldsEscapedBy: string(c.escape), LinesTerminatedBy: "\n",
				Null: []string{`\N`},
			}, bytes.NewReader([]byte(c.expected+"\n")), csvparser.ReadBlockSize, false)
			require.NoError(t, err)
			row, err := parser.Read(nil)
			require.NoError(t, err)
			require.Len(t, row, 1)
			require.Equal(t, "NULL", row[0].Val)
			require.False(t, row[0].IsNull)

			// The guard lives in addEscapeToString, reached through the
			// string formatter used by both CSV row writers.
			var buf bytes.Buffer
			require.NoError(t, formatOutputString(ep, []byte("NULL"), []byte("\n"), 0, false, true, &buf))
			require.Equal(t, c.expected+"\n", buf.String())
		})
	}

	t.Run("document why inner escape bytes are encoded", func(t *testing.T) {
		// The reviewer's probes against the first-N-only shortcut: LOAD's
		// unescape collapses the escape byte inside the word.
		for _, probe := range []struct {
			data, escape, want string
		}{
			{"UNULL\n", "U", "NLL"},
			{"LNULL\n", "L", "NUL"},
		} {
			parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
				FieldsTerminatedBy: ",", FieldsEscapedBy: probe.escape, LinesTerminatedBy: "\n",
				Null: []string{`\N`},
			}, bytes.NewReader([]byte(probe.data)), csvparser.ReadBlockSize, false)
			require.NoError(t, err)
			row, err := parser.Read(nil)
			require.NoError(t, err)
			require.Len(t, row, 1)
			require.Equal(t, probe.want, row[0].Val)
		}
	})
}

// TestOutfileNewlineEscapeByteRequiresEnclosure rejects a bare CR or LF
// ESCAPED BY when LOAD's parser still recognizes newlines as record
// boundaries (empty/LF/CRLF terminators): csvparser consumes the newline
// BEFORE applying the escape byte, so an escaped separator lands after the
// record the escape byte itself ended. Enclosed output keeps both bytes
// inside quotes, and non-newline terminators disable newline recognition.
func TestOutfileNewlineEscapeByteRequiresEnclosure(t *testing.T) {
	const (
		newRuleMsg = "CSV FIELDS ESCAPED BY must not be CR or LF without a nonempty ENCLOSED BY"
		oldRuleMsg = "require a nonempty ENCLOSED BY"
	)
	t.Run("rejected without enclosure", func(t *testing.T) {
		for _, c := range []struct {
			name    string
			escape  byte
			line    string
			message string
		}{
			// CR escape with the default or empty terminator only trips
			// the new rule: no configured terminator byte equals CR.
			{"CR escape LF term", '\r', "\n", newRuleMsg},
			{"CR escape empty term", '\r', "", newRuleMsg},
			// The remaining newline-mode cells are already caught by the
			// earlier terminator/escape collision rule; assert the shared
			// trailer so either rejection ordering satisfies the test.
			{"CR escape CRLF term", '\r', "\r\n", oldRuleMsg},
			{"LF escape LF term", '\n', "\n", oldRuleMsg},
			{"LF escape CRLF term", '\n', "\r\n", oldRuleMsg},
			{"LF escape empty term", '\n', "", oldRuleMsg},
		} {
			t.Run(c.name, func(t *testing.T) {
				ep := newOutfileExportConfig(tree.NewFields(",", false, 0, c.escape), nil, c.line)
				require.ErrorContains(t, ep.validateCSVFraming(context.Background()), c.message)
			})
		}
	})

	t.Run("allowed configurations", func(t *testing.T) {
		for _, c := range []struct {
			name    string
			escape  byte
			enclose byte
			line    string
		}{
			// A non-newline terminator makes LOAD stop treating CR/LF as
			// record boundaries, so the escape byte survives.
			{"CR escape pipe term", '\r', 0, "|"},
			{"LF escape pipe term", '\n', 0, "|"},
			// Enclosure keeps the escape byte and the newline inside
			// quotes where both stay literal.
			{"CR escape enclosed LF", '\r', '"', "\n"},
			{"LF escape enclosed LF", '\n', '"', "\n"},
			{"CR escape enclosed CRLF", '\r', '"', "\r\n"},
			{"CR escape enclosed pipe", '\r', '"', "|"},
		} {
			t.Run(c.name, func(t *testing.T) {
				ep := newOutfileExportConfig(tree.NewFields(",", false, c.enclose, c.escape), nil, c.line)
				require.NoError(t, ep.validateCSVFraming(context.Background()))
			})
		}
	})

	t.Run("terminator normalization in the reader model", func(t *testing.T) {
		// validateCSVFraming's earlier collision rule intercepts CR/LF
		// escapes before this helper sees a CRLF terminator, so exercise
		// the normalization directly.
		for _, c := range []struct {
			name string
			ep   *ExportConfig
			want bool
		}{
			{"CRLF normalizes to LF", selectIntoOutfileConfig(",", 0, '\\', "", "\r\n"), true},
			{"LF", selectIntoOutfileConfig(",", 0, '\\', "", "\n"), true},
			{"empty terminator", selectIntoOutfileConfig(",", 0, '\\', "", ""), true},
			{"pipe disables newline recognition", selectIntoOutfileConfig(",", 0, '\\', "", "|"), false},
			{"LF-prefixed composite disables it", selectIntoOutfileConfig(",", 0, '\\', "", "\n|"), false},
			{"nil lines defaults to LF", &ExportConfig{userConfig: &tree.ExportParam{Fields: tree.NewFields(",", false, 0, '\\')}}, true},
		} {
			t.Run(c.name, func(t *testing.T) {
				require.Equal(t, c.want, c.ep.readerRecognizesNewlineBoundaries())
			})
		}
	})

	t.Run("document the reader corruption the rule prevents", func(t *testing.T) {
		// The reviewer's probe: escape CR ends the record at the byte
		// meant to escape the comma, so 'a\r,b' reads back as 'a'.
		parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
			FieldsTerminatedBy: ",", FieldsEscapedBy: "\r", LinesTerminatedBy: "\n",
			LinesStartingBy: "p:", Null: []string{`\N`},
		}, bytes.NewReader([]byte("p:1,a\r,b\np:2,cd\n")), csvparser.ReadBlockSize, false)
		require.NoError(t, err)
		row, err := parser.Read(nil)
		require.NoError(t, err)
		require.Len(t, row, 2)
		require.Equal(t, "a", row[1].Val, "the escape byte swallows the rest of the value")
	})

	t.Run("document enclosed and non-newline round-trips", func(t *testing.T) {
		enclosed, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
			FieldsTerminatedBy: ",", FieldsEscapedBy: "\r", FieldsEnclosedBy: `"`,
			LinesTerminatedBy: "\n", LinesStartingBy: "p:", Null: []string{`\N`},
		}, bytes.NewReader([]byte("p:1,\"a\r,b\"\np:2,cd\n")), csvparser.ReadBlockSize, false)
		require.NoError(t, err)
		row, err := enclosed.Read(nil)
		require.NoError(t, err)
		require.Equal(t, "a,b", row[1].Val)

		pipe, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
			FieldsTerminatedBy: ",", FieldsEscapedBy: "\r", LinesTerminatedBy: "|",
			LinesStartingBy: "p:", Null: []string{`\N`},
		}, bytes.NewReader([]byte("p:1,a\r,b|p:2,cd|")), csvparser.ReadBlockSize, false)
		require.NoError(t, err)
		row, err = pipe.Read(nil)
		require.NoError(t, err)
		require.Equal(t, "a,b", row[1].Val)
	})
}

// TestOutfileNewlineEscapeByteEnclosesBinaryValues pins the promote-to-enclosure
// rule: with a CR or LF FIELDS ESCAPED BY and LOAD recognizing newlines, no
// CR-prefixed escape can protect a bare field (the reader ends the record at
// the escape byte before decoding it). The shared formatter therefore encloses
// any value whose bare encoding contains the escape byte — including BIT,
// whose callers pass quote=false so the ambiguity checks never saw it.
func TestOutfileNewlineEscapeByteEnclosesBinaryValues(t *testing.T) {
	// BIT(24) 6368354 = 0x612C62 = "a,b": the reviewer's probe value.
	bitAB := []byte{0x61, 0x2c, 0x62}

	roundTrip := func(t *testing.T, data, escaped, enclosed, lineTerm, startingBy string, want string) {
		t.Helper()
		parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
			FieldsTerminatedBy: ",", FieldsEscapedBy: escaped, FieldsEnclosedBy: enclosed,
			LinesTerminatedBy: lineTerm, LinesStartingBy: startingBy, Null: []string{`\N`},
		}, bytes.NewReader([]byte(data)), csvparser.ReadBlockSize, false)
		require.NoError(t, err)
		row, err := parser.Read(nil)
		require.NoError(t, err)
		require.NotEmpty(t, row)
		require.Equal(t, want, row[len(row)-1].Val)
	}

	t.Run("formatter promotes values whose encoding emits the escape byte", func(t *testing.T) {
		ep := newOutfileExportConfig(tree.NewFields(",", false, '"', '\r'), nil, "\n")
		for _, c := range []struct {
			name string
			tmp  []byte
			want string
		}{
			{"BIT bytes with separator", bitAB, "\"a,b\"\n"},
			{"plain value stays raw", []byte("plain"), "plain\n"},
			{"embedded CR encodes as escape-r", []byte("a\rb"), "\"a\rrb\"\n"},
			{"trailing CR encodes as escape-r", []byte{0x61, 0x0d}, "\"a\rr\"\n"},
		} {
			t.Run(c.name, func(t *testing.T) {
				var buf bytes.Buffer
				require.NoError(t, formatOutputString(ep, c.tmp, []byte("\n"), '"', false, true, &buf))
				require.Equal(t, c.want, buf.String())
			})
		}
		// Every promoted record reads back losslessly through LOAD's
		// parser: inside quotes the separator stays literal and the
		// translated escape-r decodes back to CR (csvparser unescape).
		roundTrip(t, "\"a,b\"\n", "\r", `"`, "\n", "", "a,b")
		roundTrip(t, "\"a\rrb\"\n", "\r", `"`, "\n", "", "a\rb")
		roundTrip(t, "\"a\rr\"\n", "\r", `"`, "\n", "", "a\r")
	})

	t.Run("LF escape byte promotes too", func(t *testing.T) {
		ep := newOutfileExportConfig(tree.NewFields(",", false, '"', '\n'), nil, "\n")
		var buf bytes.Buffer
		require.NoError(t, formatOutputString(ep, bitAB, []byte("\n"), '"', false, true, &buf))
		require.Equal(t, "\"a,b\"\n", buf.String())
		roundTrip(t, buf.String(), "\n", `"`, "\n", "", "a,b")
	})

	t.Run("non-newline terminator keeps the bare encoding", func(t *testing.T) {
		ep := newOutfileExportConfig(tree.NewFields(",", false, '"', '\r'), nil, "|")
		var buf bytes.Buffer
		require.NoError(t, formatOutputString(ep, bitAB, []byte("|"), '"', false, true, &buf))
		require.Equal(t, "a\r,b|", buf.String(), "LOAD recognizes no newlines here, so CR stays a plain escape")
		roundTrip(t, buf.String(), "\r", `"`, "|", "", "a,b")
	})

	t.Run("default backslash escape is unaffected", func(t *testing.T) {
		ep := newOutfileExportConfig(tree.NewFields(",", false, '"', '\\'), nil, "\n")
		var buf bytes.Buffer
		require.NoError(t, formatOutputString(ep, bitAB, []byte("\n"), '"', false, true, &buf))
		require.Equal(t, "a\\,b\n", buf.String())
		roundTrip(t, buf.String(), "\\", `"`, "\n", "", "a,b")
	})

	t.Run("batch writer encloses BIT", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer func() { require.Zero(t, mp.CurrNB()) }()
		bat := batch.NewOffHeapWithSize(2)
		bat.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int32.ToType())
		bat.Vecs[1] = vector.NewOffHeapVecWithType(types.New(types.T_bit, 24, 0))
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(1), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], uint64(6368354), false, mp))
		bat.SetRowCount(1)
		defer bat.Clean(mp)

		ep := selectIntoOutfileConfig(",", '"', '\r', "p:", "\n")
		ep.ctx = context.Background()
		ch := make(chan *BatchByte, 1)
		constructByte(ep.ctx, &backSession{feSessionImpl: feSessionImpl{pool: mp}}, bat, 0, ch, ep)
		result := <-ch
		require.NoError(t, result.err)
		require.Equal(t, "p:1,\"a,b\"\n", string(result.writeByte))
		roundTrip(t, string(result.writeByte), "\r", `"`, "\n", "p:", "a,b")
	})

	t.Run("result-set writer encloses BIT", func(t *testing.T) {
		ctx := context.Background()
		mrs := &MysqlResultSet{}
		bit := new(MysqlColumn)
		bit.SetName("b")
		bit.SetColumnType(defines.MYSQL_TYPE_BIT)
		bit.SetLength(24)
		mrs.AddColumn(bit)
		mrs.Data = [][]any{{uint64(6368354)}}

		ep := &ExportConfig{proc: sysFileAccessTestProcess(),
			userConfig: &tree.ExportParam{
				FilePath: filepath.Join(t.TempDir(), "rs_bit.csv"),
				Fields:   tree.NewFields(",", false, '"', '\r'),
				Lines:    tree.NewLines("p:", "\n"),
			},
			ctx: ctx,
			mrs: mrs,
		}
		initExportFileParam(ep, mrs)
		ep.init()
		ep.Index.Add(1)
		require.NoError(t, openNewFile(ctx, ep, mrs))
		t.Cleanup(func() {
			if ep.AsyncWriter != nil {
				_ = Close(ep)
			}
		})
		require.NoError(t, exportDataFromResultSetToCSVFile(ep))
		require.NoError(t, Close(ep))

		data, err := os.ReadFile(ep.userConfig.FilePath)
		require.NoError(t, err)
		require.Equal(t, "p:\"a,b\"\n", string(data))
		roundTrip(t, string(data), "\r", `"`, "\n", "p:", "a,b")
	})
}

// TestOutfileNullMarkerEnclosureCollisionSimulation pins the NULL-marker
// simulation: the emitted marker always is a literal backslash plus N, and
// whether LOAD's field scan survives depends on real parser behavior (an
// enclosure backslash consumes the marker's leading byte as a quote-open;
// the promoted N-wrapped form breaks when the escape byte forced
// promotion). formatOutputNull replays the emission against the loader's
// own parser configuration and rejects what does not read back as NULL.
func TestOutfileNullMarkerEnclosureCollisionSimulation(t *testing.T) {
	newGuardCfg := func(escaped, enclosed byte) *ExportConfig {
		ep := newOutfileExportConfig(tree.NewFields(",", false, enclosed, escaped), nil, "\n")
		ep.ctx = context.Background()
		return ep
	}

	t.Run("rejections", func(t *testing.T) {
		for _, c := range []struct {
			name            string
			escape, enclose byte
		}{
			// The reviewer's core probe: the bare marker's leading
			// backslash opens an enclosure that never closes.
			{"enclosure backslash, hash escape", '#', '\\'},
			// ENCLOSED BY 'N' promotes the marker (field or line equals
			// the escape byte) and the promoted form breaks, exactly the
			// reviewer's comma- and LF-escape probes.
			{"enclosure N, comma escape", ',', 'N'},
			{"enclosure N, LF escape", '\n', 'N'},
			// Only the default backslash enclosure promotion could parse
			// the marker; ESCAPED BY '' disables escaping entirely, so the
			// bare marker's leading backslash opens an unterminated quote.
			{"enclosure backslash, no escape", 0, '\\'},
		} {
			t.Run(c.name, func(t *testing.T) {
				var buf bytes.Buffer
				require.ErrorContains(t, formatOutputNull(newGuardCfg(c.escape, c.enclose), []byte("\n"), c.enclose, true, &buf),
					"a NULL value cannot round-trip with ENCLOSED BY")
			})
		}
	})

	t.Run("allowed configurations", func(t *testing.T) {
		for _, c := range []struct {
			name            string
			escape, enclose byte
			emitted         string
		}{
			// Loader-ground-truth controls: these all parse back to NULL
			// through csvparser with the loader's own settings.
			{"enclosure quote, hash escape", '#', '"', "\\N\n"},
			{"enclosure N, hash escape", '#', 'N', "\\N\n"},
			{"enclosure N, default escape", '\\', 'N', "\\N\n"},
			{"enclosure equals escape hash", '#', '#', "#\\N#\n"},
			{"enclosure equals escape backslash", '\\', '\\', "\\\\N\\\n"},
		} {
			t.Run(c.name, func(t *testing.T) {
				ep := newGuardCfg(c.escape, c.enclose)
				var buf bytes.Buffer
				require.NoError(t, formatOutputNull(ep, []byte("\n"), c.enclose, true, &buf))
				require.Equal(t, c.emitted, buf.String())
				parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
					FieldsTerminatedBy: ",", FieldsEscapedBy: string(c.escape), FieldsEnclosedBy: string(c.enclose),
					LinesTerminatedBy: "\n", Null: []string{`\N`}, UnescapedQuote: true,
				}, bytes.NewReader(buf.Bytes()), csvparser.ReadBlockSize, false)
				require.NoError(t, err)
				row, err := parser.Read(nil)
				require.NoError(t, err)
				require.Len(t, row, 1)
				require.True(t, row[0].IsNull)
			})
		}
	})

	t.Run("document the reader failures the simulation prevents", func(t *testing.T) {
		// The reviewer's probes against the loader's real configuration.
		for _, c := range []struct {
			data, escape, enclose, message string
		}{
			{"p:1,\\N\n", "#", "\\", "unterminated quoted field"},
			{"p:1,N\\NN\n", ",", "N", "unterminated quoted field"},
			{"p:1,N\\NN\n", "\n", "N", "no character after backslash"},
		} {
			parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
				FieldsTerminatedBy: ",", FieldsEscapedBy: c.escape, FieldsEnclosedBy: c.enclose,
				LinesTerminatedBy: "\n", LinesStartingBy: "p:", Null: []string{`\N`}, UnescapedQuote: true,
			}, bytes.NewReader([]byte(c.data)), csvparser.ReadBlockSize, false)
			require.NoError(t, err)
			_, err = parser.Read(nil)
			require.ErrorContains(t, err, c.message)
		}
	})

	t.Run("batch writer aborts on a NULL row", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer func() { require.Zero(t, mp.CurrNB()) }()
		bat := newIdNameBatch(t, mp, nil)
		defer bat.Clean(mp)
		ep := selectIntoOutfileConfig(",", '\\', '#', "p:", "\n")
		ep.ctx = context.Background()
		ch := make(chan *BatchByte, 1)
		constructByte(ep.ctx, &backSession{feSessionImpl: feSessionImpl{pool: mp}}, bat, 0, ch, ep)
		result := <-ch
		require.ErrorContains(t, result.err, "a NULL value cannot round-trip with ENCLOSED BY '\\'")
	})

	t.Run("result-set writer aborts on a NULL row", func(t *testing.T) {
		ep := selectIntoOutfileConfig(",", 'N', ',', "p:", "\n")
		ep.ctx = context.Background()
		ep.mrs.Data = [][]any{{int32(1), nil}}
		require.ErrorContains(t, exportDataFromResultSetToCSVFile(ep), "a NULL value cannot round-trip with ENCLOSED BY 'N'")
	})
}

// TestOutfileEscapeEqualsEnclosureSimulation pins the value-level round-trip
// simulation: with ENCLOSED BY equal to ESCAPED BY, whether an encoded field
// survives LOAD's enclosure scan is real parser behavior, not a static rule
// (mid-value CR under 'r' parses while a trailing CR opens an unterminated
// quoted field — the generated escape payload letter collides with the
// escape byte itself). formatOutputString replays the emission through the
// loader's own parser settings and rejects what does not come back
// byte-identical — through both CSV writers, BIT included.
func TestOutfileEscapeEqualsEnclosureSimulation(t *testing.T) {
	t.Run("rejections", func(t *testing.T) {
		// The reviewer's exact probe: VARCHAR bytes 61 0D (a trailing CR)
		// under ENCLOSED BY 'r' ESCAPED BY 'r'. The writer's own encoding
		// (double escape prefix plus the translated 'r') leaves an
		// ambiguous run of r-bytes that LOAD's enclosure scan cannot
		// resolve — reject instead of publishing an unparsable file.
		ep := newOutfileExportConfig(tree.NewFields(",", false, 'r', 'r'), nil, "\n")
		ep.ctx = context.Background()
		var buf bytes.Buffer
		require.ErrorContains(t, formatOutputString(ep, []byte{0x61, 0x0d}, []byte("\n"), 'r', false, true, &buf),
			"when ENCLOSED BY and ESCAPED BY are the same character 'r'")
	})

	t.Run("translated-escape guard layers above", func(t *testing.T) {
		// A literal 'r' inside the value was already rejected by the
		// translated-escape rule before the simulation runs.
		ep := newOutfileExportConfig(tree.NewFields(",", false, 'r', 'r'), nil, "\n")
		ep.ctx = context.Background()
		var buf bytes.Buffer
		require.ErrorContains(t, formatOutputString(ep, []byte("car"), []byte("\n"), 'r', false, true, &buf),
			"a value containing 'r' cannot round-trip with FIELDS ESCAPED BY 'r'")
	})

	t.Run("allowed configurations", func(t *testing.T) {
		// Loader-ground-truth controls: each accepted emission round-trips
		// byte-identically through csvparser with the loader's settings,
		// proving the simulation rejects only genuinely broken shapes.
		for _, c := range []struct {
			name    string
			escape  byte
			enclose byte
			tmp     []byte
			emitted string
		}{
			{"mid-value CR under r", 'r', 'r', []byte{0x61, 0x0d, 0x62}, "rarrrbr\n"},
			{"plain value under r", 'r', 'r', []byte("ab"), "rabr\n"},
			{"mid-value CR under hash", '#', '#', []byte{0x61, 0x0d, 0x62}, "#a##rb#\n"},
			{"mid-value CR under backslash", '\\', '\\', []byte{0x61, 0x0d, 0x62}, "\\a\\\\rb\\\n"},
			{"mid-value TAB stays raw under backslash", '\\', '\\', []byte("a\tb"), "\\a\tb\\\n"},
			{"raw LF under n", 'n', 'n', []byte("a\nb"), "na\nbn\n"},
			{"different enclosure escapes r", 'r', '"', []byte{0x61, 0x0d, 0x62}, "arrb\n"},
		} {
			t.Run(c.name, func(t *testing.T) {
				ep := newOutfileExportConfig(tree.NewFields(",", false, c.enclose, c.escape), nil, "\n")
				ep.ctx = context.Background()
				var buf bytes.Buffer
				require.NoError(t, formatOutputString(ep, c.tmp, []byte("\n"), c.enclose, false, true, &buf))
				require.Equal(t, c.emitted, buf.String())
				parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
					FieldsTerminatedBy: ",", FieldsEscapedBy: string(c.escape), FieldsEnclosedBy: string(c.enclose),
					LinesTerminatedBy: "\n", Null: []string{`\N`}, UnescapedQuote: true,
				}, bytes.NewReader(buf.Bytes()), csvparser.ReadBlockSize, false)
				require.NoError(t, err)
				row, err := parser.Read(nil)
				require.NoError(t, err)
				require.Len(t, row, 1)
				require.Equal(t, string(c.tmp), row[0].Val)
			})
		}
	})

	t.Run("simulation predicate branches", func(t *testing.T) {
		// CRLF collapses to LF for the simulated record terminator.
		ep := newOutfileExportConfig(tree.NewFields(",", false, '#', '#'), nil, "\r\n")
		ep.ctx = context.Background()
		require.Equal(t, "\n", string(ep.csvNormalizedRecordTerm()))
		// The NULL variant accepts a parsable marker.
		require.True(t, ep.csvFieldEmissionParses([]byte("#\\N#"), ep.csvNormalizedRecordTerm(), true, nil, true))
		// Trailing records fail the exactly-one-record requirement.
		require.False(t, ep.csvFieldEmissionParses([]byte("#a#\n#b#"), ep.csvNormalizedRecordTerm(), true, []byte("a"), false))
	})

	t.Run("absent ESCAPED BY mirrors the loader default escape", func(t *testing.T) {
		// Regression for the embed smoke legs: with no ESCAPED BY clause the
		// loader parses with a backslash escape (newReaderWithParam), so the
		// simulation must apply the same default. An absent clause used to
		// simulate escapeFlavorNone and reject every value whose encoding
		// contains a backslash. Pin the CI leg shapes: BIT CR triples, 'r'
		// and 'r:' separators, trailing CR, embedded backslash, NULL.
		newEp := func(fieldTerm string) *ExportConfig {
			ep := &ExportConfig{
				userConfig: &tree.ExportParam{
					Fields: &tree.Fields{
						Terminated: &tree.Terminated{Value: fieldTerm},
						EnclosedBy: &tree.EnclosedBy{Value: '\\'},
					},
					Lines: tree.NewLines("", "\n"),
				},
				ctx: context.Background(),
			}
			return ep
		}
		for _, c := range []struct {
			name   string
			field  string
			tmp    []byte
			want   string
			quoted bool
		}{
			{"bit mid CR", ",", []byte{0x61, 0x0d, 0x62}, "\\a\\\\rb\\\n", true},
			{"bit leading CR", ",", []byte{0x0d, 0x61, 0x62}, "\\\\\\rab\\\n", true},
			{"bit CR colon", "r:", []byte{0x61, 0x0d, 0x3a}, "\\a\\\\r:\\\n", true},
			{"string trailing CR", ",", []byte("tail\r"), "\\tail\\\\r\\\n", true},
			{"string embedded backslash", ",", []byte(`back\slash`), "\\back\\\\\\\\slash\\\n", true},
			{"bit bare CR", ",", []byte{0x0d}, "\\\\\\r\\\n", true},
			{"bit backslash byte", ",", []byte{0x5c}, "\\\\\\\\\\\\\n", true},
		} {
			t.Run(c.name, func(t *testing.T) {
				ep := newEp(c.field)
				var buf bytes.Buffer
				require.NoError(t, formatOutputString(ep, c.tmp, []byte("\n"), '\\', false, true, &buf))
				require.Equal(t, c.want, buf.String())
				parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
					FieldsTerminatedBy: c.field, FieldsEscapedBy: "\\", FieldsEnclosedBy: "\\",
					LinesTerminatedBy: "\n", Null: []string{`\N`}, UnescapedQuote: true,
				}, bytes.NewReader(buf.Bytes()), csvparser.ReadBlockSize, false)
				require.NoError(t, err)
				row, err := parser.Read(nil)
				require.NoError(t, err)
				require.Len(t, row, 1)
				require.Equal(t, string(c.tmp), row[0].Val)
			})
		}

		t.Run("NULL under bare backslash enclosure", func(t *testing.T) {
			ep := newEp(",")
			var buf bytes.Buffer
			require.NoError(t, formatOutputNull(ep, []byte("\n"), '\\', true, &buf))
			require.Equal(t, "\\\\N\\\n", buf.String())
		})
	})

	t.Run("document the reader failure the simulation prevents", func(t *testing.T) {
		// The reviewer's probe bytes under the loader's real configuration:
		// the emitted field of run r-bytes leaves an unterminated quote.
		parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
			FieldsTerminatedBy: ",", FieldsEscapedBy: "r", FieldsEnclosedBy: "r",
			LinesTerminatedBy: "\n", LinesStartingBy: "p:", Null: []string{`\N`}, UnescapedQuote: true,
		}, bytes.NewReader([]byte("p:r1r,rarrrr\n")), csvparser.ReadBlockSize, false)
		require.NoError(t, err)
		_, err = parser.Read(nil)
		require.ErrorContains(t, err, "unterminated quoted field")
	})

	t.Run("batch writer aborts on a trailing-CR BIT value", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer func() { require.Zero(t, mp.CurrNB()) }()
		bat := batch.NewOffHeapWithSize(2)
		bat.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int32.ToType())
		bat.Vecs[1] = vector.NewOffHeapVecWithType(types.New(types.T_bit, 16, 0))
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(1), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], uint64(0x610D), false, mp)) // a CR
		bat.SetRowCount(1)
		defer bat.Clean(mp)

		ep := selectIntoOutfileConfig(",", 'r', 'r', "p:", "\n")
		ep.ctx = context.Background()
		ch := make(chan *BatchByte, 1)
		constructByte(ep.ctx, &backSession{feSessionImpl: feSessionImpl{pool: mp}}, bat, 0, ch, ep)
		result := <-ch
		require.ErrorContains(t, result.err, "when ENCLOSED BY and ESCAPED BY are the same character")
	})

	t.Run("batch writer keeps a control-free BIT value", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer func() { require.Zero(t, mp.CurrNB()) }()
		bat := batch.NewOffHeapWithSize(2)
		bat.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int32.ToType())
		bat.Vecs[1] = vector.NewOffHeapVecWithType(types.New(types.T_bit, 16, 0))
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(1), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], uint64(0x6162), false, mp)) // ab
		bat.SetRowCount(1)
		defer bat.Clean(mp)

		ep := selectIntoOutfileConfig(",", 'r', 'r', "p:", "\n")
		ep.ctx = context.Background()
		ch := make(chan *BatchByte, 1)
		constructByte(ep.ctx, &backSession{feSessionImpl: feSessionImpl{pool: mp}}, bat, 0, ch, ep)
		result := <-ch
		require.NoError(t, result.err)
		require.Equal(t, "p:r1r,rabr\n", string(result.writeByte))
		parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
			FieldsTerminatedBy: ",", FieldsEscapedBy: "r", FieldsEnclosedBy: "r",
			LinesTerminatedBy: "\n", LinesStartingBy: "p:", Null: []string{`\N`}, UnescapedQuote: true,
		}, bytes.NewReader([]byte("p:r1r,rabr\n")), csvparser.ReadBlockSize, false)
		require.NoError(t, err)
		row, err := parser.Read(nil)
		require.NoError(t, err)
		require.Equal(t, "ab", row[1].Val)
	})

	t.Run("result-set writer aborts on a trailing-CR BIT value", func(t *testing.T) {
		mrs := &MysqlResultSet{}
		id := new(MysqlColumn)
		id.SetName("id")
		id.SetColumnType(defines.MYSQL_TYPE_LONG)
		bit := new(MysqlColumn)
		bit.SetName("b")
		bit.SetColumnType(defines.MYSQL_TYPE_BIT)
		bit.SetLength(16)
		mrs.AddColumn(id)
		mrs.AddColumn(bit)
		mrs.Data = [][]any{{int32(1), uint64(0x610D)}}
		ep := selectIntoOutfileConfig(",", 'r', 'r', "p:", "\n")
		ep.ctx = context.Background()
		ep.mrs = mrs
		require.ErrorContains(t, exportDataFromResultSetToCSVFile(ep), "when ENCLOSED BY and ESCAPED BY are the same character")
	})
}

// TestOutfileInvalidFieldDelimiterRejected pins the deliberate OUTFILE
// compatibility restriction described in docs/developer-guide/outfile-framing.md:
// older writers could emit these separators for non-MO consumers, but now fail
// before opening a file, even when no new OUTFILE clause is specified.
// It also pins the csvparser constructor
// contract (validDelim): CR, LF, double quote, NUL and invalid runes are
// never legal field delimiters in LOAD, so export must reject them up front
// with a statement error instead of writing a file no LOAD can parse (and
// the emission simulator must never be asked to replay such a framing).
func TestOutfileInvalidFieldDelimiterRejected(t *testing.T) {
	const msg = "CSV FIELDS TERMINATED BY must not be CR, LF, a double quote, NUL or an invalid rune"
	t.Run("rejected delimiters", func(t *testing.T) {
		for _, c := range []struct {
			name string
			term string
		}{
			{"double quote", "\""},
			{"carriage return", "\r"},
			{"line feed", "\n"},
			{"NUL", "\x00"},
			{"invalid rune", "\xff"},
		} {
			t.Run(c.name, func(t *testing.T) {
				ep := newOutfileExportConfig(tree.NewFields(c.term, false, 0, 0), nil, "\n")
				require.ErrorContains(t, ep.validateCSVFraming(context.Background()), msg)
				// The old OUTFILE syntax leaves ESCAPED BY absent, not empty.
				ep.userConfig.Fields.EscapedBy = nil
				require.ErrorContains(t, ep.validateCSVFraming(context.Background()), msg)
			})
		}
	})

	t.Run("loadable delimiters still pass", func(t *testing.T) {
		for _, c := range []struct {
			name string
			term string
		}{
			{"comma", ","},
			{"tab", "\t"},
			{"multibyte rune", "€"},
			{"multi-character", "|@"},
		} {
			t.Run(c.name, func(t *testing.T) {
				ep := newOutfileExportConfig(tree.NewFields(c.term, false, 0, 0), nil, "\n")
				require.NoError(t, ep.validateCSVFraming(context.Background()))
			})
		}
	})

	t.Run("an unconstructable replay parser fails closed", func(t *testing.T) {
		// validateCSVFraming keeps such a framing from ever reaching the
		// gate; if one still did, the gate must report mismatch instead of
		// dereferencing a nil parser.
		ep := newOutfileExportConfig(tree.NewFields("\"", false, '#', '#'), nil, "\n")
		require.False(t, ep.csvFieldEmissionParses([]byte("#a#"), ep.csvNormalizedRecordTerm(), true, []byte("a"), false))
	})
}

// Both boundary verdicts belong to the immutable statement, not a row or a
// conversion worker. A failing field boundary must neither poison the final
// column nor get forgotten on the next row; non-NULL output remains allowed.
func TestOutfileNullEmissionCache(t *testing.T) {
	ep := selectIntoOutfileConfig("NN", '"', '#', "", "\n")
	var ordinary bytes.Buffer
	require.NoError(t, formatOutputString(ep, []byte("plain"), []byte("NN"), '"', true, false, &ordinary))
	start := make(chan struct{})
	var workers sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		workers.Go(func() {
			<-start
			for range 20 {
				var field, record bytes.Buffer
				if err := formatOutputNull(ep, []byte("NN"), '"', false, &field); err == nil || field.Len() != 0 {
					errs <- fmt.Errorf("unsafe field boundary accepted: %q, %v", field.String(), err)
					return
				}
				if err := formatOutputNull(ep, []byte("\n"), '"', true, &record); err != nil || record.String() != "\\N\n" {
					errs <- fmt.Errorf("safe record boundary changed: %q, %v", record.String(), err)
					return
				}
			}
		})
	}
	close(start)
	workers.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	require.ErrorContains(t, ep.nullEmission[0].err, "a NULL value cannot round-trip")
	require.NoError(t, ep.nullEmission[1].err)
}

// Exercise both real row writers, not just the NULL predicate. The batch path
// includes the producer's normal duplication/worker cleanup. Source creation
// and file I/O are outside this CPU benchmark.
func BenchmarkOutfileCSVRow(b *testing.B) {
	oldWrite := Write
	b.Cleanup(func() { Write = oldWrite })
	var written int
	Write = func(_ *ExportConfig, data []byte) (int, error) {
		written = len(data)
		return len(data), nil
	}
	for _, escape := range []byte{'\\', '#'} {
		for _, nullHeavy := range []bool{false, true} {
			for _, writer := range []string{"result-set", "batch-128"} {
				b.Run(fmt.Sprintf("escape-%c/null-heavy-%v/%s", escape, nullHeavy, writer), func(b *testing.B) {
					ep := selectIntoOutfileConfig(",", '"', escape, "", "\n")
					ep.ctx = context.Background()
					ep.mrs = &MysqlResultSet{}
					row := []any{"alice", "has,comma", "héllo", "plain", "double\"quote", "", "last", "value"}
					for i := range row {
						col := &MysqlColumn{}
						col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
						ep.mrs.AddColumn(col)
						if nullHeavy && i%2 == 1 {
							row[i] = nil
						}
					}
					ep.mrs.Data = [][]any{row}
					initExportFileParam(ep, ep.mrs)
					emit := func() error { return exportDataFromResultSetToCSVFile(ep) }
					if writer == "batch-128" {
						mp := mpool.MustNewZero()
						bat := batch.NewOffHeapWithSize(len(row))
						b.Cleanup(func() {
							bat.Clean(mp)
							if mp.CurrNB() != 0 {
								b.Fatalf("batch benchmark leaked %d bytes", mp.CurrNB())
							}
						})
						for col, val := range row {
							bat.Vecs[col] = vector.NewOffHeapVecWithType(types.T_varchar.ToType())
							for range 128 {
								var raw []byte
								if val != nil {
									raw = []byte(val.(string))
								}
								if err := vector.AppendBytes(bat.Vecs[col], raw, val == nil, mp); err != nil {
									b.Fatal(err)
								}
							}
						}
						bat.SetRowCount(128)
						ses := &backSession{feSessionImpl: feSessionImpl{pool: mp}}
						ch := make(chan *BatchByte, 1)
						emit = func() error {
							dup, err := bat.DupWithoutAllocationAccount(mp)
							if err != nil {
								return err
							}
							constructByte(ep.ctx, ses, dup, 0, ch, ep)
							result := <-ch
							written = len(result.writeByte)
							return result.err
						}
					}
					if err := emit(); err != nil {
						b.Fatal(err)
					}
					b.SetBytes(int64(written))
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						if err := emit(); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}

// TestOutfileEmissionSimulatorPoolDeterminism verifies that reusing one
// csvparser through Reset (the per-statement simulator pool) gives exactly
// the same accept/reject verdicts as fresh parsers, including after a
// simulator is dropped for failing mid-scan, and that concurrent conversion
// workers sharing the pool do not interfere.
func TestOutfileEmissionSimulatorPoolDeterminism(t *testing.T) {
	t.Run("LOAD defaults are shared", func(t *testing.T) {
		ep := newOutfileExportConfig(nil, nil, "\n")
		cfg := ep.csvEmissionConfig()
		require.Equal(t, tree.DefaultFieldsTerminated, cfg.FieldsTerminatedBy)
		require.Equal(t, tree.DefaultFieldsEnclosedBy, cfg.FieldsEnclosedBy)
		require.Equal(t, tree.DefaultFieldsEscapedBy, cfg.FieldsEscapedBy)
	})
	// Enclosure and escape both '#': exercises the enclosure==escape scan
	// where fresh-parser versus pooled-reuse differences were historically
	// the most surprising.
	fixtures := []struct {
		name     string
		field    string
		want     string
		wantNull bool
		accept   bool
	}{
		{"plain enclosed value", "#ab#", "ab", false, true},
		{"CR triple inside enclosure", "#a##r#", "a\r", false, true},
		{"NULL marker inside enclosure", "#\\N#", "", true, true},
		{"text value fails the NULL check", "#ab#", "", true, false},
		{"NULL marker fails the value check", "#\\N#", "ab", false, false},
		{"unterminated enclosure scans to an error", "#a", "", false, false},
		{"extra record is rejected", "#a#\n#b#", "a", false, false},
	}

	t.Run("sequential reuse is deterministic", func(t *testing.T) {
		ep := newOutfileExportConfig(tree.NewFields(",", false, '#', '#'), nil, "\n")
		term := ep.csvNormalizedRecordTerm()
		for round := 0; round < 50; round++ {
			for _, c := range fixtures {
				for _, last := range []bool{true, false} {
					boundary := term
					if !last {
						boundary = []byte(",")
					}
					require.Equal(t, c.accept,
						ep.csvFieldEmissionParses([]byte(c.field), boundary, last, []byte(c.want), c.wantNull),
						"round %d case %s last=%v", round, c.name, last)
				}
			}
		}
	})

	t.Run("concurrent workers are deterministic", func(t *testing.T) {
		ep := newOutfileExportConfig(tree.NewFields(",", false, '#', '#'), nil, "\n")
		term := ep.csvNormalizedRecordTerm()
		var wg sync.WaitGroup
		errs := make([]string, 0)
		var mu sync.Mutex
		for w := 0; w < 8; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for round := 0; round < 100; round++ {
					c := fixtures[(w+round)%len(fixtures)]
					last := (w+round)%2 == 0
					boundary := term
					if !last {
						boundary = []byte(",")
					}
					if got := ep.csvFieldEmissionParses([]byte(c.field), boundary, last, []byte(c.want), c.wantNull); got != c.accept {
						mu.Lock()
						errs = append(errs, fmt.Sprintf("worker %d round %d case %s: got %v", w, round, c.name, got))
						mu.Unlock()
					}
				}
			}(w)
		}
		wg.Wait()
		require.Empty(t, errs)
	})
}

// TestOutfileNullMarkerTerminatorCollision pins the review finding that a
// field terminator starting with the NULL marker's bytes splits the emitted
// marker into two non-NULL fields on LOAD: the marker is always a literal
// backslash + N, so FIELDS TERMINATED BY 'N' with ESCAPED BY '#' reloads
// the record as ('\', ”) instead of NULL. The marker replay therefore runs
// for every framing, not only for the enclosure-collision class; framings
// the loader's scan survives — the default backslash escape guards \N as
// its own NULL marker — keep exporting the same bytes.
func TestOutfileNullMarkerTerminatorCollision(t *testing.T) {
	const (
		generalMsg   = "a NULL value cannot round-trip with this CSV framing"
		enclosureMsg = "a NULL value cannot round-trip with ENCLOSED BY"
	)

	t.Run("actual field boundary at every NULL position", func(t *testing.T) {
		for _, term := range []string{"NN", "NNN", ","} {
			for _, escape := range []byte{'#', '\\'} {
				for nullCol := 0; nullCol < 3; nullCol++ {
					for _, writer := range []string{"batch", "result-set"} {
						t.Run(fmt.Sprintf("%s/%c/null-%d/%s", term, escape, nullCol, writer), func(t *testing.T) {
							ep := selectIntoOutfileConfig(term, '"', escape, "", "\n")
							ep.ctx = context.Background()
							ep.Symbol = [][]byte{[]byte(term), []byte(term), []byte("\n")}
							ep.ColumnFlag = []bool{false, false, false}
							ep.mrs = &MysqlResultSet{}
							row := []any{int32(12), int32(23), int32(34)}
							row[nullCol] = nil
							for range row {
								column := &MysqlColumn{}
								column.SetColumnType(defines.MYSQL_TYPE_LONG)
								ep.mrs.AddColumn(column)
							}
							var data []byte
							var err error
							if writer == "batch" {
								mp := mpool.MustNewZero()
								defer func() { require.Zero(t, mp.CurrNB()) }()
								bat := batch.NewOffHeapWithSize(3)
								defer bat.Clean(mp)
								for i := range row {
									bat.Vecs[i] = vector.NewOffHeapVecWithType(types.T_int32.ToType())
									require.NoError(t, vector.AppendFixed(bat.Vecs[i], int32(12+11*i), i == nullCol, mp))
								}
								bat.SetRowCount(1)
								ch := make(chan *BatchByte, 1)
								constructByte(ep.ctx, &backSession{feSessionImpl: feSessionImpl{pool: mp}}, bat, 0, ch, ep)
								result := <-ch
								data, err = result.writeByte, result.err
							} else {
								var out bytes.Buffer
								old := Write
								Write = func(_ *ExportConfig, b []byte) (int, error) { return out.Write(b) }
								defer func() { Write = old }()
								ep.mrs.Data = [][]any{row}
								err = exportDataFromResultSetToCSVFile(ep)
								data = out.Bytes()
							}
							if escape == '#' && term != "," && nullCol != 2 {
								require.ErrorContains(t, err, generalMsg)
								require.Empty(t, data, "a corrupt row must not be published")
								return
							}
							require.NoError(t, err)
							parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
								FieldsTerminatedBy: term, FieldsEnclosedBy: `"`, FieldsEscapedBy: string(escape),
								LinesTerminatedBy: "\n", Null: []string{`\N`}, UnescapedQuote: true,
							}, bytes.NewReader(data), csvparser.ReadBlockSize, false)
							require.NoError(t, err)
							got, err := parser.Read(nil)
							require.NoError(t, err)
							require.Len(t, got, 3)
							for i := range row {
								require.Equal(t, i == nullCol, got[i].IsNull, "column %d", i)
								if i != nullCol {
									require.Equal(t, strconv.Itoa(12+11*i), got[i].Val)
								}
							}
							_, err = parser.Read(nil)
							require.ErrorIs(t, err, io.EOF)
						})
					}
				}
			}
		}
	})

	t.Run("simulation predicate on terminator collisions", func(t *testing.T) {
		// Escape '#': the raw marker cannot survive a terminator of N.
		ep := newOutfileExportConfig(tree.NewFields("N", false, '"', '#'), nil, "\n")
		require.False(t, ep.csvFieldEmissionParses([]byte{'\\', 'N'}, ep.csvNormalizedRecordTerm(), true, nil, true))
		// The default backslash escape guards \N as its own NULL marker.
		ep = newOutfileExportConfig(tree.NewFields("N", false, '"', '\\'), nil, "\n")
		require.True(t, ep.csvFieldEmissionParses([]byte{'\\', 'N'}, ep.csvNormalizedRecordTerm(), true, nil, true))
		// A comma terminator never touches the marker.
		ep = newOutfileExportConfig(tree.NewFields(",", false, '"', '#'), nil, "\n")
		require.True(t, ep.csvFieldEmissionParses([]byte{'\\', 'N'}, ep.csvNormalizedRecordTerm(), true, nil, true))
	})

	t.Run("batch writer rejects the terminator-N framing", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer func() { require.Zero(t, mp.CurrNB()) }()
		bat := newIdNameBatch(t, mp, nil)
		defer bat.Clean(mp)
		ep := selectIntoOutfileConfig("N", '"', '#', "", "\n")
		ep.ctx = context.Background()
		ch := make(chan *BatchByte, 1)
		constructByte(ep.ctx, &backSession{feSessionImpl: feSessionImpl{pool: mp}}, bat, 0, ch, ep)
		result := <-ch
		require.ErrorContains(t, result.err, generalMsg)
	})

	t.Run("result-set writer rejects the terminator-N framing", func(t *testing.T) {
		ep := selectIntoOutfileConfig("N", '"', '#', "", "\n")
		ep.ctx = context.Background()
		ep.mrs.Data = [][]any{{int32(1), nil}}
		require.ErrorContains(t, exportDataFromResultSetToCSVFile(ep), generalMsg)
	})

	t.Run("enclosure-collision rejections keep their diagnostic", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer func() { require.Zero(t, mp.CurrNB()) }()
		bat := newIdNameBatch(t, mp, nil)
		defer bat.Clean(mp)
		ep := selectIntoOutfileConfig(",", '\\', '#', "p:", "\n")
		ep.ctx = context.Background()
		ch := make(chan *BatchByte, 1)
		constructByte(ep.ctx, &backSession{feSessionImpl: feSessionImpl{pool: mp}}, bat, 0, ch, ep)
		result := <-ch
		require.ErrorContains(t, result.err, enclosureMsg)
	})

	t.Run("backslash escape keeps the marker and the NULL", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer func() { require.Zero(t, mp.CurrNB()) }()
		bat := newIdNameBatch(t, mp, nil)
		defer bat.Clean(mp)
		ep := selectIntoOutfileConfig("N", '"', '\\', "", "\n")
		ep.ctx = context.Background()
		ch := make(chan *BatchByte, 1)
		constructByte(ep.ctx, &backSession{feSessionImpl: feSessionImpl{pool: mp}}, bat, 0, ch, ep)
		result := <-ch
		require.NoError(t, result.err)
		require.Equal(t, "1N\\N\n", string(result.writeByte))
		parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
			FieldsTerminatedBy: "N", FieldsEscapedBy: `\`, FieldsEnclosedBy: `"`,
			LinesTerminatedBy: "\n", Null: []string{`\N`}, UnescapedQuote: true,
		}, bytes.NewReader(result.writeByte), csvparser.ReadBlockSize, false)
		require.NoError(t, err)
		row, err := parser.Read(nil)
		require.NoError(t, err)
		require.Len(t, row, 2)
		require.Equal(t, "1", row[0].Val)
		require.True(t, row[1].IsNull)
	})

	t.Run("ordinary values under a terminator-N framing stay allowed", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer func() { require.Zero(t, mp.CurrNB()) }()
		bat := newIdNameBatch(t, mp, "ab")
		defer bat.Clean(mp)
		ep := selectIntoOutfileConfig("N", '"', '#', "", "\n")
		ep.ctx = context.Background()
		ch := make(chan *BatchByte, 1)
		constructByte(ep.ctx, &backSession{feSessionImpl: feSessionImpl{pool: mp}}, bat, 0, ch, ep)
		result := <-ch
		require.NoError(t, result.err)
		require.Equal(t, "1N\"ab\"\n", string(result.writeByte))
	})
}

// BenchmarkOutfileEmissionSimulation measures the per-cell cost of the
// LOAD-replay gate. Constructing a csvparser per cell (the pre-pool shape)
// allocates a 320 KiB block buffer and compiles the escape regexp every
// time; the pooled simulator must stay in the nanosecond/low-alloc range so
// the gate never dominates CSV formatting.
func BenchmarkOutfileEmissionSimulation(b *testing.B) {
	ep := newOutfileExportConfig(tree.NewFields(",", false, '#', '#'), nil, "\n")
	term := ep.csvNormalizedRecordTerm()
	field := []byte("#a##r#")
	want := []byte{'a', '\r'}

	b.Run("fresh parser per cell (pre-pool shape)", func(b *testing.B) {
		b.ReportAllocs()
		cfg := &csvparser.CSVConfig{
			FieldsTerminatedBy: ",", FieldsEnclosedBy: "#", FieldsEscapedBy: "#",
			LinesTerminatedBy: "\n", Null: []string{`\N`}, UnescapedQuote: true,
		}
		for i := 0; i < b.N; i++ {
			p, err := csvparser.NewCSVParser(cfg, bytes.NewReader(append(append([]byte{}, field...), term...)), csvparser.ReadBlockSize, false)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := p.Read(nil); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("pooled simulator", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if !ep.csvFieldEmissionParses(field, term, true, want, false) {
				b.Fatal("fixture must round-trip")
			}
		}
	})
}
