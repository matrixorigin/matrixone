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
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
		require.NoError(t, formatOutputNull(ep, []byte("\n"), 0, &buf))
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
			Symbol:     [][]byte{[]byte{','}, []byte{','}, []byte{'\n'}},
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
			Symbol:     [][]byte{[]byte{','}, []byte{','}, []byte{'\n'}},
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
