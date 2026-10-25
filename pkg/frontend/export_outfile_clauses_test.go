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

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
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
