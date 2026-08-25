// Copyright 2026 Matrix Origin
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

package external

import (
	"io"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	sqlkafka "github.com/matrixorigin/matrixone/pkg/sql/kafka"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// column layout shared by the file tests: two declared columns followed by the
// synthetic ones, which is how the planner appends them.
const (
	colA = iota
	colS
	colLine
	colMsg
	colText
	colPath
	numTestCols
)

// errorModeParam builds a scan of `(a int, s varchar)` over one of the two
// text formats. Columns are dropped from the tail to model column pruning:
// `keep` is how many of the six columns the query kept.
func errorModeParam(t *testing.T, format string, jsonData string, keep int) (*ExternalParam, *process.Process, *batch.Batch) {
	t.Helper()
	all := []*plan.ColDef{
		{Name: "a", Typ: plan.Type{Id: int32(types.T_int32)}},
		{Name: "s", Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
		{Name: catalog.ExternalFileLine, ColId: catalog.ExternalFileLineColId, Typ: plan.Type{Id: int32(types.T_int64)}},
		{Name: catalog.ExternalErrorMessage, ColId: catalog.ExternalErrorMessageColId, Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
		{Name: catalog.ExternalErrorText, ColId: catalog.ExternalErrorTextColId, Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
		{Name: catalog.ExternalFilePath, ColId: catalog.ExternalFilePathColId, Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
	}
	cols := all[:keep]

	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)

	attrs := make([]plan.ExternAttr, len(cols))
	names := make([]string, len(cols))
	for i, c := range cols {
		fieldIdx := int32(i)
		if i >= 2 {
			fieldIdx = 0 // synthetic: never read out of the record
		}
		attrs[i] = plan.ExternAttr{ColName: c.Name, ColIndex: int32(i), ColFieldIndex: fieldIdx}
		names[i] = c.Name
	}

	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Attrs:         attrs,
			Cols:          cols,
			ColumnListLen: 2,
			StrictSqlMode: true,
			maxBatchSize:  1 << 20,
			Extern: &tree.ExternParam{
				ExParamConst: tree.ExParamConst{Format: format, Tail: &tree.TailParameter{}},
				ExParam: tree.ExParam{
					ExternType: int32(plan.ExternType_EXTERNAL_TB),
					JsonData:   jsonData,
				},
			},
		},
		ExParam: ExParam{
			Fileparam: &ExFileparam{FileCnt: 1, Filepath: "/tmp/rows.dat"},
			Filter:    &FilterParam{},
		},
	}
	resolveExternalErrorMode(param)

	bat := batch.NewOffHeap(names)
	for i := range cols {
		bat.Vecs[i] = vector.NewOffHeapVecWithType(makeType(&cols[i].Typ, false))
	}
	t.Cleanup(func() { bat.Clean(proc.Mp()) })
	return param, proc, bat
}

// readAllText drives the shared CSV/JSONLINE reader over content.
func readAllText(t *testing.T, param *ExternalParam, proc *process.Process, bat *batch.Batch, content string) error {
	t.Helper()
	r := &CsvReader{param: param}
	r.reader = io.NopCloser(strings.NewReader(content))
	parser, err := newCSVParserFromReader(param.Extern, r.reader)
	require.NoError(t, err)
	r.plh = &ParseLineHandler{csvReader: parser}
	for {
		finished, err := r.makeBatchRows(proc, bat)
		if err != nil {
			return err
		}
		if finished {
			return nil
		}
	}
}

type wantRow struct {
	a       int32
	s       string
	line    int64  // 0 means "expected NULL"
	message string // "" means "expected NULL", i.e. the row parsed
	text    string
}

func requireRows(t *testing.T, bat *batch.Batch, want []wantRow) {
	t.Helper()
	require.Equal(t, len(want), bat.RowCount())
	for i, w := range want {
		if w.message == "" {
			require.False(t, bat.Vecs[colA].IsNull(uint64(i)), "row %d: a", i)
			require.Equal(t, w.a, vector.GetFixedAtWithTypeCheck[int32](bat.Vecs[colA], i), "row %d: a", i)
			require.Equal(t, w.s, string(bat.Vecs[colS].GetBytesAt(i)), "row %d: s", i)
			require.True(t, bat.Vecs[colMsg].IsNull(uint64(i)), "row %d: a parsed row has no error message", i)
			require.True(t, bat.Vecs[colText].IsNull(uint64(i)), "row %d: a parsed row has no error text", i)
		} else {
			require.True(t, bat.Vecs[colA].IsNull(uint64(i)), "row %d: a failed row NULLs every user column", i)
			require.True(t, bat.Vecs[colS].IsNull(uint64(i)), "row %d: a failed row NULLs every user column", i)
			require.Contains(t, string(bat.Vecs[colMsg].GetBytesAt(i)), w.message, "row %d: message", i)
			require.Equal(t, w.text, string(bat.Vecs[colText].GetBytesAt(i)), "row %d: text", i)
		}
		if w.line == 0 {
			require.True(t, bat.Vecs[colLine].IsNull(uint64(i)), "row %d: line", i)
		} else {
			require.Equal(t, w.line, vector.GetFixedAtWithTypeCheck[int64](bat.Vecs[colLine], i), "row %d: line", i)
		}
	}
}

// TestResolveExternalErrorMode: tolerance is switched on by the error columns
// alone. __mo_file_line is position metadata -- asking for it must not make a
// bad record survive, or a query that only wants line numbers would silently
// stop failing.
func TestResolveExternalErrorMode(t *testing.T) {
	for _, tc := range []struct {
		name         string
		keep         int
		wantTolerate bool
		wantWantLine bool
	}{
		{"declared columns only", colLine, false, false},
		{"file line alone does not tolerate", colMsg, false, true},
		{"error message tolerates", colText, true, true},
		{"error text tolerates", numTestCols, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			param, _, _ := errorModeParam(t, tree.CSV, "", tc.keep)
			require.Equal(t, tc.wantTolerate, param.ErrorMode.Tolerate)
			require.Equal(t, tc.wantWantLine, param.ErrorMode.WantLine)
		})
	}
}

// TestResolveExternalErrorModeIgnoresLookalikeColumns: the switch is keyed on
// the reserved column ids, not the names, so a user column that happens to be
// called __mo_error_message (a table created before the name was reserved)
// cannot turn tolerance on.
func TestResolveExternalErrorModeIgnoresLookalikeColumns(t *testing.T) {
	param, _, _ := errorModeParam(t, tree.CSV, "", numTestCols)
	for i := range param.Cols {
		param.Cols[i].ColId = 0
	}
	resolveExternalErrorMode(param)
	require.False(t, param.ErrorMode.Tolerate)
	require.False(t, param.ErrorMode.WantLine)
}

// TestCSVErrorModeTolerates: a record that does not convert is replaced by a
// row that describes the failure, and the records around it are unaffected.
func TestCSVErrorModeTolerates(t *testing.T) {
	param, proc, bat := errorModeParam(t, tree.CSV, "", numTestCols)
	require.NoError(t, readAllText(t, param, proc, bat, "1,alpha\nnotanint,beta\n3,gamma\n"))
	requireRows(t, bat, []wantRow{
		{a: 1, s: "alpha", line: 1},
		{line: 2, message: "is not int32 type", text: "notanint,beta"},
		{a: 3, s: "gamma", line: 3},
	})
	require.Equal(t, "/tmp/rows.dat", string(bat.Vecs[colPath].GetBytesAt(1)),
		"__mo_filepath is set on a failed row too")
}

// TestCSVWithoutErrorColumnsStillFails: with the error columns pruned the scan
// must behave exactly as it did before error mode existed.
func TestCSVWithoutErrorColumnsStillFails(t *testing.T) {
	param, proc, bat := errorModeParam(t, tree.CSV, "", colMsg) // through __mo_file_line
	err := readAllText(t, param, proc, bat, "1,alpha\nnotanint,beta\n3,gamma\n")
	require.Error(t, err)
	require.Contains(t, err.Error(), "is not int32 type")
}

// TestCSVErrorModeReportsRecordStartLine: a quoted field spanning lines makes
// the record's line differ from the record's ordinal.
func TestCSVErrorModeReportsRecordStartLine(t *testing.T) {
	param, proc, bat := errorModeParam(t, tree.CSV, "", numTestCols)
	require.NoError(t, readAllText(t, param, proc, bat, "1,\"al\npha\"\nnotanint,beta\n"))
	require.Equal(t, 2, bat.RowCount())
	require.Equal(t, int64(1), vector.GetFixedAtWithTypeCheck[int64](bat.Vecs[colLine], 0))
	require.Equal(t, int64(3), vector.GetFixedAtWithTypeCheck[int64](bat.Vecs[colLine], 1),
		"the record after a 2-line record starts on line 3")
}

// TestJSONLineErrorModeTolerates covers the three ways a JSONLINE record
// fails: a value that does not convert, a line that is not JSON at all, and a
// line whose object is never completed by the end of the file.
func TestJSONLineErrorModeTolerates(t *testing.T) {
	t.Run("bad value and non-json line", func(t *testing.T) {
		param, proc, bat := errorModeParam(t, tree.JSONLINE, tree.OBJECT, numTestCols)
		content := `{"a":1,"s":"alpha"}` + "\n" +
			`{"a":"notanint","s":"beta"}` + "\n" +
			"this is not json\n" +
			`{"a":3,"s":"gamma"}` + "\n"
		require.NoError(t, readAllText(t, param, proc, bat, content))
		requireRows(t, bat, []wantRow{
			{a: 1, s: "alpha", line: 1},
			{line: 2, message: "is not int32 type", text: `{"a":"notanint","s":"beta"}`},
			{line: 3, message: "not a well-formed json object", text: "this is not json"},
			{a: 3, s: "gamma", line: 4},
		})
	})

	// A truncated object is reported as that line's own failure and the scan
	// resumes at the next line. Holding it over to be completed later would
	// append every following line to it and report one failure at EOF, losing
	// the good records in between.
	t.Run("truncated object does not swallow the lines after it", func(t *testing.T) {
		param, proc, bat := errorModeParam(t, tree.JSONLINE, tree.OBJECT, numTestCols)
		require.NoError(t, readAllText(t, param, proc, bat,
			`{"a":1,"s":"alpha"}`+"\n"+`{"a":2`+"\n"+`{"a":3,"s":"gamma"}`+"\n"))
		requireRows(t, bat, []wantRow{
			{a: 1, s: "alpha", line: 1},
			{line: 2, message: "incomplete json record", text: `{"a":2`},
			{a: 3, s: "gamma", line: 3},
		})
	})

	t.Run("trailing garbage does not corrupt the next line", func(t *testing.T) {
		param, proc, bat := errorModeParam(t, tree.JSONLINE, tree.OBJECT, numTestCols)
		content := `{"a":1,"s":"alpha"}x` + "\n" + `{"a":2,"s":"beta"}` + "\n"
		require.NoError(t, readAllText(t, param, proc, bat, content))
		requireRows(t, bat, []wantRow{
			{line: 1, message: "not a well-formed json object", text: `{"a":1,"s":"alpha"}x`},
			{a: 2, s: "beta", line: 2},
		})
	})

	t.Run("blank lines still count", func(t *testing.T) {
		param, proc, bat := errorModeParam(t, tree.JSONLINE, tree.OBJECT, numTestCols)
		content := `{"a":1,"s":"alpha"}` + "\n\n" + `{"a":2,"s":"beta"}` + "\n"
		require.NoError(t, readAllText(t, param, proc, bat, content))
		requireRows(t, bat, []wantRow{
			{a: 1, s: "alpha", line: 1},
			{a: 2, s: "beta", line: 3},
		})
	})
}

// TestJSONLineWithoutErrorColumnsStillFails: pruned error columns restore the
// pre-existing behaviour for JSONLINE too.
func TestJSONLineWithoutErrorColumnsStillFails(t *testing.T) {
	param, proc, bat := errorModeParam(t, tree.JSONLINE, tree.OBJECT, colMsg)
	err := readAllText(t, param, proc, bat,
		`{"a":1,"s":"alpha"}`+"\nthis is not json\n")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a well-formed json object")
}

// kafkaErrorModeParam declares `(a int, s varchar)` plus __mo_message_id, the
// three error-mode columns and __mo_message_value.
func kafkaErrorModeParam(t *testing.T, ks *plan.KafkaScan) (*ExternalParam, *process.Process, *batch.Batch) {
	t.Helper()
	proc := testutil.NewProc(t)
	proc.Session = &fakeKafkaSession{}
	cols := []*plan.ColDef{
		{Name: "a", Typ: plan.Type{Id: int32(types.T_int32)}},
		{Name: "s", Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
		{Name: catalog.ExternalFileLine, ColId: catalog.ExternalFileLineColId, Typ: plan.Type{Id: int32(types.T_int64)}},
		{Name: catalog.ExternalErrorMessage, ColId: catalog.ExternalErrorMessageColId, Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
		{Name: catalog.ExternalErrorText, ColId: catalog.ExternalErrorTextColId, Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
		{Name: catalog.KafkaMessageID, ColId: catalog.KafkaMessageIDColId, Typ: plan.Type{Id: int32(types.T_int64)}},
	}
	attrs := make([]plan.ExternAttr, len(cols))
	names := make([]string, len(cols))
	for i, c := range cols {
		fieldIdx := int32(i)
		if i >= 2 {
			fieldIdx = 0
		}
		attrs[i] = plan.ExternAttr{ColName: c.Name, ColIndex: int32(i), ColFieldIndex: fieldIdx}
		names[i] = c.Name
	}
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Attrs:         attrs,
			Cols:          cols,
			ColumnListLen: 2,
			Extern:        KafkaExternParam(ks),
			KafkaScan:     ks,
			StrictSqlMode: true,
			maxBatchSize:  1 << 20,
		},
		ExParam: ExParam{
			Fileparam: &ExFileparam{FileCnt: 1},
			Filter:    &FilterParam{},
		},
	}
	resolveExternalErrorMode(param)
	bat := batch.NewOffHeap(names)
	for i := range cols {
		bat.Vecs[i] = vector.NewOffHeapVecWithType(makeType(&cols[i].Typ, false))
	}
	t.Cleanup(func() { bat.Clean(proc.Mp()) })
	return param, proc, bat
}

// TestKafkaErrorModeTolerates: a message whose value does not parse is
// reported instead of failing the scan, and the row still identifies the
// message it came from. A Kafka record has no line in a file, so
// __mo_file_line is NULL and __mo_message_id is what locates the failure.
func TestKafkaErrorModeTolerates(t *testing.T) {
	const kafkaMsgID = 5
	t.Run("csv", func(t *testing.T) {
		addr := startKafka(t, "t_em_csv", [][2]string{
			{"", "1,alpha"}, {"", "notanint,beta"}, {"", "1,a,extra"}, {"", "3,gamma"},
		})
		param, proc, bat := kafkaErrorModeParam(t, kafkaScan(addr, "t_em_csv", sqlkafka.FormatCSV))
		readAllKafka(t, param, proc, bat)
		require.Equal(t, 4, bat.RowCount())

		// offsets are preserved across the failed messages
		require.Equal(t, []int64{0, 1, 2, 3},
			vector.MustFixedColWithTypeCheck[int64](bat.Vecs[kafkaMsgID])[:4])
		for i := 0; i < 4; i++ {
			require.True(t, bat.Vecs[colLine].IsNull(uint64(i)),
				"row %d: a kafka record has no file line", i)
		}
		// row 1: the value does not convert; row 2: wrong field count
		require.Contains(t, string(bat.Vecs[colMsg].GetBytesAt(1)), "is not int32 type")
		require.Equal(t, "notanint,beta", string(bat.Vecs[colText].GetBytesAt(1)))
		require.Contains(t, string(bat.Vecs[colMsg].GetBytesAt(2)), "is not equal to input columns")
		require.Equal(t, "1,a,extra", string(bat.Vecs[colText].GetBytesAt(2)))
		for _, good := range []int{0, 3} {
			require.True(t, bat.Vecs[colMsg].IsNull(uint64(good)), "row %d parsed", good)
		}
		require.Equal(t, int32(3), vector.MustFixedColWithTypeCheck[int32](bat.Vecs[colA])[3])
	})

	t.Run("jsonl", func(t *testing.T) {
		addr := startKafka(t, "t_em_json", [][2]string{
			{"", `{"a":1,"s":"alpha"}`}, {"", "this is not json"}, {"", `{"a":3,"s":"gamma"}`},
		})
		param, proc, bat := kafkaErrorModeParam(t, kafkaScan(addr, "t_em_json", sqlkafka.FormatJSONL))
		readAllKafka(t, param, proc, bat)
		require.Equal(t, 3, bat.RowCount())
		require.Equal(t, []int64{0, 1, 2},
			vector.MustFixedColWithTypeCheck[int64](bat.Vecs[kafkaMsgID])[:3])
		require.True(t, bat.Vecs[colMsg].IsNull(0))
		require.Contains(t, string(bat.Vecs[colMsg].GetBytesAt(1)), "not a well-formed json object")
		require.Equal(t, "this is not json", string(bat.Vecs[colText].GetBytesAt(1)))
		require.True(t, bat.Vecs[colMsg].IsNull(2))
		require.Equal(t, int32(3), vector.MustFixedColWithTypeCheck[int32](bat.Vecs[colA])[2],
			"a bad message does not disturb the message after it")
	})
}
