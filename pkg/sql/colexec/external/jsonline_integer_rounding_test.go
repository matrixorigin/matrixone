// Copyright 2021 - 2026 Matrix Origin
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

package external

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// TestJSONLineIntegerAssignmentRounding drives the shared CsvReader over real
// JSONLINE records (object and array shapes, JSON numbers and numeric
// strings) and asserts integer assignment uses the shared exact half-away
// decimal rounding on both EXTERNAL_TB SELECT and directly decoded LOAD.
// The reader path is the production one: bytejson.ParseNodeString keeps the
// original number lexeme, jsonLineScalarText renders it as Field text without
// CSV quotes, and getColData reuses parseCSVInt/parseCSVUint.
func TestJSONLineIntegerAssignmentRounding(t *testing.T) {
	type jsonLineIntCase struct {
		name     string
		jsonData string // defaults to tree.OBJECT
		extern   int32  // defaults to ExternType_EXTERNAL_TB
		typ      types.Type
		enum     string // SET Enumvalues; goes through ParseSet untouched
		record   string // one JSONLINE record
		want     int64
		wantU64  uint64 // expected uint64 row that does not fit int64
		wantErr  bool
		wantNull bool
	}
	cases := []jsonLineIntCase{
		// JSON numbers carry the exact original lexeme; assignment must round
		// half away from zero (issue #31923), not truncate via float64.
		{name: "object number 2.5", typ: types.T_int8.ToType(), record: `{"i":2.5}`, want: 3},
		{name: "object number -2.5", typ: types.T_int8.ToType(), record: `{"i":-2.5}`, want: -3},
		{name: "object number 0.5", typ: types.T_int8.ToType(), record: `{"i":0.5}`, want: 1},
		{name: "object number -0.5", typ: types.T_int8.ToType(), record: `{"i":-0.5}`, want: -1},
		// Numeric JSON strings never carry HasStringQuote: they round too.
		{name: "object string 1.7", typ: types.T_int8.ToType(), record: `{"i":"1.7"}`, want: 2},
		{name: "object string 2.5", typ: types.T_int8.ToType(), record: `{"i":"2.5"}`, want: 3},
		// Array shape maps values by position.
		{name: "array number 1.7", typ: types.T_int8.ToType(), jsonData: tree.ARRAY, record: `[1.7]`, want: 2},
		{name: "array string 2.5", typ: types.T_int8.ToType(), jsonData: tree.ARRAY, record: `["2.5"]`, want: 3},
		// Values above float64's exact-integer range must stay exact.
		{name: "bigint number exact above 2^53", typ: types.T_int64.ToType(), record: `{"i":9007199254740993.0}`, want: 9007199254740993},
		{name: "bigint string exact above 2^53", typ: types.T_int64.ToType(), record: `{"i":"9007199254740993.0"}`, want: 9007199254740993},
		// Decimal exponent text is exact-decimal input, like unquoted CSV.
		{name: "exponent not decimal", typ: types.T_int32.ToType(), record: `{"i":1e2}`, want: 100},
		{name: "string exponent rounds half away", typ: types.T_int32.ToType(), record: `{"i":"1.5e0"}`, want: 2},
		{name: "overflowed mantissa scales into range", typ: types.T_int64.ToType(), record: `{"i":1000000000000000000000e-21}`, want: 1},
		// Width is enforced after rounding (8388607.6 rounds up past mediumint).
		{name: "mediumint width checked after rounding", typ: types.New(types.T_int32, 24, 0), record: `{"i":8388607.6}`, wantErr: true},
		// Unsigned keeps the CSV exact helper's -0 control: negative zero
		// accepted, negative fractions still rejected, boundary stays exact.
		{name: "unsigned fractional rounds", typ: types.T_uint8.ToType(), record: `{"i":2.5}`, want: 3},
		{name: "unsigned negative zero stays zero", typ: types.T_uint8.ToType(), record: `{"i":-0.0}`, want: 0},
		{name: "unsigned negative fraction still rejected", typ: types.T_uint8.ToType(), record: `{"i":-1.6}`, wantErr: true},
		{name: "unsigned word boundary exact", typ: types.T_uint64.ToType(), record: `{"i":18446744073709551615.0}`, wantU64: 18446744073709551615},
		// Directly decoded LOAD shares the reader and its assignment.
		{name: "direct load number 1.9", extern: int32(plan.ExternType_LOAD), typ: types.T_int8.ToType(), record: `{"i":1.9}`, want: 2},
		{name: "direct load string 2.5", extern: int32(plan.ExternType_LOAD), typ: types.T_int8.ToType(), record: `{"i":"2.5"}`, want: 3},
		// SET is a logical string type: "1.9" is a member name, not a rounding input.
		{name: "set member parse not rounded", typ: types.T_uint64.ToType(), enum: "1.9,two", record: `{"i":"1.9"}`, want: 1},
		// NULL/empty and fail-fast diagnostics do not change.
		{name: "json null is null", typ: types.T_int8.ToType(), record: `{"i":null}`, wantNull: true},
		{name: "empty string is null", typ: types.T_int8.ToType(), record: `{"i":""}`, wantNull: true},
		{name: "out-of-range fraction still fails fast", typ: types.T_int32.ToType(), record: `{"i":2147483647.5}`, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			typ := tc.typ
			col := &plan.ColDef{Name: "i", Typ: plan.Type{Id: int32(typ.Oid), Width: int32(typ.Width)}}
			if tc.enum != "" {
				col.Typ.Enumvalues = tc.enum
			}
			externType := tc.extern
			if externType == 0 {
				externType = int32(plan.ExternType_EXTERNAL_TB)
			}
			jsonData := tc.jsonData
			if jsonData == "" {
				jsonData = tree.OBJECT
			}
			param := &ExternalParam{
				ExParamConst: ExParamConst{
					Attrs:         []plan.ExternAttr{{ColName: "i", ColIndex: 0, ColFieldIndex: 0}},
					Cols:          []*plan.ColDef{col},
					ColumnListLen: 1,
					Ctx:           context.Background(),
					StrictSqlMode: true,
					maxBatchSize:  1 << 20,
					Extern: &tree.ExternParam{
						ExParamConst: tree.ExParamConst{Format: tree.JSONLINE, Tail: &tree.TailParameter{}},
						ExParam:      tree.ExParam{ExternType: externType, JsonData: jsonData, Ctx: context.Background()},
					},
				},
				ExParam: ExParam{Fileparam: &ExFileparam{FileCnt: 1, Filepath: "/tmp/rows.dat"}, Filter: &FilterParam{}},
			}
			resolveExternalErrorMode(param)
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = vector.NewVec(typ)
			defer bat.Clean(proc.Mp())
			err := readJSONLineText(t, param, proc, bat, tc.record+"\n")
			if tc.wantErr {
				require.Error(t, err)
				require.Equal(t, 0, bat.Vecs[0].Length())
				return
			}
			require.NoError(t, err)
			if tc.wantNull {
				require.Equal(t, 1, bat.Vecs[0].Length())
				require.True(t, bat.Vecs[0].IsNull(0))
				return
			}
			require.Equal(t, 1, bat.Vecs[0].Length())
			switch typ.Oid {
			case types.T_int8:
				require.EqualValues(t, tc.want, vector.MustFixedColWithTypeCheck[int8](bat.Vecs[0])[0])
			case types.T_int16:
				require.EqualValues(t, tc.want, vector.MustFixedColWithTypeCheck[int16](bat.Vecs[0])[0])
			case types.T_int32:
				require.EqualValues(t, tc.want, vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])[0])
			case types.T_int64:
				require.EqualValues(t, tc.want, vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0])[0])
			case types.T_uint8:
				require.EqualValues(t, tc.want, vector.MustFixedColWithTypeCheck[uint8](bat.Vecs[0])[0])
			case types.T_uint16:
				require.EqualValues(t, tc.want, vector.MustFixedColWithTypeCheck[uint16](bat.Vecs[0])[0])
			case types.T_uint32:
				require.EqualValues(t, tc.want, vector.MustFixedColWithTypeCheck[uint32](bat.Vecs[0])[0])
			case types.T_uint64:
				if tc.wantU64 != 0 {
					require.EqualValues(t, tc.wantU64, vector.MustFixedColWithTypeCheck[uint64](bat.Vecs[0])[0])
				} else {
					require.EqualValues(t, tc.want, vector.MustFixedColWithTypeCheck[uint64](bat.Vecs[0])[0])
				}
			default:
				t.Fatalf("unsupported assertion type %v", typ)
			}
		})
	}
}

// TestJSONLineErrorModeTolerantFractionalInteger extends the existing
// error-mode consumer (errorModeParam/readAllText/requireRows) with the
// rounded-fraction contract: a row that does not convert is still replaced by
// an error row, and the NEXT VALID ROW loads with exact rounding. Fail-fast
// when error columns are pruned is covered by TestJSONLineWithoutErrorColumnsStillFails.
func TestJSONLineErrorModeTolerantFractionalInteger(t *testing.T) {
	param, proc, bat := errorModeParam(t, tree.JSONLINE, tree.OBJECT, numTestCols)
	content := `{"a":"notanint","s":"beta"}` + "\n" +
		`{"a":2.5,"s":"gamma"}` + "\n" +
		`{"a":-2.5,"s":"delta"}` + "\n"
	require.NoError(t, readAllText(t, param, proc, bat, content))
	requireRows(t, bat, []wantRow{
		{line: 1, message: "is not int32 type", text: `{"a":"notanint","s":"beta"}`},
		{a: 3, s: "gamma", line: 2},
		{a: -3, s: "delta", line: 3},
	})
}

// readJSONLineText drives the shared CSV/JSONLINE reader machinery over
// in-memory content, like readAllText in error_mode_test.go.
func readJSONLineText(t *testing.T, param *ExternalParam, proc *process.Process, bat *batch.Batch, content string) error {
	t.Helper()
	r := &CsvReader{param: param}
	r.reader = io.NopCloser(strings.NewReader(content))
	parser, err := newCSVParserFromReader(param.Extern, r.reader)
	if err != nil {
		return err
	}
	r.plh = &ParseLineHandler{csvReader: parser}
	for {
		fin, err := r.makeBatchRows(proc, bat)
		if err != nil {
			return err
		}
		if fin {
			return nil
		}
	}
}
