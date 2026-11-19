// Copyright 2021 - 2026 Matrix Origin
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

// Issue #30653, JSONLINE half. MO exports BIT columns into JSONLINE files as
// canonical nonnegative integer JSON NUMBER tokens (export.go renders the
// stored uint64; a BIT value 1 is written as {"b":1}). Both file consumers of
// that scalar must read back the exact uint64: the directly decoded
// external-SELECT/direct-LOAD value in getColData, and the staged
// parallel-LOAD varchar value that CAST(varchar AS BIT) decodes. Reading the
// token's bytes instead turns 1 into 49 and rejects any token wider than the
// field byte budget ("44"/"255" on BIT(8), UINT64_MAX on BIT(64)).
//
// The guards here pin everything that must NOT move: byte semantics for JSON
// strings, JSON null, raw-field NULLIF sentinel matching (including no
// spurious match after numeric conversion), and out-of-domain token behavior.

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func bitJSONLineType(width int32) types.Type {
	return types.New(types.T_bit, width, 0)
}

// bitJSONLineParam builds a single BIT-column ExternalParam and drives it
// through the real CsvReader/makeBatchRows/getColData machinery, mirroring
// TestJSONLineIntegerAssignmentRounding's parameter shape.
func bitJSONLineParam(externType int32, jsonData string, parallel bool, width int32, nullMap map[string][]string) *ExternalParam {
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Attrs:         []plan.ExternAttr{{ColName: "b", ColIndex: 0, ColFieldIndex: 0}},
			Cols:          []*plan.ColDef{{Name: "b", Typ: plan.Type{Id: int32(types.T_bit), Width: width}}},
			ColumnListLen: 1,
			Ctx:           context.Background(),
			StrictSqlMode: true,
			maxBatchSize:  1 << 20,
			ParallelLoad:  parallel,
			Extern: &tree.ExternParam{
				ExParamConst: tree.ExParamConst{Format: tree.JSONLINE, Tail: &tree.TailParameter{}},
				ExParam:      tree.ExParam{ExternType: externType, JsonData: jsonData, Ctx: context.Background()},
			},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileCnt: 1, Filepath: "/tmp/rows.dat"}, Filter: &FilterParam{}},
	}
	if nullMap != nil {
		param.Extern.NullMap = nullMap
	}
	resolveExternalErrorMode(param)
	return param
}

// bitRows snapshots decoded rows so the batch can be cleaned safely.
type bitRows struct {
	values []uint64
	nulls  []bool
}

func (r *bitRows) Length() int       { return len(r.values) }
func (r *bitRows) IsNull(i int) bool { return r.nulls[i] }
func (r *bitRows) Value(i int) uint64 {
	return r.values[i]
}

// readBitColumn decodes one JSONLINE record through the real reader pipeline
// into a BIT vector and snapshots the result before cleanup.
func readBitColumn(t *testing.T, param *ExternalParam, proc *process.Process, width int32, content string) (*bitRows, error) {
	t.Helper()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(bitJSONLineType(width))
	defer bat.Clean(proc.Mp())
	err := readJSONLineText(t, param, proc, bat, content)
	vec := bat.Vecs[0]
	col := vector.MustFixedColWithTypeCheck[uint64](vec)
	rows := &bitRows{
		values: make([]uint64, vec.Length()),
		nulls:  make([]bool, vec.Length()),
	}
	for i := 0; i < vec.Length(); i++ {
		rows.nulls[i] = vec.IsNull(uint64(i))
		rows.values[i] = col[i]
	}
	return rows, err
}

// TestJSONLineBitNumberRoundtripObject drives the real reader + getColData
// direct decode over JSON NUMBER tokens for the widths the exporter emits.
// Desired-success cases fail on an unfixed tree: 1 decodes as 49, and every
// token whose decimal text exceeds the raw byte budget errors or mutates.
func TestJSONLineBitNumberRoundtripObject(t *testing.T) {
	type bitCase struct {
		width   int32
		lexeme  string
		want    uint64
		wantErr bool
	}
	cases := []bitCase{
		// BIT(8): exact uint64 for every value exported in [0, 255].
		{width: 8, lexeme: "0", want: 0},
		{width: 8, lexeme: "1", want: 1},
		{width: 8, lexeme: "32", want: 32},
		{width: 8, lexeme: "44", want: 44},
		{width: 8, lexeme: "255", want: 255},
		// BIT(1) boundary values.
		{width: 1, lexeme: "0", want: 0},
		{width: 1, lexeme: "1", want: 1},
		// BIT(64): exact uint64 including above float64 precision and the max.
		{width: 64, lexeme: "0", want: 0},
		{width: 64, lexeme: "1", want: 1},
		{width: 64, lexeme: "32", want: 32},
		{width: 64, lexeme: "44", want: 44},
		{width: 64, lexeme: "255", want: 255},
		{width: 64, lexeme: "9007199254740993", want: 9007199254740993},
		{width: 64, lexeme: "18446744073709551615", want: 18446744073709551615},
		// Out-of-domain tokens stay rejected with zero rows.
		{width: 8, lexeme: "256", wantErr: true},
		{width: 1, lexeme: "2", wantErr: true},
	}
	for _, tc := range cases {
		name := fmt.Sprintf("w%d/%s", tc.width, tc.lexeme)
		t.Run(name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			param := bitJSONLineParam(int32(plan.ExternType_EXTERNAL_TB), tree.OBJECT, false, tc.width, nil)
			res, err := readBitColumn(t, param, proc, tc.width, `{"b":`+tc.lexeme+`}`+"\n")
			if tc.wantErr {
				require.Error(t, err)
				require.Equal(t, 0, res.Length())
				return
			}
			require.NoError(t, err)
			require.False(t, res.IsNull(0))
			require.Equal(t, tc.want, res.Value(0))
		})
	}
}

// TestJSONLineBitNumberRoundtripArray covers the jsondata='array' record shape
// sharing the same scalar conversion path.
func TestJSONLineBitNumberRoundtripArray(t *testing.T) {
	cases := []struct {
		width int32
		lex   string
		want  uint64
	}{
		{8, "0", 0},
		{8, "1", 1},
		{8, "44", 44},
		{8, "255", 255},
		{64, "1", 1},
		{64, "18446744073709551615", 18446744073709551615},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("w%d/%s", tc.width, tc.lex), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			param := bitJSONLineParam(int32(plan.ExternType_EXTERNAL_TB), tree.ARRAY, false, tc.width, nil)
			res, err := readBitColumn(t, param, proc, tc.width, `[`+tc.lex+`]`+"\n")
			require.NoError(t, err)
			require.False(t, res.IsNull(0))
			require.Equal(t, tc.want, res.Value(0))
		})
	}
}

// TestJSONLineBitNumberDirectLoad proves the directly decoded LOAD reader
// (non-parallel ExternType_LOAD) shares the value-bearing conversion.
func TestJSONLineBitNumberDirectLoad(t *testing.T) {
	cases := []struct {
		width int32
		lex   string
		want  uint64
	}{
		{8, "1", 1},
		{8, "44", 44},
		{64, "18446744073709551615", 18446744073709551615},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("w%d/%s", tc.width, tc.lex), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			param := bitJSONLineParam(int32(plan.ExternType_LOAD), tree.OBJECT, false, tc.width, nil)
			res, err := readBitColumn(t, param, proc, tc.width, `{"b":`+tc.lex+`}`+"\n")
			require.NoError(t, err)
			require.False(t, res.IsNull(0))
			require.Equal(t, tc.want, res.Value(0))
		})
	}
}

// TestJSONLineBitNumberStagedParallelLoadCast exercises the real composed
// staged path: parallel LOAD stages the field bytes into a varchar column
// (makeType), and the projection casts it with the public cast entry the
// pipeline uses. The staged value must already carry the exact BIT value for
// the byte-semantic cast consumer to decode it losslessly.
func TestJSONLineBitNumberStagedParallelLoadCast(t *testing.T) {
	cases := []struct {
		width    int32
		lex      string
		want     uint64
		wantNull bool
	}{
		{8, "0", 0, false},
		{8, "1", 1, false},
		{8, "32", 32, false},
		{8, "44", 44, false},
		{8, "255", 255, false},
		{64, "18446744073709551615", 18446744073709551615, false},
		{8, "null", 0, true},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("w%d/%s", tc.width, tc.lex), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			param := bitJSONLineParam(int32(plan.ExternType_LOAD), tree.OBJECT, true, tc.width, nil)
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
			defer bat.Clean(proc.Mp())
			require.NoError(t, readJSONLineText(t, param, proc, bat, `{"b":`+tc.lex+`}`+"\n"))
			src := bat.Vecs[0]
			require.Equal(t, 1, src.Length())

			typVec := vector.NewVec(bitJSONLineType(tc.width))
			defer typVec.Free(proc.Mp())
			result := vector.NewFunctionResultWrapper(bitJSONLineType(tc.width), proc.Mp())
			defer result.Free()
			// Mirror FunctionTestCase.Run's prepration of the result vector
			// before evaluating the cast function.
			require.NoError(t, result.PreExtendAndReset(src.Length()))
			require.NoError(t, function.NewCast([]*vector.Vector{src, typVec}, result, proc, src.Length(), nil))
			out := result.GetResultVector()
			require.Equal(t, 1, out.Length())
			if tc.wantNull {
				require.True(t, out.IsNull(0))
				return
			}
			require.False(t, out.IsNull(0))
			require.Equal(t, tc.want, vector.MustFixedColWithTypeCheck[uint64](out)[0])
		})
	}
}

// TestJSONLineBitNumberRawNullIf pins the raw-field NULLIF contract: sentinel
// matching happens on the ORIGINAL JSON scalar text before any numeric value
// conversion. SET b=NULLIF(b,'1') must null a JSON NUMBER 1 both before and
// after the fix, must not lose a numeric 65, and must not let a converted
// value's bytes spuriously match a sentinel ('A') they never matched as text.
func TestJSONLineBitNumberRawNullIf(t *testing.T) {
	cases := []struct {
		name     string
		nullMap  map[string][]string
		lexeme   string
		want     uint64
		wantNull bool
	}{
		{name: "numeric 1 maps to null", nullMap: map[string][]string{"b": {"1"}}, lexeme: "1", wantNull: true},
		{name: "numeric 65 survives own nullif", nullMap: map[string][]string{"b": {"1"}}, lexeme: "65", want: 65},
		{name: "numeric 65 no spurious A sentinel", nullMap: map[string][]string{"b": {"A"}}, lexeme: "65", want: 65},
		{name: "numeric 65 no nullif", lexeme: "65", want: 65},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			param := bitJSONLineParam(int32(plan.ExternType_LOAD), tree.OBJECT, false, 64, tc.nullMap)
			res, err := readBitColumn(t, param, proc, 64, `{"b":`+tc.lexeme+`}`+"\n")
			require.NoError(t, err)
			if tc.wantNull {
				require.True(t, res.IsNull(0))
				return
			}
			require.False(t, res.IsNull(0))
			require.Equal(t, tc.want, res.Value(0))
		})
	}
}

// TestJSONLineBitStringKindControls: JSON strings are byte-semantic BIT input
// on every route. These all pass on an unfixed tree and must keep passing --
// provenance must only mark unquoted JSON NUMBER tokens.
func TestJSONLineBitStringKindControls(t *testing.T) {
	cases := []struct {
		name     string
		lex      string
		width    int32
		want     uint64
		wantNull bool
	}{
		{name: "quoted A is byte 0x41", lex: `"A"`, width: 8, want: 65},
		{name: "quoted 1 is byte 0x31", lex: `"1"`, width: 8, want: 49},
		{name: "quoted space is byte 0x20", lex: `" "`, width: 8, want: 32},
		{name: "quoted 0 is byte 0x30", lex: `"0"`, width: 8, want: 48},
		{name: "empty string stays zero", lex: `""`, width: 8, want: 0},
		{name: "json string digits width 64", lex: `"250"`, width: 64, want: 0x323530},
		{name: "json null stays null", lex: `null`, width: 64, wantNull: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			param := bitJSONLineParam(int32(plan.ExternType_EXTERNAL_TB), tree.OBJECT, false, tc.width, nil)
			res, err := readBitColumn(t, param, proc, tc.width, `{"b":`+tc.lex+`}`+"\n")
			require.NoError(t, err)
			if tc.wantNull {
				require.True(t, res.IsNull(0))
				return
			}
			require.False(t, res.IsNull(0))
			require.Equal(t, tc.want, res.Value(0))
		})
	}
}

// TestJSONLineBitNonCanonicalNumberControls: number tokens that are NOT
// canonical nonnegative uint64 integers keep today's byte conversion values
// and errors. These pass on an unfixed tree and must keep passing.
func TestJSONLineBitNonCanonicalNumberControls(t *testing.T) {
	cases := []struct {
		name    string
		lex     string
		want    uint64
		wantErr bool
	}{
		// ASCII big-endian decode of the raw token text.
		{name: "negative token keeps bytes", lex: `-1`, want: 0x2d31},
		{name: "fraction token keeps bytes", lex: `1.5`, want: 0x312e35},
		{name: "exponent token keeps bytes", lex: `1e3`, want: 0x316533},
		{name: "leading zero token keeps bytes", lex: `0.0`, want: 0x302e30},
		{name: "bigger than uint64 keeps error", lex: `18446744073709551616`, wantErr: true},
		// Non-number scalar tokens stay byte semantics too.
		{name: "json true keeps bytes", lex: `true`, want: 0x74727565},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			param := bitJSONLineParam(int32(plan.ExternType_EXTERNAL_TB), tree.OBJECT, false, 64, nil)
			res, err := readBitColumn(t, param, proc, 64, `{"b":`+tc.lex+`}`+"\n")
			if tc.wantErr {
				require.Error(t, err)
				require.Equal(t, 0, res.Length())
				return
			}
			require.NoError(t, err)
			require.False(t, res.IsNull(0))
			require.Equal(t, tc.want, res.Value(0))
		})
	}
}

// TestJSONLineBitKafkaJSONKeepsBytes proves the file-JSONLINE-only gate: a
// Kafka external read consumes the same JSON records through transJsonObject2Lines
// but must keep the pre-existing byte conversion for BIT, so this repair never
// broadens into Kafka behavior.
func TestJSONLineBitKafkaJSONKeepsBytes(t *testing.T) {
	proc := testutil.NewProcess(t)
	param := bitJSONLineParam(int32(plan.ExternType_EXTERNAL_TB), tree.OBJECT, false, 8, nil)
	// A non-nil KafkaScan is the scan-kind gate the reader consults; a bare
	// scan descriptor is enough to steer the shared scalar path.
	param.KafkaScan = &plan.KafkaScan{}
	res, err := readBitColumn(t, param, proc, 8, `{"b":1}`+"\n")
	require.NoError(t, err)
	require.False(t, res.IsNull(0))
	// Token bytes '1' = 0x31 decode big-endian to 49, byte semantics kept.
	require.Equal(t, uint64(49), res.Value(0))
}
