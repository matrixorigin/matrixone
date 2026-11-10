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

package external

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestLoadTimeFractionalParsing(t *testing.T) {
	cases := []struct {
		name, text string
		scale      int32
		rounded    types.Time
	}{
		{"endpoint", "838:59:59.9", 0, 3020400000000},
		{"negative", "-00:00:00.9999999", 6, -1000000},
		{"single_rounding", "00:00:00.4999999", 0, 0},
		{"fraction", "00:00:00.9876549", 3, 988000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, load := range []bool{true, false} {
				proc := testutil.NewProcess(t)
				mode := ""
				proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return mode, nil })
				typ := types.T_time.ToTypeWithScale(tc.scale)
				if load {
					typ = types.T_varchar.ToType()
				}
				bat := batch.NewWithSize(1)
				bat.Vecs[0] = vector.NewVec(typ)
				defer bat.Clean(proc.Mp())
				param := &ExternalParam{ExParamConst: ExParamConst{
					Ctx:    context.Background(),
					Extern: &tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.CSV}},
					Cols:   []*plan.ColDef{{Name: "v", Typ: plan.Type{Id: int32(types.T_time), Scale: tc.scale}}},
				}}
				param.Extern.ExternType = int32(plan.ExternType_EXTERNAL_TB)
				if load {
					param.Extern.ExternType = int32(plan.ExternType_LOAD)
				}
				for _, truncate := range []bool{false, true} {
					mode = ""
					if truncate {
						mode = "TIME_TRUNCATE_FRACTIONAL"
					}
					bat.Vecs[0].Reset(typ)
					require.NoError(t, getColData(bat, []csvparser.Field{{Val: tc.text}}, 0, param, proc.Mp(), plan.ExternAttr{ColName: "v"}, proc))
					if load {
						require.Equal(t, tc.text, bat.Vecs[0].GetStringAt(0), "decoding must not round or truncate before assignment")
					} else {
						require.Equal(t, []types.Time{tc.rounded}, vector.MustFixedColWithTypeCheck[types.Time](bat.Vecs[0]))
					}
				}
			}
		})
	}
}

type timeAssignmentWarningSink struct{ codes []uint16 }

func (s *timeAssignmentWarningSink) AppendWarningDiagnostic(code uint16, _ string) {
	s.codes = append(s.codes, code)
}

func TestLoadTimeTextAssignmentRange(t *testing.T) {
	for _, scale := range []int32{0, 3, 6} {
		for _, truncate := range []bool{false, true} {
			t.Run(fmt.Sprintf("fsp%d/truncate%v", scale, truncate), func(t *testing.T) {
				proc := testutil.NewProcess(t)
				mode := ""
				proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return mode, nil })
				sink := &timeAssignmentWarningSink{}
				proc.WarningSink = sink
				colType := plan.Type{Id: int32(types.T_time), Scale: scale}
				param := &ExternalParam{ExParamConst: ExParamConst{Ctx: t.Context(),
					Extern: &tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.CSV}, ExParam: tree.ExParam{ExternType: int32(plan.ExternType_LOAD)}},
					Cols:   []*plan.ColDef{{Name: "v", Typ: colType}},
				}}
				input := batch.NewWithSize(1)
				input.Vecs[0] = vector.NewVec(makeType(&colType, loadTimeTextInput(param, types.T_time)))
				defer input.Clean(proc.Mp())
				target := vector.NewVec(types.T_time.ToTypeWithScale(scale))
				defer target.Free(proc.Mp())
				result := vector.NewFunctionResultWrapper(*target.GetType(), proc.Mp())
				defer result.Free()
				for _, text := range []string{"2562047788:00:00", "-2562047788:00:00"} {
					input.Vecs[0].ResetWithSameType()
					require.NoError(t, getColData(input, []csvparser.Field{{Val: text}}, 0, param, proc.Mp(), plan.ExternAttr{ColName: "v"}, proc))
					require.Equal(t, text, input.Vecs[0].GetStringAt(0))
					for _, policy := range []string{"strict", "permissive", "ignore"} {
						mode = ""
						if policy != "permissive" {
							mode = "STRICT_TRANS_TABLES"
						}
						if truncate {
							mode += ",TIME_TRUNCATE_FRACTIONAL"
						}
						sink.codes = nil
						require.NoError(t, result.PreExtendAndReset(1))
						cast := function.NewAssignCast
						if policy == "ignore" {
							cast = function.NewAssignIgnoreCast
						}
						err := cast([]*vector.Vector{input.Vecs[0], target}, result, proc, 1, nil)
						if policy == "strict" {
							require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "%v", err)
							require.ErrorContains(t, err, text)
							require.Empty(t, sink.codes)
						} else {
							require.NoError(t, err, "policy=%s mode=%q snapshot=%q", policy, mode, proc.GetSessionInfo().SqlMode)
							want := types.Time(3020399000000)
							if text[0] == '-' {
								want = -want
							}
							require.Equal(t, []types.Time{want}, vector.MustFixedColWithTypeCheck[types.Time](result.GetResultVector()))
							require.Equal(t, []uint16{1264}, sink.codes)
						}
					}
				}
				// Staging must retain TIME's existing empty/whitespace/NullMap rules.
				param.Extern.NullMap = map[string][]string{"v": {"NULL_TIME"}}
				for _, field := range []csvparser.Field{{IsNull: true}, {Val: ""}, {Val: "   "}, {Val: "NULL_TIME"}} {
					input.Vecs[0].ResetWithSameType()
					require.NoError(t, getColData(input, []csvparser.Field{field}, 0, param, proc.Mp(), plan.ExternAttr{ColName: "v"}, proc))
					require.True(t, input.Vecs[0].GetNulls().Contains(0))
				}
			})
		}
	}
}

func TestParquetStringTimeAssignmentRange(t *testing.T) {
	rows := []parquet.Row{
		{parquet.ByteArrayValue([]byte("00:00:01.9876549")).Level(0, 1, 0)},
		{parquet.ByteArrayValue([]byte("2562047788:00:00")).Level(0, 1, 0)},
		{parquet.ByteArrayValue([]byte("-2562047788:00:00")).Level(0, 1, 0)},
		{parquet.NullValue().Level(0, 0, 0)},
		{parquet.ByteArrayValue([]byte("2562047788:00:00")).Level(0, 1, 0)},
	}
	for _, dictionary := range []bool{false, true} {
		node := parquet.Optional(parquet.String())
		if dictionary {
			node = parquet.Optional(parquet.Encoded(parquet.String(), &parquet.RLEDictionary))
		}
		f, page := writeColumnAndGetPage(t, node, rows)
		require.Equal(t, dictionary, page.Dictionary() != nil)
		for _, scale := range []int32{0, 3, 6} {
			for _, truncate := range []bool{false, true} {
				t.Run(fmt.Sprintf("dictionary%v/fsp%d/truncate%v", dictionary, scale, truncate), func(t *testing.T) {
					proc := testutil.NewProcess(t)
					proc.SetStmtProfile(&process.StmtProfile{})
					mode := ""
					proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return mode, nil })
					sink := &timeAssignmentWarningSink{}
					proc.WarningSink = sink
					h := &ParquetHandler{timeAssignment: true}
					mapper := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_time), Scale: scale})
					require.NotNil(t, mapper)
					vec := vector.NewVec(types.T_time.ToTypeWithScale(scale))
					defer vec.Free(proc.Mp())
					for _, policy := range []string{"strict", "permissive", "ignore"} {
						mode = ""
						if policy != "permissive" {
							mode = "STRICT_TRANS_TABLES"
						}
						if truncate {
							mode += ",TIME_TRUNCATE_FRACTIONAL"
						}
						proc.GetStmtProfile().SetStatementRuntimeProfile("Load Data", tree.QueryTypeDML, policy == "ignore")
						sink.codes = nil
						vec.ResetWithSameType()
						require.NoError(t, vector.AppendFixed(vec, types.Time(123), false, proc.Mp()))
						before := proc.Mp().CurrNB()
						err := mapper.mapping(page, proc, vec)
						if policy == "strict" {
							require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "%v", err)
							require.ErrorContains(t, err, "2562047788:00:00")
							require.Equal(t, []types.Time{123}, vector.MustFixedColWithTypeCheck[types.Time](vec), "a failed page must leave its destination unchanged")
							require.Equal(t, before, proc.Mp().CurrNB(), "temporary vectors must be freed on conversion failure")
							require.Empty(t, sink.codes)
						} else {
							require.NoError(t, err)
							valid := map[int32]types.Time{0: 2000000, 3: 1988000, 6: 1987655}[scale]
							if truncate {
								valid = map[int32]types.Time{0: 1000000, 3: 1987000, 6: 1987654}[scale]
							}
							require.Equal(t, []types.Time{123, valid, 3020399000000, -3020399000000, 0, 3020399000000}, vector.MustFixedColWithTypeCheck[types.Time](vec))
							require.True(t, vec.GetNulls().Contains(4))
							require.Equal(t, []uint16{1264, 1264, 1264}, sink.codes, "warnings count physical rows, including repeated dictionary values")
						}
					}
				})
			}
		}
		// Foreign reads continue to reject text outside their internal TIME range.
		proc := testutil.NewProcess(t)
		var foreign ParquetHandler
		mapper := foreign.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_time), Scale: 6})
		vec := vector.NewVec(types.T_time.ToTypeWithScale(6))
		defer vec.Free(proc.Mp())
		require.Error(t, mapper.mapping(page, proc, vec))
		require.Zero(t, vec.Length())
	}
}

func TestParquetTimeAssignmentHasNoVarcharCapacity(t *testing.T) {
	// One row just beyond a VARCHAR's capacity distinguishes transport staging
	// from target assignment; no volume fixture or performance assertion. Hours
	// beyond uint64 are malformed native input, unlike the valid int64-duration
	// overflow covered above: retain that distinct conversion diagnostic.
	for _, tc := range []struct {
		name, text string
		want       types.Time
		codes      []uint16
	}{
		{"long_fraction", "00:00:01." + strings.Repeat("0", types.MaxVarcharLen+1), 1000000, nil},
		{"invalid_huge_hours", strings.Repeat("9", types.MaxVarcharLen+1) + ":00:00", 0, []uint16{1265}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, page := writeColumnAndGetPage(t, parquet.String(), []parquet.Row{{parquet.ByteArrayValue([]byte(tc.text))}})
			proc := testutil.NewProcess(t)
			proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return "", nil })
			sink := &timeAssignmentWarningSink{}
			proc.WarningSink = sink
			h := &ParquetHandler{timeAssignment: true}
			mapper := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_time), Scale: 6})
			vec := vector.NewVec(types.T_time.ToTypeWithScale(6))
			defer vec.Free(proc.Mp())
			require.NoError(t, mapper.mapping(page, proc, vec))
			require.Equal(t, []types.Time{tc.want}, vector.MustFixedColWithTypeCheck[types.Time](vec))
			require.Equal(t, tc.codes, sink.codes)
		})
	}
}

func TestParquetLoadTimeFractionalParsing(t *testing.T) {
	f, page := writeColumnAndGetPage(t, parquet.Optional(parquet.String()), []parquet.Row{
		{parquet.ByteArrayValue([]byte("838:59:59.9")).Level(0, 1, 0)},
		{parquet.NullValue().Level(0, 0, 0)},
		{parquet.ByteArrayValue([]byte("00:00:00.4999999")).Level(0, 1, 0)},
	})
	for _, load := range []bool{false, true} {
		proc := testutil.NewProcess(t)
		proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return "TIME_TRUNCATE_FRACTIONAL", nil })
		h := &ParquetHandler{timeAssignment: load}
		mapper := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_time)})
		require.NotNil(t, mapper)
		vec := vector.NewVec(types.T_time.ToType())
		defer vec.Free(proc.Mp())
		require.NoError(t, mapper.mapping(page, proc, vec))
		want := types.Time(3020400000000)
		if load {
			want = 3020399000000
		}
		values := vector.MustFixedColWithTypeCheck[types.Time](vec)
		require.Equal(t, want, values[0])
		require.True(t, vec.GetNulls().Contains(1))
		require.Zero(t, values[2])
	}
}

func TestParquetLoadTimestampTimePreservesPrecision(t *testing.T) {
	f, page := writeColumnAndGetPage(t, parquet.Timestamp(parquet.Microsecond), []parquet.Row{{parquet.Int64Value(1234567)}})
	for _, load := range []bool{false, true} {
		proc := testutil.NewProcess(t)
		proc.GetSessionInfo().TimeZone = time.UTC
		h := &ParquetHandler{timeAssignment: load}
		mapper := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_time)})
		require.NotNil(t, mapper)
		vec := vector.NewVec(types.T_time.ToType())
		defer vec.Free(proc.Mp())
		require.NoError(t, mapper.mapping(page, proc, vec))
		want := types.Time(1000000)
		if load {
			want = 1234567
		}
		require.Equal(t, []types.Time{want}, vector.MustFixedColWithTypeCheck[types.Time](vec))
	}
}
