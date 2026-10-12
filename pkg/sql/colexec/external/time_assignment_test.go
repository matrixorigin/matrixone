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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestLoadTimeFractionalParsing(t *testing.T) {
	cases := []struct {
		name, text         string
		scale              int32
		rounded, truncated types.Time
	}{
		{"endpoint", "838:59:59.9", 0, 3020400000000, 3020399000000},
		{"negative", "-00:00:00.9999999", 6, -1000000, -999999},
		{"single_rounding", "00:00:00.4999999", 0, 0, 0},
		{"fraction", "00:00:00.9876549", 3, 988000, 987000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, load := range []bool{true, false} {
				proc := testutil.NewProcess(t)
				mode := ""
				proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return mode, nil })
				typ := types.T_time.ToTypeWithScale(tc.scale)
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
					want := tc.rounded
					if load && truncate {
						want = tc.truncated
					}
					require.Equal(t, []types.Time{want}, vector.MustFixedColWithTypeCheck[types.Time](bat.Vecs[0]))
				}
			}
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
