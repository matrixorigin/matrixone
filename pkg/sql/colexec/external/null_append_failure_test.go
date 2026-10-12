// Copyright 2026 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestExternalNullAppendFailureIsReturned(t *testing.T) {
	for _, tc := range []struct {
		name string
		line []csvparser.Field
	}{
		{"explicit NULL", []csvparser.Field{{Val: "next"}, {IsNull: true}}},
		{"missing trailing field", []csvparser.Field{{Val: "next"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp, err := mpool.NewMPool("external-null-cap", 1<<20, mpool.NoFixed)
			require.NoError(t, err)
			proc := process.NewTopProcess(context.Background(), mp, nil, nil, nil, nil, nil, nil, nil, nil, nil)
			defer proc.Free()
			defer mpool.DeleteMPool(mp)

			bat := batch.NewOffHeap([]string{"a", "b"})
			bat.Vecs[0] = vector.NewOffHeapVecWithType(types.T_varchar.ToType())
			bat.Vecs[1] = vector.NewOffHeapVecWithType(types.T_varchar.ToType())
			defer bat.Clean(mp)
			param := &ExternalParam{ExParamConst: ExParamConst{
				Ctx: context.Background(), ColumnListLen: 2,
				Cols: []*plan.ColDef{
					{Name: "a", Typ: plan.Type{Id: int32(types.T_varchar)}},
					{Name: "b", Typ: plan.Type{Id: int32(types.T_varchar)}},
				},
				Attrs: []plan.ExternAttr{
					{ColName: "a", ColIndex: 0, ColFieldIndex: 0},
					{ColName: "b", ColIndex: 1, ColFieldIndex: 1},
				},
				Extern: &tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.CSV}},
			}}
			row := []csvparser.Field{{Val: "x"}, {Val: "y"}}
			require.NoError(t, getOneRowData(proc, bat, row, 0, param))
			capacity := bat.Vecs[1].Capacity()
			require.Positive(t, capacity)
			require.NoError(t, bat.Vecs[0].PreExtend(capacity+1, mp))
			for i := 1; i < capacity; i++ {
				require.NoError(t, getOneRowData(proc, bat, row, i, param))
			}
			require.Equal(t, capacity, bat.Vecs[1].Length())
			remaining := mp.Cap() - mp.CurrNB() - 1
			require.Positive(t, remaining)
			pressure, err := mp.Alloc(int(remaining), true)
			require.NoError(t, err)
			defer mp.Free(pressure)

			err = getOneRowData(proc, bat, tc.line, capacity, param)
			require.Equal(t, capacity+1, bat.Vecs[0].Length(), "first column must have appended")
			require.Equal(t, capacity, bat.Vecs[1].Length(), "failed append must not publish a value")
			require.Error(t, err, "an unappended NULL must abort the row")
		})
	}
}
