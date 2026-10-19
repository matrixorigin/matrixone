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

package preinsert

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func checkReservationArgument() *PreInsert {
	return &PreInsert{
		HasAutoCol: true, TrackAutoIncrementGenerated: true, AutoIncrementGeneratedColumn: 2,
		CheckEligibility: &plan.ColRef{ColPos: 2}, Attrs: []string{"id", "v"},
		TableDef: &plan.TableDef{TblId: 44, IsTemporary: true,
			Cols: []*plan.ColDef{
				{Name: "id", Typ: plan.Type{Id: int32(types.T_int32), AutoIncr: true}},
				{Name: "v", Typ: plan.Type{Id: int32(types.T_int32)}},
			}, Pkey: &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
		},
	}
}

func checkInput(t *testing.T, proc *process.Process, ids []int32, nullRows []uint64, pass []bool) *batch.Batch {
	t.Helper()
	input := batch.NewWithSize(3)
	input.Vecs[0] = testutil.MakeInt32Vector(ids, nullRows, proc.Mp())
	input.Vecs[1] = testutil.MakeInt32Vector(make([]int32, len(ids)), nil, proc.Mp())
	input.Vecs[2] = vector.NewVec(types.T_bool.ToType())
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	for _, p := range pass {
		require.NoError(t, vector.AppendFixed(input.Vecs[2], p, false, proc.Mp()))
	}
	input.SetRowCount(len(ids))
	return input
}

func TestPreInsertDeferredCheckSelection(t *testing.T) {
	for _, tc := range []struct {
		name      string
		ids       []int32
		nulls     []uint64
		pass      []bool
		want      []int32
		generated []bool
	}{
		{"all_rejected", []int32{0, 0}, []uint64{0, 1}, []bool{false, false}, nil, nil},
		{"first_rejected", []int32{0, 0}, []uint64{0, 1}, []bool{false, true}, []int32{0}, []bool{true}},
		{"last_rejected", []int32{0, 0}, []uint64{0, 1}, []bool{true, false}, []int32{0}, []bool{true}},
		{"rejected_explicit", []int32{127, 0}, []uint64{1}, []bool{false, true}, []int32{0}, []bool{true}},
		{"manual_only", []int32{0, 5}, []uint64{0}, []bool{false, true}, []int32{5}, []bool{false}},
		{"mixed", []int32{0, 5}, []uint64{0}, []bool{true, true}, []int32{0, 5}, []bool{true, false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProc(t)
			t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
			input := checkInput(t, proc, tc.ids, tc.nulls, tc.pass)
			// No allocator call is legal, including EOF and Reset/reuse.
			proc.Base.IncrService = mock_frontend.NewMockAutoIncrementService(gomock.NewController(t))
			arg := checkReservationArgument()
			arg.DeferAutoIncrement = true
			t.Cleanup(func() { arg.Free(proc, false, nil) })
			child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input, input})
			arg.AppendChild(child)
			require.NoError(t, arg.Prepare(proc))
			for range 2 {
				result, err := arg.Call(proc)
				require.NoError(t, err)
				if tc.want == nil {
					require.True(t, result.Batch.IsEmpty())
				} else {
					require.Equal(t, tc.want, vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])[:result.Batch.RowCount()])
					require.Equal(t, tc.generated, vector.MustFixedColNoTypeCheck[bool](result.Batch.Vecs[2])[:result.Batch.RowCount()])
					for row, generated := range tc.generated {
						require.Equal(t, generated, result.Batch.Vecs[0].IsNull(uint64(row)))
					}
				}
				require.Equal(t, tc.ids, vector.MustFixedColNoTypeCheck[int32](input.Vecs[0]))
				require.Equal(t, len(tc.ids), input.RowCount())
			}
			_, err := arg.Call(proc)
			require.NoError(t, err)
			require.Zero(t, proc.GetStatementLastInsertID())
			arg.Reset(proc, false, nil)
		})
	}
}
