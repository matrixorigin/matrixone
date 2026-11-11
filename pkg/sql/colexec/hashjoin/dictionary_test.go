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

package hashjoin

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestHashJoinConsumesSelectedProbeRows(t *testing.T) {
	for _, mode := range []plan.Node_JoinType{plan.Node_INNER, plan.Node_SEMI, plan.Node_ANTI} {
		t.Run(mode.String(), func(t *testing.T) {
			typ := types.T_int32.ToType()
			fixture := newTestCase(t, []types.Type{typ}, []colexec.ResultPos{colexec.NewResultPos(0, 0)}, [][]*plan.Expr{{newExpr(0, typ)}, {newExpr(0, typ)}})
			fixture.arg.JoinType, fixture.arg.NonEqCond = mode, nil
			t.Cleanup(func() {
				fixture.arg.Reset(fixture.proc, false, nil)
				fixture.barg.Reset(fixture.proc, false, nil)
				if len(fixture.arg.Children) > 0 {
					fixture.arg.GetChildren(0).Free(fixture.proc, false, nil)
				}
				if len(fixture.barg.Children) > 0 {
					fixture.barg.GetChildren(0).Free(fixture.proc, false, nil)
				}
				fixture.arg.Free(fixture.proc, false, nil)
				fixture.barg.Free(fixture.proc, false, nil)
				fixture.proc.GetMessageBoard().Reset()
				fixture.cancel()
				fixture.proc.Free()
				require.Zero(t, fixture.proc.Mp().CurrNB())
			})
			mp := fixture.proc.Mp()
			probe := batch.NewWithSize(1)
			probe.Vecs[0] = vector.NewVec(typ)
			require.NoError(t, vector.AppendFixedList(probe.Vecs[0], []int32{1, 2, 3, 4, 5, 6}, nil, mp))
			probe.SetRowCount(6)
			selected := make([]*vector.Vector, 1)
			admitted, err := vector.TrySelectDictionaryVectors(probe.Vecs, selected, []int64{5, 1, 5, 0}, 6, mp, nil)
			require.NoError(t, err)
			require.True(t, admitted)
			probe.Vecs[0].Free(mp)
			probe.Vecs = selected
			probe.SetRowCount(4)
			resetChildrenWithBatch(fixture.arg, probe)
			build := batch.NewWithSize(1)
			build.Vecs[0] = vector.NewVec(typ)
			require.NoError(t, vector.AppendFixedList(build.Vecs[0], []int32{2, 6}, nil, mp))
			build.SetRowCount(2)
			resetHashBuildChildrenWithBatch(fixture.barg, build)
			require.NoError(t, fixture.barg.Prepare(fixture.proc))
			require.NoError(t, fixture.arg.Prepare(fixture.proc))
			require.True(t, fixture.arg.AcceptsDictionary(0))
			require.True(t, fixture.arg.PrefersOwnedDictionaryInput(0))
			require.False(t, fixture.arg.PrefersOwnedDictionaryInput(1))
			_, err = vm.Exec(fixture.barg, fixture.proc)
			require.NoError(t, err)
			var got []int32
			for {
				result, err := vm.Exec(fixture.arg, fixture.proc)
				require.NoError(t, err)
				if result.Batch == nil {
					break
				}
				require.False(t, result.Batch.HasDictionary(), "join payload remains owned and flat")
				got = append(got, vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])...)
			}
			want := []int32{6, 2, 6}
			if mode == plan.Node_ANTI {
				want = []int32{1}
			}
			require.Equal(t, want, got)
			require.True(t, probe.Vecs[0].IsDictionary())
		})
	}
}
