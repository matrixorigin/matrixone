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

type dictionaryJoinSink struct{ *colexec.MockOperator }

func (*dictionaryJoinSink) AcceptsDictionary(child int) bool { return child == 0 }

func TestHashJoinConsumesSelectedProbeRows(t *testing.T) {
	for _, tc := range []struct {
		name     string
		mode     plan.Node_JoinType
		selected bool
		views    bool
		complete bool
		prefix   bool
	}{
		{"INNER/selected", plan.Node_INNER, true, false, false, false},
		{"INNER/flat", plan.Node_INNER, false, false, false, false},
		{"SEMI/selected", plan.Node_SEMI, true, false, false, false},
		{"ANTI/selected", plan.Node_ANTI, true, false, false, false},
		{"INNER/partial-fallback", plan.Node_INNER, true, true, false, false},
		{"SEMI/partial-fallback", plan.Node_SEMI, true, true, false, false},
		{"ANTI/partial-fallback", plan.Node_ANTI, true, true, false, false},
		{"INNER/identity-selected", plan.Node_INNER, true, true, true, false},
		{"INNER/identity-flat", plan.Node_INNER, false, true, true, false},
		{"SEMI/identity", plan.Node_SEMI, true, true, true, false},
		{"ANTI/identity", plan.Node_ANTI, true, true, true, false},
		{"INNER/matched-prefix", plan.Node_INNER, false, true, false, true},
		{"SEMI/matched-prefix", plan.Node_SEMI, false, true, false, true},
		{"ANTI/matched-prefix", plan.Node_ANTI, false, true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typ := types.T_int32.ToType()
			fixture := newTestCase(t, []types.Type{typ}, []colexec.ResultPos{colexec.NewResultPos(0, 0)}, [][]*plan.Expr{{newExpr(0, typ)}, {newExpr(0, typ)}})
			fixture.arg.JoinType, fixture.arg.NonEqCond = tc.mode, nil
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
			resetChildrenWithBatch(fixture.arg, probe)
			probe.Vecs[0] = vector.NewVec(typ)
			values := []int32{6, 2, 6, 1}
			if tc.prefix {
				values = make([]int32, 257)
				for row := range values {
					values[row] = int32(row + 1)
				}
			}
			if tc.selected {
				values = []int32{1, 2, 3, 4, 5, 6}
			}
			require.NoError(t, vector.AppendFixedList(probe.Vecs[0], values, nil, mp))
			probe.SetRowCount(len(values))
			if tc.selected {
				selected := make([]*vector.Vector, 1)
				admitted, err := vector.TrySelectDictionaryVectors(probe.Vecs, selected, []int64{5, 1, 5, 0}, 6, mp, nil)
				require.NoError(t, err)
				require.True(t, admitted)
				probe.Vecs[0].Free(mp)
				probe.Vecs = selected
				probe.SetRowCount(4)
			}
			build := batch.NewWithSize(1)
			resetHashBuildChildrenWithBatch(fixture.barg, build)
			build.Vecs[0] = vector.NewVec(typ)
			buildValues := []int32{2, 6}
			if tc.prefix {
				buildValues = values[:256]
				if tc.mode == plan.Node_ANTI {
					buildValues = []int32{257}
				}
			}
			if tc.complete {
				buildValues = []int32{1, 2, 6}
				if tc.mode == plan.Node_ANTI {
					buildValues = []int32{99}
				}
			}
			require.NoError(t, vector.AppendFixedList(build.Vecs[0], buildValues, nil, mp))
			build.SetRowCount(len(buildValues))
			require.NoError(t, fixture.barg.Prepare(fixture.proc))
			require.NoError(t, fixture.arg.Prepare(fixture.proc))
			if tc.views {
				sink := &dictionaryJoinSink{colexec.NewMockOperator()}
				sink.AppendChild(fixture.arg)
				vm.ConfigureDictionaryBoundaries(sink)
				require.True(t, fixture.arg.ctr.dictionaryBoundary.AllowsDictionaryOutput())
			}
			require.True(t, fixture.arg.AcceptsDictionary(0))
			require.True(t, fixture.arg.PrefersOwnedDictionaryInput(0))
			require.False(t, fixture.arg.PrefersOwnedDictionaryInput(1))
			_, err := vm.Exec(fixture.barg, fixture.proc)
			require.NoError(t, err)
			var got []int32
			for {
				result, err := vm.Exec(fixture.arg, fixture.proc)
				require.NoError(t, err)
				if result.Batch == nil {
					break
				}
				if result.Batch.IsEmpty() {
					continue
				}
				require.Equal(t, tc.views && tc.complete && tc.selected, result.Batch.HasDictionary())
				if tc.views && tc.complete {
					require.Same(t, probe.Vecs[0], result.Batch.Vecs[0], "identity output must forward the existing header")
				}
				for row := range result.Batch.RowCount() {
					got = append(got, vector.GetFixedAtNoTypeCheck[int32](result.Batch.Vecs[0], row))
				}
			}
			want := []int32{6, 2, 6}
			if tc.mode == plan.Node_ANTI {
				want = []int32{1}
			}
			if tc.complete {
				want = []int32{6, 2, 6, 1}
			}
			if tc.prefix {
				want = values[:256]
			}
			require.Equal(t, want, got)
			require.Equal(t, tc.selected, probe.Vecs[0].IsDictionary())
		})
	}
}
