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

package hashjoin

import (
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestHashJoinMembershipProjection(t *testing.T) {
	for _, joinType := range []plan.Node_JoinType{plan.Node_SEMI, plan.Node_ANTI} {
		t.Run(joinType.String(), func(t *testing.T) {
			typ := types.T_int32.ToType()
			tc := newTestCase(t, []types.Type{typ}, nil,
				[][]*plan.Expr{{newExpr(0, typ)}, {newExpr(0, typ)}})
			tc.arg.JoinType = joinType
			residual := tc.arg.NonEqCond
			tc.arg.LeftTypes = append(tc.arg.LeftTypes, types.T_varchar.ToType())
			tc.arg.ResultCols = []colexec.ResultPos{{Rel: 0, Pos: 0}, {Rel: 0, Pos: 1}}
			inputs := make([]*batch.Batch, 0, 8)
			t.Cleanup(func() {
				tc.arg.Reset(tc.proc, false, nil)
				tc.barg.Reset(tc.proc, false, nil)
				tc.arg.Free(tc.proc, false, nil)
				tc.barg.Free(tc.proc, false, nil)
				for _, input := range inputs {
					input.Clean(tc.proc.Mp())
				}
				tc.proc.Free()
				tc.cancel()
				require.Zero(t, tc.proc.Mp().CurrNB())
			})
			for generation := range 4 {
				tc.arg.NonEqCond = nil
				if generation == 2 {
					// The existing equality residual is semantically redundant,
					// but must retain the scalar residual-evaluation path.
					tc.arg.NonEqCond = residual
				}
				values := make([]int32, colexec.DefaultBatchSize+4)
				if generation == 3 {
					values = values[:8]
				}
				for row := range values {
					values[row] = int32(row%2 + 1)
				}
				values[1] = 3
				buildValues := []int32{1, 2}
				if joinType == plan.Node_ANTI {
					buildValues = []int32{3, 4}
				}
				if generation != 0 {
					buildValues = append(buildValues, buildValues[0], 0)
				}
				probe := makeInt32Batch(tc.proc, values)
				inputs = append(inputs, probe)
				payload := vector.NewVec(types.T_varchar.ToType())
				probe.Vecs = append(probe.Vecs, payload)
				if generation == 3 {
					// A constant value may still have per-logical-row grouping.
					require.NoError(t, vector.SetConstBytes(payload, []byte("7"), len(values), tc.proc.Mp()))
					payload.SetClass(vector.CONSTANT)
				} else {
					for row := range values {
						require.NoError(t, vector.AppendBytes(payload, []byte(strconv.Itoa(row)), row == 3, tc.proc.Mp()))
					}
				}
				payload.GetGrouping().Add(2)
				payload.SetPrepareParamKind(vector.PrepareParamInteger)
				payload.SetIsBinaryString(true)
				require.NoError(t, payload.SetStringSource(types.StringSourceLiteral))
				build := makeInt32Batch(tc.proc, buildValues)
				inputs = append(inputs, build)
				probe.Vecs[0].GetNulls().Add(0)
				if generation != 0 {
					build.Vecs[0].GetNulls().Add(uint64(len(buildValues) - 1))
				}
				resetChildrenWithBatch(tc.arg, probe)
				resetHashBuildChildrenWithBatch(tc.barg, build)
				require.NoError(t, tc.arg.Prepare(tc.proc))
				require.NoError(t, tc.barg.Prepare(tc.proc))
				_, err := vm.Exec(tc.barg, tc.proc)
				require.NoError(t, err)
				next := 0
				if joinType == plan.Node_SEMI {
					next = 2 // NULL and the absent key do not match.
				}
				batches := 0
				for {
					res, err := vm.Exec(tc.arg, tc.proc)
					require.NoError(t, err)
					if res.Batch == nil {
						break
					}
					batches++
					require.LessOrEqual(t, res.Batch.RowCount(), colexec.DefaultBatchSize)
					for row := range res.Batch.RowCount() {
						if joinType == plan.Node_ANTI && next == 1 {
							next++ // Key 3 matches and is excluded by ANTI.
						}
						require.Less(t, next, len(values))
						require.Equal(t, next == 0, res.Batch.Vecs[0].IsNull(uint64(row)))
						if next != 0 {
							require.Equal(t, values[next], vector.GetFixedAtNoTypeCheck[int32](res.Batch.Vecs[0], row))
						}
						output := res.Batch.Vecs[1]
						nullPayload := next == 3 && generation != 3
						require.Equal(t, nullPayload, output.IsNull(uint64(row)))
						require.Equal(t, next == 2, output.GetGrouping().Contains(uint64(row)))
						require.Equal(t, types.StringSourceLiteral, output.GetStringSourceAt(row))
						if !nullPayload {
							expected := strconv.Itoa(next)
							if generation == 3 {
								expected = "7"
							}
							require.Equal(t, expected, string(output.GetBytesAt(row)))
							require.Equal(t, vector.PrepareParamInteger, output.GetPrepareParamKindAt(row))
							require.True(t, output.GetIsBinaryStringAt(row))
						}
						next++
					}
				}
				require.Equal(t, len(values), next)
				expectedBatches := 2
				if generation == 3 {
					expectedBatches = 1
				}
				require.Equal(t, expectedBatches, batches, "resume across the output batch boundary")
				tc.arg.Reset(tc.proc, false, nil)
				tc.barg.Reset(tc.proc, false, nil)
				tc.proc.GetMessageBoard().Reset()
			}
		})
	}
}

func BenchmarkHashJoinMembershipProjection(b *testing.B) {
	for _, joinType := range []plan.Node_JoinType{plan.Node_SEMI, plan.Node_ANTI} {
		b.Run(joinType.String(), func(b *testing.B) {
			typ := types.T_int32.ToType()
			tc := newTestCase(b, []types.Type{typ}, nil,
				[][]*plan.Expr{{newExpr(0, typ)}, {newExpr(0, typ)}})
			tc.arg.JoinType, tc.arg.NonEqCond = joinType, nil
			tc.arg.ResultCols = []colexec.ResultPos{{Rel: 0, Pos: 0}}
			var probe, build *batch.Batch
			b.Cleanup(func() {
				tc.arg.Reset(tc.proc, false, nil)
				tc.barg.Reset(tc.proc, false, nil)
				tc.arg.Free(tc.proc, false, nil)
				tc.barg.Free(tc.proc, false, nil)
				if probe != nil {
					probe.Clean(tc.proc.Mp())
				}
				if build != nil {
					build.Clean(tc.proc.Mp())
				}
				tc.proc.Free()
				tc.cancel()
				require.Zero(b, tc.proc.Mp().CurrNB())
			})
			values := make([]int32, hashmap.UnitLimit)
			for row := range values {
				values[row] = int32(row%2 + 1)
			}
			probe = makeInt32Batch(tc.proc, values)
			build = makeInt32Batch(tc.proc, []int32{1, 1})
			resetHashBuildChildrenWithBatch(tc.barg, build)
			require.NoError(b, tc.arg.Prepare(tc.proc))
			require.NoError(b, tc.barg.Prepare(tc.proc))
			_, err := vm.Exec(tc.barg, tc.proc)
			require.NoError(b, err)
			require.NoError(b, tc.arg.build(tc.arg.OpAnalyzer, tc.proc))
			require.NoError(b, tc.arg.resetResultBat())
			ctr := &tc.arg.ctr
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				ctr.resBat.CleanOnlyData()
				ctr.leftBat = probe
				var result vm.CallResult
				if err := ctr.probe(tc.arg, tc.proc, &result); err != nil {
					b.Fatal(err)
				}
				if result.Batch.RowCount() != hashmap.UnitLimit/2 {
					b.Fatal("incorrect membership cardinality")
				}
			}
		})
	}
}
