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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestHashJoinUniqueConstantGrouping(t *testing.T) {
	for _, residual := range []bool{true, false} {
		name := "batch"
		if residual {
			name = "scalar-control"
		}
		t.Run(name, func(t *testing.T) {
			typ := types.T_int32.ToType()
			tc := newTestCase(t, []types.Type{typ}, nil,
				[][]*plan.Expr{{newExpr(0, typ)}, {newExpr(0, typ)}})
			tc.arg.JoinType = plan.Node_INNER
			if !residual {
				tc.arg.NonEqCond = nil
			}
			tc.arg.LeftTypes = append(tc.arg.LeftTypes, types.T_varchar.ToType())
			tc.arg.ResultCols = []colexec.ResultPos{{Rel: 0, Pos: 0}, {Rel: 0, Pos: 1}}
			probe := makeInt32Batch(tc.proc, []int32{9, 1, 2, 1})
			payload := vector.NewVec(types.T_varchar.ToType())
			require.NoError(t, vector.SetConstBytes(payload, []byte("v"), 4, tc.proc.Mp()))
			payload.SetClass(vector.CONSTANT)
			payload.GetGrouping().Add(2)
			probe.Vecs = append(probe.Vecs, payload)
			build := makeInt32Batch(tc.proc, []int32{1, 2})
			defer func() {
				tc.arg.Reset(tc.proc, false, nil)
				tc.barg.Reset(tc.proc, false, nil)
				tc.arg.Free(tc.proc, false, nil)
				tc.barg.Free(tc.proc, false, nil)
				probe.Clean(tc.proc.Mp())
				build.Clean(tc.proc.Mp())
				tc.proc.Free()
				tc.cancel()
				require.Zero(t, tc.proc.Mp().CurrNB())
			}()
			resetChildrenWithBatch(tc.arg, probe)
			resetHashBuildChildrenWithBatch(tc.barg, build)
			require.NoError(t, tc.arg.Prepare(tc.proc))
			require.NoError(t, tc.barg.Prepare(tc.proc))
			_, err := vm.Exec(tc.barg, tc.proc)
			require.NoError(t, err)
			result, err := vm.Exec(tc.arg, tc.proc)
			require.NoError(t, err)
			require.NotNil(t, result.Batch)
			require.Equal(t, 3, result.Batch.RowCount())
			require.Equal(t, []int32{1, 2, 1}, vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[0]))
			for row := range 3 {
				require.Equal(t, "v", result.Batch.Vecs[1].GetStringAt(row))
				require.Equal(t, row == 1, result.Batch.Vecs[1].GetGrouping().Contains(uint64(row)), "row %d", row)
			}
		})
	}
}

func TestHashJoinUniqueConstantGroupingSelections(t *testing.T) {
	for _, shape := range []string{"fixed", "bytes", "null"} {
		for _, groupingRow := range []int{0, 2} {
			t.Run(fmt.Sprintf("%s/grouping-%d", shape, groupingRow), func(t *testing.T) {
				proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
				typ := types.T_varchar.ToType()
				if shape == "fixed" {
					typ = types.T_int32.ToType()
				}
				makeSource := func() *batch.Batch {
					bat := batch.NewWithSize(1)
					var v *vector.Vector
					switch shape {
					case "fixed":
						var err error
						v, err = vector.NewConstFixed(typ, int32(7), 5, proc.Mp())
						require.NoError(t, err)
					case "bytes":
						v = vector.NewVec(typ)
						require.NoError(t, vector.SetConstBytes(v, []byte("constant payload"), 5, proc.Mp()))
						v.SetClass(vector.CONSTANT)
					case "null":
						v = vector.NewConstNull(typ, 5, proc.Mp())
					}
					v.GetGrouping().Add(uint64(groupingRow))
					if shape != "fixed" {
						v.SetPrepareParamKind(vector.PrepareParamInteger)
						v.SetIsBinaryString(true)
						require.NoError(t, v.SetStringSource(types.StringSourceLiteral))
					}
					bat.Vecs[0] = v
					bat.SetRowCount(5)
					return bat
				}
				left, right := makeSource(), makeSource()
				arg := &HashJoin{ResultCols: []colexec.ResultPos{{Rel: 0, Pos: 0}, {Rel: 1, Pos: 0}}}
				ctr := &arg.ctr
				ctr.leftBat, ctr.rightBats = left, []*batch.Batch{right}
				ctr.resBat = batch.NewWithSize(2)
				ctr.resBat.Vecs[0], ctr.resBat.Vecs[1] = vector.NewVec(typ), vector.NewVec(typ)
				defer func() {
					left.Clean(proc.Mp())
					right.Clean(proc.Mp())
					arg.Free(proc, false, nil)
					proc.Free()
					require.Zero(t, proc.Mp().CurrNB())
				}()
				ctr.vs = []uint64{4, 1, 3, 2, 3}
				ctr.zvs = []int64{1, 1, 1, 1, 1}
				matched, err := ctr.appendUniqueMatches(arg, proc, 5)
				require.NoError(t, err)
				require.Equal(t, 5, matched)
				buildRows := []int{3, 0, 2, 1, 2}
				for side, v := range ctr.resBat.Vecs {
					for row := range 5 {
						sourceRow := row
						if side == 1 {
							sourceRow = buildRows[row]
						}
						require.Equal(t, sourceRow == groupingRow, v.GetGrouping().Contains(uint64(row)), "side %d row %d", side, row)
						require.Equal(t, shape == "null", v.IsNull(uint64(row)))
						if shape != "fixed" {
							require.Equal(t, types.StringSourceLiteral, v.GetStringSourceAt(row))
						}
						if shape == "null" {
							continue
						}
						if shape == "fixed" {
							require.Equal(t, int32(7), vector.GetFixedAtNoTypeCheck[int32](v, row))
						} else {
							require.Equal(t, vector.PrepareParamInteger, v.GetPrepareParamKindAt(row))
							require.True(t, v.GetIsBinaryStringAt(row))
							require.Equal(t, "constant payload", v.GetStringAt(row))
						}
					}
				}
			})
		}
	}
}
