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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestHashJoinFinalizeProjection(t *testing.T) {
	for _, shape := range []string{"flat", "constant", "const-null"} {
		t.Run(shape, func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			typ := types.T_varchar.ToType()
			arg := &HashJoin{
				LeftTypes: []types.Type{types.T_int64.ToType()}, RightTypes: []types.Type{typ},
				ResultCols: []colexec.ResultPos{{Rel: 0, Pos: 0}, {Rel: 1, Pos: 0}},
			}
			want := vector.NewVec(typ)
			sources := make([]*batch.Batch, 0, 2)
			t.Cleanup(func() {
				arg.Free(proc, false, nil)
				for _, source := range sources {
					source.Clean(proc.Mp())
				}
				want.Free(proc.Mp())
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})
			account := installTestAllocation(t, arg)
			for source := range 2 {
				bat := batch.NewWithSize(1)
				v := vector.NewVec(typ)
				bat.Vecs[0] = v
				sources = append(sources, bat)
				for _, value := range []string{"a non-inline build payload", "unselected", "null", "tail"} {
					require.NoError(t, vector.AppendBytes(v, []byte(value), false, proc.Mp()))
				}
				if shape == "constant" {
					v.ToConst()
					v.SetLength(4)
				} else if shape == "const-null" {
					require.NoError(t, vector.SetConstNull(v, 4, proc.Mp()))
				} else {
					v.SetNull(2)
					values, _ := vector.MustVarlenaRawData(v)
					values[2].SetOffsetLen(1<<30, 65536) // Never read NULL payload.
				}
				v.GetGrouping().Add(2)
				origin := types.StringSourceLiteral
				if source == 1 {
					origin = types.StringSourceExpression
					v.SetPrepareParamKind(vector.PrepareParamInteger)
					v.SetIsBinaryString(true)
				}
				require.NoError(t, v.SetStringSource(origin))
				bat.SetRowCount(4)
			}
			// Resolved build ordinals use DefaultBatchSize stride; only the
			// referenced rows need materialization at this operator boundary.
			for _, selected := range [][]uint64{
				{0, 2, 3, colexec.DefaultBatchSize + 1, colexec.DefaultBatchSize + 3},
				{colexec.DefaultBatchSize}, // Reuse after Reset, singleton gather.
				{},                         // Empty terminal output.
			} {
				want.CleanOnlyData()
				var bm bitmap.Bitmap
				bm.InitWithSize(colexec.DefaultBatchSize + 4)
				for _, row := range selected {
					bm.Add(row)
					require.NoError(t, want.UnionOne(sources[row/colexec.DefaultBatchSize].Vecs[0], int64(row%colexec.DefaultBatchSize), proc.Mp()))
				}
				arg.ctr.state, arg.ctr.rightBats = Finalize, sources
				arg.ctr.rightMatchedIter = bm.Iterator()
				result, err := arg.Call(proc)
				require.NoError(t, err)
				if len(selected) == 0 {
					require.Nil(t, result.Batch)
				} else {
					require.NotNil(t, result.Batch)
					require.Equal(t, len(selected), result.Batch.RowCount())
					got := result.Batch.Vecs[1]
					for row := range want.Length() {
						require.True(t, result.Batch.Vecs[0].IsNull(uint64(row)))
						require.Equal(t, want.IsNull(uint64(row)), got.IsNull(uint64(row)))
						require.Equal(t, want.GetGrouping().Contains(uint64(row)), got.GetGrouping().Contains(uint64(row)))
						require.Equal(t, want.GetPrepareParamKindAt(row), got.GetPrepareParamKindAt(row))
						require.Equal(t, want.GetStringSourceAt(row), got.GetStringSourceAt(row))
						require.Equal(t, want.GetRuntimeStringDomainAt(row), got.GetRuntimeStringDomainAt(row))
						if !want.IsNull(uint64(row)) {
							require.Equal(t, want.GetBytesAt(row), got.GetBytesAt(row))
						}
					}
				}
				result, err = arg.Call(proc)
				require.NoError(t, err)
				require.Nil(t, result.Batch)
				require.Equal(t, vm.ExecStop, result.Status)
				arg.Reset(proc, false, nil)
				require.Zero(t, account.Snapshot().Used)
			}
		})
	}
}

func TestHashJoinFinalizeProjectionAllocation(t *testing.T) {
	for _, reject := range []bool{false, true} {
		name := "dead-area"
		if reject {
			name = "partial-rejection"
		}
		t.Run(name, func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			arg := &HashJoin{
				RightTypes: []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()},
				ResultCols: []colexec.ResultPos{{Rel: 1, Pos: 0}, {Rel: 1, Pos: 1}},
			}
			source := batch.NewWithSize(2)
			for i, typ := range arg.RightTypes {
				source.Vecs[i] = vector.NewVec(typ)
			}
			t.Cleanup(func() {
				source.Clean(proc.Mp())
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})
			registry, err := mpool.NewAllocationAccountRegistry(1, 16)
			require.NoError(t, err)
			limit, count := uint64(64<<10), 2
			if reject {
				limit, count = 4<<10, hashmap.UnitLimit
			}
			account, err := registry.Open(limit)
			require.NoError(t, err)
			t.Cleanup(func() {
				arg.Free(proc, true, nil)
				require.NoError(t, arg.ClearAllocationAccount(account))
				require.Zero(t, account.Seal().Used)
				_, err := registry.Finalize(account)
				require.NoError(t, err)
			})
			require.NoError(t, arg.SetAllocationAccount(account))
			var bm bitmap.Bitmap
			bm.InitWithSize(int64(count))
			for row := range count {
				require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(row), false, proc.Mp()))
				value := "live"
				if row == 0 {
					value = strings.Repeat("x", 128<<10)
				}
				require.NoError(t, vector.AppendBytes(source.Vecs[1], []byte(value), false, proc.Mp()))
				bm.Add(uint64(row))
			}
			source.Vecs[1].SetNull(0)
			source.SetRowCount(count)
			arg.ctr.rightBats = []*batch.Batch{source}
			arg.ctr.rightMatchedIter = bm.Iterator()
			var result vm.CallResult
			err = arg.ctr.finalize(arg, proc, &result)
			if reject {
				require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
				require.Nil(t, result.Batch)
				require.Zero(t, arg.ctr.resBat.RowCount())
				require.Positive(t, account.Snapshot().Used, "first output column was admitted")
			} else {
				require.NoError(t, err)
				require.Equal(t, 2, result.Batch.RowCount())
				require.True(t, result.Batch.Vecs[1].IsNull(0))
				require.Equal(t, "live", result.Batch.Vecs[1].GetStringAt(1))
				require.Empty(t, result.Batch.Vecs[1].GetArea())
			}
			arg.Reset(proc, reject, err)
			require.Zero(t, account.Snapshot().Used)
		})
	}
}
