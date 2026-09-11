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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
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

func TestHashJoinUniqueProjectionPreservesSelectionMetadata(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	left := makeInt32Batch(proc, []int32{0, 1, 2, 3, 4, 5, 6, 7})
	arg := &HashJoin{ResultCols: []colexec.ResultPos{{Rel: 0, Pos: 0}, {Rel: 1, Pos: 0}}}
	ctr := &arg.ctr
	ctr.leftBat = left
	ctr.resBat = batch.NewWithSize(2)
	ctr.resBat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	ctr.resBat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	t.Cleanup(func() {
		left.Clean(proc.Mp())
		for _, bat := range ctr.rightBats {
			bat.Clean(proc.Mp())
		}
		arg.Free(proc, false, nil)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	for source, values := range [][]string{{"a", "b"}, {"c", "d"}} {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		ctr.rightBats = append(ctr.rightBats, bat)
		for row, value := range values {
			require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte(value), source == 0 && row == 1, proc.Mp()))
		}
		origin := types.StringSourceLiteral
		if source == 1 {
			origin = types.StringSourceExpression
			bat.Vecs[0].SetPrepareParamKind(vector.PrepareParamInteger)
			bat.Vecs[0].SetIsBinaryString(true)
		} else {
			bat.Vecs[0].GetGrouping().Add(1)
		}
		require.NoError(t, bat.Vecs[0].SetStringSource(origin))
		bat.SetRowCount(2)
	}
	// The helper's input is resolved global build ordinals, with the existing
	// DefaultBatchSize stride. Only referenced rows need materialization here.
	ctr.lastIdx, ctr.vsIdx = 1, 1
	ctr.vs = []uint64{0, 1, 2, 0, colexec.DefaultBatchSize + 1, colexec.DefaultBatchSize + 2, 1, 1}
	ctr.zvs = []int64{0, 1, 1, 1, 1, 1, 1, 0}
	matched, err := ctr.appendUniqueMatches(arg, proc, 7)
	require.NoError(t, err)
	require.Equal(t, 5, matched)
	require.Equal(t, []int32{1, 2, 4, 5, 6}, vector.MustFixedColWithTypeCheck[int32](ctr.resBat.Vecs[0]))
	payload := ctr.resBat.Vecs[1]
	for row, value := range []string{"a", "", "c", "d", "a"} {
		require.Equal(t, row == 1, payload.IsNull(uint64(row)))
		require.Equal(t, row == 1, payload.GetGrouping().Contains(uint64(row)))
		origin, kind := types.StringSourceLiteral, vector.PrepareParamNone
		binary := row == 2 || row == 3
		if binary {
			origin, kind = types.StringSourceExpression, vector.PrepareParamInteger
		}
		require.Equal(t, origin, payload.GetStringSourceAt(row))
		if row != 1 {
			require.Equal(t, value, string(payload.GetBytesAt(row)))
			require.Equal(t, kind, payload.GetPrepareParamKindAt(row))
			require.Equal(t, binary, payload.GetIsBinaryStringAt(row))
		}
	}
}

func TestHashJoinUniqueProjectionResumesAndReuses(t *testing.T) {
	typ := types.T_int32.ToType()
	tc := newTestCase(t, []bool{false}, []types.Type{typ}, nil,
		[][]*plan.Expr{{newExpr(0, typ)}, {newExpr(0, typ)}})
	tc.arg.JoinType, tc.arg.NonEqCond = plan.Node_INNER, nil
	tc.arg.ResultCols = []colexec.ResultPos{{Rel: 0, Pos: 0}, {Rel: 1, Pos: 0}}
	var inputs []*batch.Batch
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
	for generation := range 3 {
		// Exercise both declared PK and actual-map uniqueness, with a result
		// boundary in the middle of a Find chunk after NULL/nonmatch exclusion.
		tc.arg.HashOnPK, tc.barg.HashOnPK = generation == 0, generation == 0
		// Internal control: the join type alone must not bypass an existing
		// build-match tracking obligation, even for a right-oriented INNER.
		tc.arg.IsRightJoin = generation == 2
		values := make([]int32, colexec.DefaultBatchSize+4)
		for row := range values {
			values[row] = int32(row%2 + 1)
		}
		values[1] = 3
		probe, build := makeInt32Batch(tc.proc, values), makeInt32Batch(tc.proc, []int32{1, 2})
		probe.Vecs[0].GetNulls().Add(0)
		inputs = append(inputs, probe, build)
		resetChildrenWithBatch(tc.arg, probe)
		resetHashBuildChildrenWithBatch(tc.barg, build)
		require.NoError(t, tc.arg.Prepare(tc.proc))
		require.NoError(t, tc.barg.Prepare(tc.proc))
		_, err := vm.Exec(tc.barg, tc.proc)
		require.NoError(t, err)
		seen := 0
		for _, count := range []int{colexec.DefaultBatchSize, 2} {
			res, err := vm.Exec(tc.arg, tc.proc)
			require.NoError(t, err)
			require.True(t, tc.arg.ctr.probeHashOnPK, "declared and actual map uniqueness")
			require.NotNil(t, res.Batch)
			require.Equal(t, count, res.Batch.RowCount())
			for row := range count {
				for _, vec := range res.Batch.Vecs {
					require.Equal(t, values[seen+row+2], vector.GetFixedAtNoTypeCheck[int32](vec, row))
				}
			}
			seen += count
		}
		if generation == 2 {
			require.True(t, tc.arg.ctr.rightRowsMatched.Contains(0))
			require.True(t, tc.arg.ctr.rightRowsMatched.Contains(1))
		}
		res, err := vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)
		require.Nil(t, res.Batch)
		tc.arg.Reset(tc.proc, false, nil)
		tc.barg.Reset(tc.proc, false, nil)
		tc.proc.GetMessageBoard().Reset()
	}
}

func TestHashJoinUniqueProjectionPropagatesAllocationFailure(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	left := makeInt32Batch(proc, []int32{1})
	arg := &HashJoin{
		LeftTypes:  []types.Type{types.T_int32.ToType()},
		ResultCols: []colexec.ResultPos{{Rel: 0, Pos: 0}},
	}
	t.Cleanup(func() {
		arg.Reset(proc, true, nil)
		arg.Free(proc, true, nil)
		left.Clean(proc.Mp())
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	registry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	account, err := registry.Open(1)
	require.NoError(t, err)
	require.NoError(t, arg.SetAllocationAccount(account))
	require.NoError(t, arg.resetResultBat())
	arg.ctr.leftBat, arg.ctr.vs, arg.ctr.zvs = left, []uint64{1}, []int64{1}
	matched, err := arg.ctr.appendUniqueMatches(arg, proc, 1)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Zero(t, matched)
	require.Zero(t, arg.ctr.resBat.RowCount())
	require.Zero(t, account.Snapshot().Used)
}

func BenchmarkHashJoinUniqueProjection(b *testing.B) {
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	arg := &HashJoin{}
	ctr := &arg.ctr
	ctr.leftBat = batch.NewWithSize(4)
	ctr.resBat = batch.NewWithSize(8)
	for range 2 {
		ctr.rightBats = append(ctr.rightBats, batch.NewWithSize(4))
	}
	b.Cleanup(func() {
		ctr.leftBat.Clean(proc.Mp())
		for _, bat := range ctr.rightBats {
			bat.Clean(proc.Mp())
		}
		arg.Free(proc, false, nil)
		proc.Free()
	})
	for col := range 8 {
		arg.ResultCols = append(arg.ResultCols, colexec.ResultPos{Rel: int32(col / 4), Pos: int32(col % 4)})
		ctr.resBat.Vecs[col] = vector.NewVec(types.T_varchar.ToType())
	}
	for _, bat := range append([]*batch.Batch{ctr.leftBat}, ctr.rightBats...) {
		for col := range 4 {
			bat.Vecs[col] = vector.NewVec(types.T_varchar.ToType())
			for range hashmap.UnitLimit {
				require.NoError(b, vector.AppendBytes(bat.Vecs[col], []byte("a non-inline projection payload value"), false, proc.Mp()))
			}
		}
		bat.SetRowCount(hashmap.UnitLimit)
	}
	ctr.vs, ctr.zvs = make([]uint64, hashmap.UnitLimit), make([]int64, hashmap.UnitLimit)
	for _, shape := range []string{"same-source", "alternating-source"} {
		for row := range hashmap.UnitLimit {
			ctr.vs[row], ctr.zvs[row] = uint64(row+1), 1
			if shape == "alternating-source" {
				ctr.vs[row] += uint64(row % 2 * colexec.DefaultBatchSize)
			}
		}
		for _, method := range []string{"scalar", "batch"} {
			b.Run(shape+"/"+method, func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					ctr.resBat.CleanOnlyData()
					if method == "batch" {
						if _, err := ctr.appendUniqueMatches(arg, proc, hashmap.UnitLimit); err != nil {
							b.Fatal(err)
						}
					} else {
						for row, match := range ctr.vs {
							idx := int64(match - 1)
							if err := ctr.appendOneMatch(arg, proc, int64(row), idx/colexec.DefaultBatchSize, idx%colexec.DefaultBatchSize); err != nil {
								b.Fatal(err)
							}
						}
					}
				}
			})
		}
	}
}
