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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
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
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
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
	ctr.probeTrackBuildMatches = true
	ctr.rightRowsMatched = new(bitmap.Bitmap)
	ctr.rightRowsMatched.InitWithSize(colexec.DefaultBatchSize + 2)
	matched, err := ctr.appendUniqueMatches(arg, proc, 7)
	require.NoError(t, err)
	require.Equal(t, 5, matched)
	require.EqualValues(t, 4, ctr.rightRowsMatched.Count())
	for _, row := range []uint64{0, 1, colexec.DefaultBatchSize, colexec.DefaultBatchSize + 1} {
		require.True(t, ctr.rightRowsMatched.Contains(row))
	}
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

func TestHashJoinUniqueProjectionProbeRange(t *testing.T) {
	for _, shape := range []string{"contiguous", "holes", "constant"} {
		t.Run(shape, func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			arg := &HashJoin{ResultCols: []colexec.ResultPos{{Rel: 0, Pos: 0}}}
			ctr := &arg.ctr
			ctr.leftBat = batch.NewWithSize(1)
			ctr.resBat = batch.NewWithSize(1)
			typ := types.T_varbinary.ToType()
			source := vector.NewVec(typ)
			ctr.leftBat.Vecs[0] = source
			ctr.resBat.Vecs[0] = vector.NewVec(typ)
			want := vector.NewVec(typ)
			t.Cleanup(func() {
				want.Free(proc.Mp())
				ctr.leftBat.Clean(proc.Mp())
				arg.Free(proc, false, nil)
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})
			for _, value := range []string{"outside", "a non-inline probe payload value", "null", "grouped", "tail", "outside"} {
				require.NoError(t, vector.AppendBytes(source, []byte(value), false, proc.Mp()))
			}
			if shape == "constant" {
				source.ToConst()
				source.SetLength(6)
				source.SetPrepareParamKind(vector.PrepareParamFloat)
				source.SetIsBinaryString(true)
				require.NoError(t, source.SetStringSource(types.StringSourceLiteral))
			} else {
				source.SetNull(2)
				physical, _ := vector.MustVarlenaRawData(source)
				physical[2].SetOffsetLen(1<<30, 65536) // NULL payload must not be read.
				require.NoError(t, source.SetPrepareParamKindAtWithMP(1, vector.PrepareParamInteger, proc.Mp()))
				require.NoError(t, source.SetStringSourceAtWithMP(1, types.StringSourceLiteral, proc.Mp()))
				require.NoError(t, source.SetRuntimeStringDomainAtWithMP(4, types.RuntimeStringBinary, proc.Mp()))
			}
			source.GetGrouping().Add(3)
			ctr.leftBat.SetRowCount(6)
			for _, target := range []*vector.Vector{want, ctr.resBat.Vecs[0]} {
				require.NoError(t, vector.AppendBytes(target, []byte("existing-prefix"), false, proc.Mp()))
			}
			ctr.lastIdx, ctr.vsIdx = 1, 1
			ctr.vs, ctr.zvs = []uint64{0, 1, 1, 1, 1}, []int64{0, 1, 1, 1, 1}
			selection := []int64{1, 2, 3, 4}
			if shape == "holes" {
				ctr.vs[3] = 0
				selection = []int64{1, 2, 4}
			}
			require.NoError(t, want.Union(source, selection, proc.Mp()))
			matched, err := ctr.appendUniqueMatches(arg, proc, 4)
			require.NoError(t, err)
			require.Equal(t, len(selection), matched)
			got := ctr.resBat.Vecs[0]
			require.Equal(t, want.Length(), got.Length())
			for row := range want.Length() {
				require.Equal(t, want.IsNull(uint64(row)), got.IsNull(uint64(row)))
				require.Equal(t, want.GetGrouping().Contains(uint64(row)), got.GetGrouping().Contains(uint64(row)))
				require.Equal(t, want.GetPrepareParamKindAt(row), got.GetPrepareParamKindAt(row))
				require.Equal(t, want.GetStringSourceAt(row), got.GetStringSourceAt(row))
				require.Equal(t, want.GetRuntimeStringDomainAt(row), got.GetRuntimeStringDomainAt(row))
				if !want.IsNull(uint64(row)) {
					require.Equal(t, want.GetBytesAt(row), got.GetBytesAt(row))
				}
			}
		})
	}
}

func TestHashJoinUniqueProjectionResumesAndReuses(t *testing.T) {
	typ := types.T_int32.ToType()
	tc := newTestCase(t, []types.Type{typ}, nil,
		[][]*plan.Expr{{newExpr(0, typ)}, {newExpr(0, typ)}})
	tc.arg.JoinType, tc.arg.NonEqCond = plan.Node_INNER, nil
	tc.arg.LeftTypes = append(tc.arg.LeftTypes, types.T_decimal128.ToType())
	tc.arg.RightTypes = append(tc.arg.RightTypes, types.T_decimal128.ToType())
	tc.arg.ResultCols = []colexec.ResultPos{{Rel: 0, Pos: 0}, {Rel: 1, Pos: 0}, {Rel: 0, Pos: 1}, {Rel: 1, Pos: 1}}
	inputs := make([]*batch.Batch, 0, 6)
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
		// Exercise both declared PK and actual-map uniqueness, with a result
		// boundary in the middle of a Find chunk after NULL/nonmatch exclusion.
		tc.arg.HashOnPK, tc.barg.HashOnPK = generation == 0, generation == 0
		// Internal control: the join type alone must not bypass an existing
		// build-match tracking obligation, even for a right-oriented INNER.
		tc.arg.IsRightJoin = generation >= 2
		if generation == 3 {
			tc.arg.JoinType = plan.Node_RIGHT
		}
		values := make([]int32, colexec.DefaultBatchSize+4)
		for row := range values {
			values[row] = int32(row%2 + 1)
		}
		values[1] = 3
		buildValues := []int32{1, 2}
		if generation == 3 {
			buildValues = append(buildValues, 4) // Unmatched build row survives finalization.
		}
		probe, build := makeInt32Batch(tc.proc, values), makeInt32Batch(tc.proc, buildValues)
		probe.Vecs[0].GetNulls().Add(0)
		inputs = append(inputs, probe, build)
		for side, input := range []*batch.Batch{probe, build} {
			payload := vector.NewVec(types.T_decimal128.ToType())
			input.Vecs = append(input.Vecs, payload)
			for row := range input.RowCount() {
				require.NoError(t, vector.AppendFixed(payload,
					types.Decimal128{B0_63: uint64(row), B64_127: uint64(side + 1)}, false, tc.proc.Mp()))
			}
		}
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
				for _, vec := range res.Batch.Vecs[:2] {
					require.Equal(t, values[seen+row+2], vector.GetFixedAtNoTypeCheck[int32](vec, row))
				}
				require.Equal(t, types.Decimal128{B0_63: uint64(seen + row + 2), B64_127: 1},
					vector.GetFixedAtNoTypeCheck[types.Decimal128](res.Batch.Vecs[2], row))
				require.Equal(t, types.Decimal128{B0_63: uint64(values[seen+row+2] - 1), B64_127: 2},
					vector.GetFixedAtNoTypeCheck[types.Decimal128](res.Batch.Vecs[3], row))
			}
			seen += count
		}
		if generation >= 2 {
			require.True(t, tc.arg.ctr.rightRowsMatched.Contains(0))
			require.True(t, tc.arg.ctr.rightRowsMatched.Contains(1))
		}
		if generation == 3 {
			require.False(t, tc.arg.ctr.rightRowsMatched.Contains(2))
			res, err := vm.Exec(tc.arg, tc.proc)
			require.NoError(t, err)
			require.NotNil(t, res.Batch)
			require.Equal(t, 1, res.Batch.RowCount())
			require.True(t, res.Batch.Vecs[0].IsNull(0))
			require.Equal(t, int32(4), vector.GetFixedAtNoTypeCheck[int32](res.Batch.Vecs[1], 0))
		}
		res, err := vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)
		require.Nil(t, res.Batch)
		tc.arg.Reset(tc.proc, false, nil)
		tc.barg.Reset(tc.proc, false, nil)
		tc.proc.GetMessageBoard().Reset()
	}
}

func TestHashJoinUniqueProjectionSkipsDeadProbeArea(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	typ := types.T_varchar.ToType()
	left := batch.NewWithSize(1)
	left.Vecs[0] = vector.NewVec(typ)
	arg := &HashJoin{
		LeftTypes: []types.Type{typ}, ResultCols: []colexec.ResultPos{{Rel: 0, Pos: 0}},
	}
	t.Cleanup(func() {
		left.Clean(proc.Mp())
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	registry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	account, err := registry.Open(64 << 10)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.Zero(t, account.Seal().Used)
		_, err := registry.Finalize(account)
		require.NoError(t, err)
	})
	require.NoError(t, arg.SetAllocationAccount(account))
	t.Cleanup(func() {
		arg.Free(proc, false, nil)
		require.NoError(t, arg.ClearAllocationAccount(account))
	})
	require.NoError(t, arg.resetResultBat())
	require.NoError(t, vector.AppendBytes(left.Vecs[0], make([]byte, 128<<10), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(left.Vecs[0], []byte("live"), false, proc.Mp()))
	left.Vecs[0].SetNull(0)
	left.SetRowCount(2)
	// A whole-vector UnionBatch would copy the now-dead area and exceed the
	// result account; selecting just the live payload must still succeed.
	arg.ctr.leftBat, arg.ctr.vs, arg.ctr.zvs = left, []uint64{1, 1}, []int64{1, 1}
	matched, err := arg.ctr.appendUniqueMatches(arg, proc, 2)
	require.NoError(t, err)
	require.Equal(t, 2, matched)
	result := arg.ctr.resBat.Vecs[0]
	require.True(t, result.IsNull(0))
	require.Equal(t, "live", result.GetStringAt(1))
	require.Empty(t, result.GetArea())
}

func TestHashJoinUniqueProjectionPropagatesAllocationFailure(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
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
	arg.ctr.probeTrackBuildMatches = true
	arg.ctr.rightRowsMatched = new(bitmap.Bitmap)
	arg.ctr.rightRowsMatched.InitWithSize(1)
	matched, err := arg.ctr.appendUniqueMatches(arg, proc, 1)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Zero(t, matched)
	require.Zero(t, arg.ctr.resBat.RowCount())
	require.Zero(t, account.Snapshot().Used)
	require.True(t, arg.ctr.rightRowsMatched.Contains(0), "failed projection is cleaned, never finalized")
	arg.ctr.probeLeftSemi = true
	matched, err = arg.ctr.appendMembershipMatches(arg, proc, 1)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Zero(t, matched)
	require.Zero(t, arg.ctr.resBat.RowCount())
	require.Zero(t, account.Snapshot().Used)
}

func BenchmarkHashJoinUniqueProjection(b *testing.B) {
	for _, tc := range []struct {
		name     string
		typ      types.Type
		nullable bool
	}{
		{"int32", types.T_int32.ToType(), false},
		{"decimal128", types.T_decimal128.ToType(), false},
		{"varchar", types.T_varchar.ToType(), false},
		{"nullable-varchar", types.T_varchar.ToType(), true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkHashJoinUniqueProjection(b, tc.typ, tc.nullable)
		})
	}
}

func benchmarkHashJoinUniqueProjection(b *testing.B, typ types.Type, nullable bool) {
	proc := testutil.NewProcessWithOwnedMPool(b, "", mpool.MustNewZero())
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
		ctr.resBat.Vecs[col] = vector.NewVec(typ)
	}
	for _, bat := range append([]*batch.Batch{ctr.leftBat}, ctr.rightBats...) {
		rows := hashmap.UnitLimit
		if bat == ctr.leftBat {
			rows += 2 // Measure a Find window inside a larger probe batch.
		}
		for col := range 4 {
			bat.Vecs[col] = vector.NewVec(typ)
			for row := range rows {
				isNull := nullable && row%4 == 0
				switch typ.Oid {
				case types.T_int32:
					require.NoError(b, vector.AppendFixed(bat.Vecs[col], int32(row), isNull, proc.Mp()))
				case types.T_decimal128:
					require.NoError(b, vector.AppendFixed(bat.Vecs[col], types.Decimal128{B0_63: uint64(row)}, isNull, proc.Mp()))
				default:
					require.NoError(b, vector.AppendBytes(bat.Vecs[col], []byte("a non-inline projection payload value"), isNull, proc.Mp()))
				}
			}
		}
		bat.SetRowCount(rows)
	}
	ctr.lastIdx = 1
	ctr.vs, ctr.zvs = make([]uint64, hashmap.UnitLimit), make([]int64, hashmap.UnitLimit)
	for _, shape := range []string{"same-source", "alternating-source", "holes"} {
		for row := range hashmap.UnitLimit {
			ctr.vs[row], ctr.zvs[row] = uint64(row+1), 1
			if shape == "alternating-source" {
				ctr.vs[row] += uint64(row % 2 * colexec.DefaultBatchSize)
			} else if shape == "holes" && row%2 == 0 {
				ctr.vs[row] = 0
			}
		}
		for _, count := range []int{0, 1, hashmap.UnitLimit} {
			for _, method := range []string{"scalar", "batch"} {
				b.Run(fmt.Sprintf("%s/rows=%d/%s", shape, count, method), func(b *testing.B) {
					b.ReportAllocs()
					for range b.N {
						ctr.resBat.CleanOnlyData()
						if method == "batch" {
							if _, err := ctr.appendUniqueMatches(arg, proc, count); err != nil {
								b.Fatal(err)
							}
						} else {
							for row, match := range ctr.vs[:count] {
								if ctr.zvs[row] == 0 || match == 0 {
									continue
								}
								idx := int64(match - 1)
								if err := ctr.appendOneMatch(arg, proc, int64(ctr.lastIdx+row), idx/colexec.DefaultBatchSize, idx%colexec.DefaultBatchSize); err != nil {
									b.Fatal(err)
								}
							}
						}
					}
				})
			}
		}
	}
}
