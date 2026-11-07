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

package preinsert

import (
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

// TestPreInsertAliasSlotOwnershipTransition pins the ownership transfer of
// the reusable output buffer when a vector that one batch delivered to an
// ordinary (borrowed) slot comes back shared by two destinations of a later
// batch. The aliased slot flips to owned and must then receive fresh
// independent storage - reusing the previously borrowed buffer entry would
// clean the live child vector before its length is read, leaving every
// destination it feeds with zero-length data while the batch keeps its row
// count, and the pipeline teardown would later release a vector the child
// still owns. The regression drives the multi-batch sequence end to end:
// distinct-to-shared vectors, a shared batch that reuses the prior child
// vector, a later still-shared batch over the transitioned slot, exact
// output lengths, per-slot values and cleanup ownership.
func TestPreInsertAliasSlotOwnershipTransition(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()

	buildVec := func(vals []int64) *vector.Vector {
		v := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(v, vals, nil, proc.Mp()))
		return v
	}

	// batch 1: distinct vectors; every output slot borrows its child vector.
	aVec := buildVec([]int64{10, 7})
	bVec := buildVec([]int64{4, 5})
	bat1 := batch.New([]string{"a", "b"})
	bat1.Vecs = []*vector.Vector{aVec, bVec}
	bat1.SetRowCount(2)

	// batch 2: the prior child vector comes back shared by both
	// destinations, so slot 1 - borrowed by batch 1 - becomes owned now.
	bat2 := batch.New([]string{"a", "b"})
	bat2.Vecs = []*vector.Vector{bVec, bVec}
	bat2.SetRowCount(2)

	// batch 3: a fresh shared vector keeps the transitioned slot working
	// through the regular owned-reuse path.
	cVec := buildVec([]int64{8, 9})
	bat3 := batch.New([]string{"a", "b"})
	bat3.Vecs = []*vector.Vector{cVec, cVec}
	bat3.SetRowCount(2)

	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			ignoreFillNullableCol("a", types.T_int64),
			ignoreFillNullableCol("b", types.T_int64),
		},
		Pkey: &plan.PrimaryKeyDef{},
	}
	arg := &PreInsert{
		ctr:      container{canFreeVecIdx: make(map[int]bool)},
		TableDef: tableDef,
		Attrs:    []string{"a", "b"},
	}
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat1, bat2, bat3})
	arg.Children = nil
	arg.AppendChild(op)
	require.NoError(t, arg.Prepare(proc))

	// batch 1: both slots borrow, values pass through unchanged.
	_, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	out1 := arg.ctr.buf
	require.Equal(t, 2, out1.Vecs[0].Length())
	require.Equal(t, 2, out1.Vecs[1].Length())
	require.Equal(t, int64(10), vector.GetFixedAtNoTypeCheck[int64](out1.Vecs[0], 0))
	require.Equal(t, int64(7), vector.GetFixedAtNoTypeCheck[int64](out1.Vecs[0], 1))
	require.Equal(t, int64(4), vector.GetFixedAtNoTypeCheck[int64](out1.Vecs[1], 0))
	require.Equal(t, int64(5), vector.GetFixedAtNoTypeCheck[int64](out1.Vecs[1], 1))

	// batch 2: the aliased slot must hold independent storage, never the
	// child vector the first batch lent into it, and the child vector
	// itself must survive the transition untouched.
	_, err = vm.Exec(arg, proc)
	require.NoError(t, err)
	out2 := arg.ctr.buf
	require.Equal(t, 2, bVec.Length(),
		"the reused child vector survives the borrowed-to-owned transition untouched")
	require.Equal(t, 2, out2.Vecs[0].Length(),
		"the shared-borrow destination keeps the batch's row count")
	require.Equal(t, 2, out2.Vecs[1].Length(),
		"the transitioned destination keeps the batch's row count")
	require.Equal(t, 2, out2.RowCount(),
		"the batch keeps its row count while every vector keeps its length")
	require.NotSame(t, out2.Vecs[0], out2.Vecs[1],
		"the two destinations never share one physical output vector")
	require.NotSame(t, out2.Vecs[1], bVec,
		"a slot that only now became owned receives fresh storage, not the borrowed child vector")
	require.Equal(t, int64(4), vector.GetFixedAtNoTypeCheck[int64](out2.Vecs[0], 0))
	require.Equal(t, int64(5), vector.GetFixedAtNoTypeCheck[int64](out2.Vecs[0], 1))
	require.Equal(t, int64(4), vector.GetFixedAtNoTypeCheck[int64](out2.Vecs[1], 0))
	require.Equal(t, int64(5), vector.GetFixedAtNoTypeCheck[int64](out2.Vecs[1], 1))
	require.Equal(t, int64(4), vector.GetFixedAtNoTypeCheck[int64](bVec, 0),
		"the child vector keeps its own data through the transition")
	require.Equal(t, int64(5), vector.GetFixedAtNoTypeCheck[int64](bVec, 1))

	// batch 3: the transitioned slot keeps serving later shared batches.
	_, err = vm.Exec(arg, proc)
	require.NoError(t, err)
	out3 := arg.ctr.buf
	require.Equal(t, 2, out3.Vecs[0].Length())
	require.Equal(t, 2, out3.Vecs[1].Length())
	require.Equal(t, 2, out3.RowCount())
	require.NotSame(t, out3.Vecs[0], out3.Vecs[1])
	require.Equal(t, int64(8), vector.GetFixedAtNoTypeCheck[int64](out3.Vecs[0], 0))
	require.Equal(t, int64(9), vector.GetFixedAtNoTypeCheck[int64](out3.Vecs[0], 1))
	require.Equal(t, int64(8), vector.GetFixedAtNoTypeCheck[int64](out3.Vecs[1], 0))
	require.Equal(t, int64(9), vector.GetFixedAtNoTypeCheck[int64](out3.Vecs[1], 1))

	// teardown releases only operator-owned slots: the vectors the child
	// delivered stay usable afterwards, which is what Free's ownership
	// bookkeeping promises.
	arg.Free(proc, false, nil)
	for _, v := range []*vector.Vector{aVec, bVec, cVec} {
		require.Equal(t, 2, v.Length(),
			"Free releases only operator-owned storage")
	}
	require.Equal(t, int64(4), vector.GetFixedAtNoTypeCheck[int64](bVec, 0))
	require.Equal(t, int64(8), vector.GetFixedAtNoTypeCheck[int64](cVec, 0))
}

// TestPreInsertAliasConversionFailurePreservesBorrowedSlots pins the teardown
// contract when an ownership upgrade of the reusable output buffer fails
// midway through a statement. constructColBuf marks every newly aliased slot
// in canFreeVecIdx before it converts that slot's storage, so a rejected
// UnionBatch leaves the later marked slots still holding the child vector an
// earlier batch borrowed into them. Tearing the operator down after that
// failure must detach those unconverted entries exactly like ordinary
// borrows: freeing one would release the child batch's own storage and zero
// its vector's length (vector.Free), corrupting live child state. Converted
// slots keep their contract from the other direction: the storage the
// operator did allocate before the failure must be released.
//
// The rejection is deterministic: the operator runs on a 1 MiB-capped pool
// (the minimum cap NewMPool accepts), children live in their own unlimited
// pool, and the batch shape [p,p,s,s,...] upgrades three marked slots at once
// - the first union fits the cap, the second needs the same footprint again
// and is rejected by the pool cap check on its very first growth request,
// and the third remains unconverted when the error returns.
func TestPreInsertAliasConversionFailurePreservesBorrowedSlots(t *testing.T) {
	childMp := mpool.MustNew("alias-failure-child-vectors")
	ownedMp, err := mpool.NewMPool("alias-failure-operator", int64(1<<20), 0)
	require.NoError(t, err)
	proc := testutil.NewProcessWithOwnedMPool(t, "", ownedMp)
	childVecs := make([]*vector.Vector, 0, 7)
	defer func() {
		proc.Free()
		for _, v := range childVecs {
			v.Free(childMp)
		}
		mpool.DeleteMPool(childMp)
	}()

	buildVec := func(vals []int64) *vector.Vector {
		v := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(v, vals, nil, childMp))
		childVecs = append(childVecs, v)
		return v
	}

	// batch 1: five distinct borrowed vectors; every slot borrows its child
	// vector, nothing is marked, nothing is allocated.
	borrowed := make([]*vector.Vector, 0, 5)
	bat1 := batch.New([]string{"a", "b", "c", "d", "e"})
	for i := range 5 {
		v := buildVec([]int64{int64(10*(i+1) + 1), int64(10*(i+1) + 2)})
		borrowed = append(borrowed, v)
	}
	bat1.Vecs = borrowed
	bat1.SetRowCount(2)

	// batch 2: [p, p, s, s, s] with 80000 rows per vector (640000 bytes of
	// int64 data per converted destination). Slots 1, 3 and 4 are all
	// marked for conversion at once; under the 1 MiB cap slot 1's union
	// succeeds while slot 3's identical growth request - the second 640000
	// bytes against a pool already holding the first - is rejected.
	bigRows := 80000
	pVals := make([]int64, bigRows)
	sVals := make([]int64, bigRows)
	for i := range bigRows {
		pVals[i] = int64(i) + 7
		sVals[i] = int64(2*i) + 1
	}
	pVec := buildVec(pVals)
	sVec := buildVec(sVals)
	bat2 := batch.New([]string{"a", "b", "c", "d", "e"})
	bat2.Vecs = []*vector.Vector{pVec, pVec, sVec, sVec, sVec}
	bat2.SetRowCount(bigRows)

	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			ignoreFillNullableCol("a", types.T_int64),
			ignoreFillNullableCol("b", types.T_int64),
			ignoreFillNullableCol("c", types.T_int64),
			ignoreFillNullableCol("d", types.T_int64),
			ignoreFillNullableCol("e", types.T_int64),
		},
		Pkey: &plan.PrimaryKeyDef{},
	}
	arg := &PreInsert{
		ctr:      container{canFreeVecIdx: make(map[int]bool)},
		TableDef: tableDef,
		Attrs:    []string{"a", "b", "c", "d", "e"},
	}
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat1, bat2})
	arg.Children = nil
	arg.AppendChild(op)
	require.NoError(t, arg.Prepare(proc))

	// batch 1: plain borrows across the whole width.
	require.Zero(t, ownedMp.CurrNB(),
		"a plain borrowed first batch consumes nothing of the operator pool")
	_, err = vm.Exec(arg, proc)
	require.NoError(t, err)
	require.Equal(t, 2, arg.ctr.buf.RowCount())
	for i, v := range borrowed {
		require.Same(t, v, arg.ctr.buf.Vecs[i],
			"every slot borrows its child vector before any alias appears")
		require.False(t, arg.ctr.canFreeVecIdx[i])
		require.False(t, arg.ctr.ownedVecSlots[i])
	}
	used0 := ownedMp.CurrNB()

	// batch 2: three slots are marked for a simultaneous ownership upgrade;
	// the conversion fails at the second one under the capped pool.
	_, err = vm.Exec(arg, proc)
	require.Error(t, err,
		"the capped pool must reject the second simultaneous conversion deterministically")
	require.True(t, arg.ctr.canFreeVecIdx[1])
	require.True(t, arg.ctr.canFreeVecIdx[3])
	require.True(t, arg.ctr.canFreeVecIdx[4],
		"the third simultaneous upgrade is already marked before its conversion runs")
	require.False(t, arg.ctr.canFreeVecIdx[0])
	require.False(t, arg.ctr.canFreeVecIdx[2])
	require.True(t, arg.ctr.ownedVecSlots[1],
		"the first marked slot completed its conversion before the failure")
	require.True(t, arg.ctr.ownedVecSlots[3],
		"the failing slot installed its fresh storage before its union was rejected")
	require.False(t, arg.ctr.ownedVecSlots[4],
		"the third marked slot never became owned; conversion stopped at the failure")
	require.Same(t, borrowed[4], arg.ctr.buf.Vecs[4],
		"the marked-but-unconverted slot still holds the child vector batch 1 borrowed")
	require.NotSame(t, pVec, arg.ctr.buf.Vecs[1],
		"the converted slot never aliases its source child vector")
	require.Greater(t, ownedMp.CurrNB(), used0,
		"the completed conversion holds operator allocations awaiting teardown")

	// teardown exactly as the failed pipeline performs it.
	arg.Free(proc, true, err)

	// the child batch keeps full ownership of every vector it delivered:
	// unconverted marked slots are detached, never freed.
	for i, v := range borrowed {
		require.Equal(t, 2, v.Length(),
			"Free detaches every borrowed entry - a freed borrow would zero the child vector's length (slot %d)", i)
		require.Equal(t, int64(10*(i+1)+1), vector.GetFixedAtNoTypeCheck[int64](v, 0))
	}
	require.Equal(t, bigRows, pVec.Length())
	require.Equal(t, bigRows, sVec.Length())
	require.Equal(t, int64(7), vector.GetFixedAtNoTypeCheck[int64](pVec, 0))
	require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](sVec, 0))
	// the operator's own conversion allocation is released by teardown.
	require.Equal(t, used0, ownedMp.CurrNB(),
		"Free releases the storage the completed conversion allocated")
	require.Nil(t, arg.ctr.buf)
}
