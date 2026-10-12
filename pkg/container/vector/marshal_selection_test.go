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

package vector

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func TestUnionSelectionPreservesOwnedDisjointVarlenaLayout(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		require.Zero(t, mp.CurrNB())
	})

	source := NewVec(types.T_varchar.ToType())
	defer source.Free(mp)
	values := [][]byte{
		bytes.Repeat([]byte{'a'}, 49),
		bytes.Repeat([]byte{'b'}, 53),
		[]byte("inline"),
		bytes.Repeat([]byte{'c'}, 57),
	}
	for row, value := range values {
		require.NoError(t, AppendBytes(source, value, row == 1, mp))
	}

	selected := NewVec(types.T_varchar.ToType())
	defer selected.Free(mp)
	sels := []int32{3, 0, 1, 3, 2}
	require.NoError(t, selected.PreExtend(len(sels), mp))
	require.NoError(t, selected.UnionInt32(source, sels, mp))
	require.True(t, selected.VarlenaAreaIsDisjoint())
	plan, err := selected.PrepareMarshalBinary()
	require.NoError(t, err)
	require.False(t, plan.canonicalVarlen)

	descriptors := MustFixedColNoTypeCheck[types.Varlena](selected)
	firstOffset, _ := descriptors[0].OffsetLen()
	repeatedOffset, _ := descriptors[3].OffsetLen()
	require.NotEqual(t, firstOffset, repeatedOffset)

	wire, err := selected.MarshalBinary()
	require.NoError(t, err)
	decoded := NewVecFromReuse()
	defer decoded.Free(nil)
	require.NoError(t, decoded.UnmarshalBinary(wire))
	require.Equal(t, values[3], decoded.GetBytesAt(0))
	require.Equal(t, values[0], decoded.GetBytesAt(1))
	require.True(t, decoded.IsNull(2))
	require.Equal(t, values[3], decoded.GetBytesAt(3))
	require.Equal(t, values[2], decoded.GetBytesAt(4))
}

func TestUnionSelectionKeepsConstVarlenaAliased(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		require.Zero(t, mp.CurrNB())
	})

	constant, err := NewConstBytes(
		types.T_varchar.ToType(),
		bytes.Repeat([]byte{'x'}, 49),
		2,
		mp,
	)
	require.NoError(t, err)
	defer constant.Free(mp)
	selected := NewVec(types.T_varchar.ToType())
	defer selected.Free(mp)
	require.NoError(t, selected.PreExtend(2, mp))
	require.NoError(t, selected.UnionInt32(constant, []int32{0, 0}, mp))
	require.False(t, selected.VarlenaAreaIsDisjoint())
	plan, err := selected.PrepareMarshalBinary()
	require.NoError(t, err)
	require.True(t, plan.canonicalVarlen)
}

func TestUnionOnePreservesOwnedDisjointVarlenaLayout(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	source := NewVec(types.T_varchar.ToType())
	t.Cleanup(func() { source.Free(mp) })
	values := [][]byte{bytes.Repeat([]byte{'a'}, 49), nil, []byte("inline"), bytes.Repeat([]byte{'b'}, 57)}
	for row, value := range values {
		require.NoError(t, AppendBytes(source, value, row == 1, mp))
	}
	selected := NewVec(types.T_varchar.ToType())
	t.Cleanup(func() { selected.Free(mp) })
	sels := []int64{3, 0, 1, 3, 2}
	for range 2 {
		selected.ResetWithSameType()
		for _, sel := range sels {
			require.NoError(t, selected.UnionOne(source, sel, mp))
			require.True(t, selected.VarlenaAreaIsDisjoint())
		}
		plan, err := selected.PrepareMarshalBinary()
		require.NoError(t, err)
		require.False(t, plan.canonicalVarlen)
		descriptors := MustFixedColNoTypeCheck[types.Varlena](selected)
		firstOffset, _ := descriptors[0].OffsetLen()
		repeatedOffset, _ := descriptors[3].OffsetLen()
		require.NotEqual(t, firstOffset, repeatedOffset)
		wire, err := selected.MarshalBinary()
		require.NoError(t, err)
		decoded := NewVecFromReuse()
		t.Cleanup(func() { decoded.Free(nil) })
		require.NoError(t, decoded.UnmarshalBinary(wire))
		for row, sel := range sels {
			require.Equal(t, sel == 1, decoded.IsNull(uint64(row)))
			if sel != 1 {
				require.Equal(t, values[sel], decoded.GetBytesAt(row))
			}
		}
	}
}

func TestUnionOneConstVarlenaCopiesIndependentPayloads(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	value := bytes.Repeat([]byte{'x'}, 49)
	source, err := NewConstBytes(types.T_varchar.ToType(), value, 3, mp)
	require.NoError(t, err)
	t.Cleanup(func() { source.Free(mp) })
	destination := NewVec(types.T_varchar.ToType())
	t.Cleanup(func() { destination.Free(mp) })
	for _, sel := range []int64{2, 0} {
		require.NoError(t, destination.UnionOne(source, sel, mp))
	}
	require.True(t, destination.VarlenaAreaIsDisjoint())
	descriptors := MustFixedColNoTypeCheck[types.Varlena](destination)
	firstOffset, _ := descriptors[0].OffsetLen()
	secondOffset, _ := descriptors[1].OffsetLen()
	require.NotEqual(t, firstOffset, secondOffset)
	for row := range destination.Length() {
		require.Equal(t, value, destination.GetBytesAt(row))
	}

	// Existing const broadcasts share an offset. A later deep copy cannot
	// make those earlier descriptors independent.
	destination.ResetWithSameType()
	require.NoError(t, destination.UnionInt32(source, []int32{0, 0}, mp))
	require.NoError(t, destination.UnionOne(source, 1, mp))
	require.False(t, destination.VarlenaAreaIsDisjoint())
	plan, err := destination.PrepareMarshalBinary()
	require.NoError(t, err)
	require.True(t, plan.canonicalVarlen)
}

func TestUnionOneBorrowedAreaRemainsConservative(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	area := bytes.Repeat([]byte{'b'}, 49)
	lease, err := NewRefCountedBufferLease(area, int64(cap(area)), nil)
	require.NoError(t, err)
	t.Cleanup(lease.Release)
	destination := NewVec(types.T_varchar.ToType())
	t.Cleanup(func() { destination.Free(mp) })
	require.NoError(t, destination.PreExtend(2, mp))
	destination.SetLength(1)
	MustFixedColNoTypeCheck[types.Varlena](destination)[0].SetOffsetLen(0, uint32(len(area)))
	require.NoError(t, destination.InstallBorrowedArea(area, lease))
	source := NewVec(types.T_varchar.ToType())
	t.Cleanup(func() { source.Free(mp) })
	require.NoError(t, AppendBytes(source, []byte("inline"), false, mp))
	require.NoError(t, destination.UnionOne(source, 0, mp))
	require.Equal(t, BorrowedLease, destination.AreaBackingKind())
	require.False(t, destination.VarlenaAreaIsDisjoint())
	plan, err := destination.PrepareMarshalBinary()
	require.NoError(t, err)
	require.True(t, plan.canonicalVarlen)
	require.Equal(t, area, destination.GetBytesAt(0))
	require.Equal(t, "inline", destination.GetStringAt(1))
}

func TestUnionOneNullClearsReusedVarlenaDescriptor(t *testing.T) {
	for _, constant := range []bool{false, true} {
		name := "flat"
		if constant {
			name = "const"
		}
		t.Run(name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			source := NewVec(types.T_varchar.ToType())
			t.Cleanup(func() { source.Free(mp) })
			if constant {
				source.Free(mp)
				source = NewConstNull(types.T_varchar.ToType(), 1, mp)
			} else {
				require.NoError(t, AppendBytes(source, nil, true, mp))
			}
			destination := NewVec(types.T_varchar.ToType())
			t.Cleanup(func() { destination.Free(mp) })
			require.NoError(t, AppendBytes(destination, bytes.Repeat([]byte{'x'}, 128), false, mp))
			destination.ResetWithSameType()
			require.NoError(t, destination.UnionOne(source, 0, mp))
			require.True(t, destination.IsNull(0))
			require.True(t, destination.VarlenaAreaIsDisjoint())
			require.Equal(t, types.Varlena{}, MustFixedColNoTypeCheck[types.Varlena](destination)[0])
			destination.GetNulls().Del(0)
			wire, err := destination.MarshalBinary()
			require.NoError(t, err)
			decoded := NewVecFromReuse()
			t.Cleanup(func() { decoded.Free(nil) })
			require.NoError(t, decoded.UnmarshalBinary(wire))
			require.False(t, decoded.IsNull(0))
			require.Empty(t, decoded.GetBytesAt(0))
		})
	}
}

func TestUnionOneConstDestinationDoesNotAcquireDisjointProof(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	destination := NewVec(types.T_varchar.ToType())
	destination.SetClass(CONSTANT)
	t.Cleanup(func() { destination.Free(mp) })
	source := NewConstNull(types.T_varchar.ToType(), 1, mp)
	t.Cleanup(func() { source.Free(mp) })
	for range 2 {
		require.NoError(t, destination.UnionOne(source, 0, mp))
		require.False(t, destination.VarlenaAreaIsDisjoint())
	}
	require.Equal(t, 2, destination.Length())
	require.True(t, destination.IsConstNull())
}

func TestUnionOneNullDoesNotClearWindowOwnerDescriptor(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	owner := NewVec(types.T_varchar.ToType())
	t.Cleanup(func() { owner.Free(mp) })
	values := [][]byte{bytes.Repeat([]byte{'a'}, 49), bytes.Repeat([]byte{'b'}, 57)}
	require.NoError(t, AppendBytesList(owner, values, nil, mp))
	view, err := owner.Window(0, 1)
	require.NoError(t, err)
	t.Cleanup(func() { view.Free(mp) })
	source := NewConstNull(types.T_varchar.ToType(), 1, mp)
	t.Cleanup(func() { source.Free(mp) })
	require.NoError(t, view.UnionOne(source, 0, mp))
	require.True(t, view.IsNull(1))
	require.False(t, view.VarlenaAreaIsDisjoint())
	for row, value := range values {
		require.Equal(t, value, owner.GetBytesAt(row), "NULL append must not mutate shared descriptors")
	}
}

func TestUnionOneVarlenaAllocationFailureFailsClosed(t *testing.T) {
	sourceMP := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, sourceMP.CurrNB()) })
	source := NewVec(types.T_varchar.ToType())
	t.Cleanup(func() { source.Free(sourceMP) })
	require.NoError(t, AppendBytes(source, make([]byte, 2<<20), false, sourceMP))
	mp, err := mpool.NewMPool(t.Name(), 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})
	destination := NewOffHeapVecWithType(types.T_varchar.ToType())
	t.Cleanup(func() { destination.Free(mp) })
	require.NoError(t, AppendBytes(destination, []byte("prefix"), false, mp))
	require.True(t, destination.VarlenaAreaIsDisjoint())
	require.Error(t, destination.UnionOne(source, 0, mp))
	require.False(t, destination.VarlenaAreaIsDisjoint())
	require.Equal(t, "prefix", destination.GetStringAt(0))
}
