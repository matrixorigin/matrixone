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

// After a selection (UnionInt32) or row copy (UnionOne), the marshal plan takes
// the bulk path when every varlena payload has its own area range, and the
// canonical path when payloads are shared (const broadcasts); either way the
// wire decodes to the selected rows.

func TestUnionSelectionPreservesOwnedDisjointVarlenaLayout(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		require.Zero(t, mp.CurrNB())
	})

	source := NewVec(types.T_varchar.ToType(), mp)
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

	selected := NewVec(types.T_varchar.ToType(), mp)
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
	decoded := new(Vector)
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
	selected := NewVec(types.T_varchar.ToType(), mp)
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
	source := NewVec(types.T_varchar.ToType(), mp)
	t.Cleanup(func() { source.Free(mp) })
	values := [][]byte{bytes.Repeat([]byte{'a'}, 49), nil, []byte("inline"), bytes.Repeat([]byte{'b'}, 57)}
	for row, value := range values {
		require.NoError(t, AppendBytes(source, value, row == 1, mp))
	}
	selected := NewVec(types.T_varchar.ToType(), mp)
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
		decoded := new(Vector)
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
	destination := NewVec(types.T_varchar.ToType(), mp)
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

func TestUnionOneNullClearsReusedVarlenaDescriptor(t *testing.T) {
	for _, constant := range []bool{false, true} {
		name := "flat"
		if constant {
			name = "const"
		}
		t.Run(name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			source := NewVec(types.T_varchar.ToType(), mp)
			t.Cleanup(func() { source.Free(mp) })
			if constant {
				source.Free(mp)
				source = NewConstNull(types.T_varchar.ToType(), 1, mp)
			} else {
				require.NoError(t, AppendBytes(source, nil, true, mp))
			}
			destination := NewVec(types.T_varchar.ToType(), mp)
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
			decoded := new(Vector)
			t.Cleanup(func() { decoded.Free(nil) })
			require.NoError(t, decoded.UnmarshalBinary(wire))
			require.False(t, decoded.IsNull(0))
			require.Empty(t, decoded.GetBytesAt(0))
		})
	}
}

// A const NULL destination stays const NULL as const NULL rows are added.
func TestUnionOneConstDestinationStaysConst(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	destination := NewVec(types.T_varchar.ToType(), mp)
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

// BenchmarkMarshalBinaryOwnedVarlena is the non-Arrow control for the shared
// vector serialization path. The vector is built through ordinary owned
// appends, and only MarshalBinaryWithBuffer is measured.
func BenchmarkMarshalBinaryOwnedVarlena(b *testing.B) {
	const rows = 8192
	tests := []struct {
		name   string
		value  []byte
		nullAt int
	}{
		{name: "inline_15_owned", value: bytes.Repeat([]byte{'i'}, 15), nullAt: -1},
		{name: "long_49_owned", value: bytes.Repeat([]byte{'l'}, 49), nullAt: -1},
		{name: "inline_15_one_null", value: bytes.Repeat([]byte{'i'}, 15), nullAt: rows / 2},
		{name: "long_49_one_null", value: bytes.Repeat([]byte{'l'}, 49), nullAt: rows / 2},
	}

	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			mp := mpool.MustNewZero()
			vec := NewVec(types.T_varchar.ToType(), mp)
			for row := 0; row < rows; row++ {
				if err := AppendBytes(vec, test.value, row == test.nullAt, mp); err != nil {
					vec.Free(mp)
					b.Fatal(err)
				}
			}
			defer vec.Free(mp)

			var buf bytes.Buffer
			buf.Grow(vec.Size() + 64)
			b.ReportAllocs()
			b.SetBytes(int64(vec.Size()))
			b.ResetTimer()
			for range b.N {
				buf.Reset()
				if err := vec.MarshalBinaryWithBuffer(&buf); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkMarshalBinarySelectedVarlena covers shuffle and hash-build spill
// batches, which materialize bucket selections through UnionInt32 before
// serialization.
func BenchmarkMarshalBinarySelectedVarlena(b *testing.B) {
	const rows = 8192
	mp := mpool.MustNewZero()
	source := NewVec(types.T_varchar.ToType(), mp)
	value := bytes.Repeat([]byte{'x'}, 49)
	sels := make([]int32, rows)
	for row := 0; row < rows; row++ {
		sels[row] = int32(row)
		if err := AppendBytes(source, value, false, mp); err != nil {
			b.Fatal(err)
		}
	}
	defer source.Free(mp)

	selected := NewVec(types.T_varchar.ToType(), mp)
	if err := selected.PreExtend(rows, mp); err != nil {
		b.Fatal(err)
	}
	if err := selected.UnionInt32(source, sels, mp); err != nil {
		b.Fatal(err)
	}
	defer selected.Free(mp)

	var buf bytes.Buffer
	buf.Grow(selected.Size() + 64)
	b.ReportAllocs()
	b.SetBytes(int64(selected.Size()))
	b.ResetTimer()
	for range b.N {
		buf.Reset()
		if err := selected.MarshalBinaryWithBuffer(&buf); err != nil {
			b.Fatal(err)
		}
	}
}
