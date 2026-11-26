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
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// After a selection (Union) or row copy (UnionOne), the marshal plan takes
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
	sels := []int64{3, 0, 1, 3, 2}
	require.NoError(t, selected.PreExtend(len(sels), mp))
	require.NoError(t, selected.Union(source, sels, mp))
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
	require.NoError(t, destination.Union(source, []int64{0, 0}, mp))
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

func TestOwnedVarlenaMarshalKeepsBulkLayout(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_varchar.ToType(), mp)
	defer func() {
		vec.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()
	values := [][]byte{
		[]byte("ordinary inline"),
		[]byte("ordinary long value that exceeds inline storage"),
		[]byte("second ordinary long value that exceeds inline storage"),
	}
	require.NoError(t, AppendBytesList(vec, values, nil, mp))

	plan, err := vec.PrepareMarshalBinary()
	require.NoError(t, err)
	require.False(t, plan.canonicalVarlen,
		"an ordinary owned append-built vector should retain bulk serialization")
	require.Equal(t, uint32(vec.Length()*vec.GetType().TypeSize()+len(vec.GetArea())),
		plan.dataLength+plan.areaLength)

	encoded, err := vec.MarshalBinary()
	require.NoError(t, err)
	decoded := new(Vector)
	defer decoded.Free(nil)
	require.NoError(t, decoded.UnmarshalBinary(encoded))
	require.Equal(t, values[0], decoded.GetBytesAt(0))
	require.Equal(t, values[1], decoded.GetBytesAt(1))
	require.Equal(t, values[2], decoded.GetBytesAt(2))

	nullable := NewVec(types.T_varchar.ToType(), mp)
	defer nullable.Free(mp)
	require.NoError(t, AppendBytes(nullable, values[0], false, mp))
	require.NoError(t, AppendBytes(nullable, values[1], true, mp))
	nullablePlan, err := nullable.PrepareMarshalBinary()
	require.NoError(t, err)
	require.False(t, nullablePlan.canonicalVarlen,
		"an ordinary owned vector with a safe NULL descriptor should retain bulk serialization")
	require.Nil(t, nullablePlan.normalizedVarlenData)
	nullableEncoded, err := nullable.MarshalBinary()
	require.NoError(t, err)
	nullableDecoded := new(Vector)
	defer nullableDecoded.Free(nil)
	require.NoError(t, nullableDecoded.UnmarshalBinary(nullableEncoded))
	require.True(t, nullableDecoded.IsNull(1))

	stale := NewVec(types.T_varchar.ToType(), mp)
	defer stale.Free(mp)
	require.NoError(t, AppendBytes(stale, values[1], false, mp))
	stale.SetNull(0)
	stalePlan, err := stale.PrepareMarshalBinary()
	require.NoError(t, err)
	require.False(t, stalePlan.canonicalVarlen,
		"an owned stale NULL descriptor should use bulk serialization with normalization")
	require.NotNil(t, stalePlan.normalizedVarlenData)

	staleEncoded, err := stale.MarshalBinary()
	require.NoError(t, err)
	staleDecoded := new(Vector)
	defer staleDecoded.Free(nil)
	require.NoError(t, staleDecoded.UnmarshalBinary(staleEncoded))
	require.True(t, staleDecoded.IsNull(0))

	// A window is a read-only reference to the whole area; it marshals only its rows.
	window, err := vec.Window(2, 3)
	require.NoError(t, err)
	defer window.Free(window.MPool())
	windowEncoded, err := window.MarshalBinary()
	require.NoError(t, err)
	windowDecoded := new(Vector)
	defer windowDecoded.Free(nil)
	require.NoError(t, windowDecoded.UnmarshalBinary(windowEncoded))
	require.Equal(t, values[2], windowDecoded.GetBytesAt(0))
}

func TestNullableFixedWidthMarshalDoesNotCastAsVarlena(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_int64.ToType(), mp)
	defer func() {
		vec.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()
	require.NoError(t, AppendFixedList(vec, []int64{11, 22, 33}, []bool{false, true, false}, mp))

	encoded, err := vec.MarshalBinary()
	require.NoError(t, err)
	size, err := vec.MarshalBinarySize()
	require.NoError(t, err)
	require.Equal(t, len(encoded), size)
	decoded := new(Vector)
	defer decoded.Free(nil)
	require.NoError(t, decoded.UnmarshalBinary(encoded))
	require.Equal(t, []int64{11, 0, 33}, MustFixedColNoTypeCheck[int64](decoded))
	require.True(t, decoded.IsNull(1))
}

func TestMarshalAndUnMarshal(t *testing.T) {
	mp := mpool.MustNewZero()
	v := NewVec(types.T_int8.ToType(), mp)
	err := AppendFixedList(v, []int8{0, 1, 2}, nil, mp)
	require.NoError(t, err)
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	size, err := v.MarshalBinarySize()
	require.NoError(t, err)
	require.Equal(t, len(data), size)
	var streamed bytes.Buffer
	require.NoError(t, v.MarshalBinaryTo(&streamed))
	require.Equal(t, data, streamed.Bytes())
	require.ErrorIs(t, v.MarshalBinaryTo(shortVectorMarshalWriter{}), io.ErrShortWrite)
	w := new(Vector)
	err = w.UnmarshalBinary(data)
	require.NoError(t, err)
	require.Equal(t, MustFixedColWithTypeCheck[int8](v), MustFixedColWithTypeCheck[int8](w))
	w = NewVec(types.T_any.ToType(), mp)
	err = w.UnmarshalBinaryWithCopy(data, mp)
	require.NoError(t, err)
	require.Equal(t, MustFixedColWithTypeCheck[int8](v), MustFixedColWithTypeCheck[int8](w))
	require.NoError(t, err)
	v.Free(mp)
	w.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

type shortVectorMarshalWriter struct{}

func (shortVectorMarshalWriter) Write(value []byte) (int, error) {
	return len(value) - 1, nil
}

func TestMarshalBinarySizeRejectsInvalidVector(t *testing.T) {
	var nilVector *Vector
	_, err := nilVector.MarshalBinarySize()
	require.Error(t, err)

	typ := types.T_int64.ToType()
	typ.Size = -1
	invalidType := NewVec(typ, mpool.MustNewZero())
	_, err = invalidType.MarshalBinarySize()
	require.Error(t, err)

	shortData := NewVec(types.T_int64.ToType(), invalidType.mp)
	shortData.SetLength(1)
	_, err = shortData.MarshalBinarySize()
	require.Error(t, err)
}

func TestUnmarshalBinaryAcceptsNullBitmapCoveragePastLength(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, AppendFixed(source, int64(0), false, mp))
	source.GetNulls().AddRange(0, 1)

	data, err := source.MarshalBinary()
	require.NoError(t, err)

	target := new(Vector)
	require.NoError(t, target.UnmarshalBinary(data))
	require.Equal(t, 1, target.Length())
	require.True(t, target.IsNull(0))

	source.Free(mp)
	target.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestUnmarshalBinaryAcceptsStaleVarlenaInNullRow(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_varchar.ToType(), mp)
	require.NoError(t, AppendBytes(source, []byte("value longer than inline storage"), false, mp))
	source.SetNull(0)
	source.ResetArea()

	data, err := source.MarshalBinary()
	require.NoError(t, err)

	target := new(Vector)
	require.NoError(t, target.UnmarshalBinary(data))
	require.Equal(t, 1, target.Length())
	require.True(t, target.IsNull(0))

	source.Free(mp)
	target.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestUnmarshalBinaryRejectsOverflowingNullBitmapLength(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, AppendFixed(source, int64(0), true, mp))
	data, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Free(mp)

	nspLenOffset := 1 + types.TSize + 4 + 4 + types.T_int64.TypeLen() + 4
	nspDataOffset := nspLenOffset + 4
	corrupted := append([]byte(nil), data[:nspDataOffset+24]...)
	corrupted = append(corrupted, data[len(data)-1])
	nspLen := uint32(24)
	count := int64(0)
	bitmapLen := ^uint64(0)
	bitmapDataLen := uint64(0)
	copy(corrupted[nspLenOffset:nspDataOffset], types.EncodeUint32(&nspLen))
	copy(corrupted[nspDataOffset:nspDataOffset+8], types.EncodeInt64(&count))
	copy(corrupted[nspDataOffset+8:nspDataOffset+16], types.EncodeUint64(&bitmapLen))
	copy(corrupted[nspDataOffset+16:nspDataOffset+24], types.EncodeUint64(&bitmapDataLen))

	target := new(Vector)
	require.Error(t, target.UnmarshalBinary(corrupted))
}

func TestUnmarshalBinaryRejectsMisalignedArrayPayload(t *testing.T) {
	for _, test := range []struct {
		name       string
		values     []float32
		corruptLen func([]byte, int)
	}{
		{
			name:   "out_of_line",
			values: make([]float32, 10),
			corruptLen: func(data []byte, varlenOffset int) {
				misalignedLength := uint32(3)
				copy(data[varlenOffset+8:varlenOffset+12], types.EncodeUint32(&misalignedLength))
			},
		},
		{
			name:   "inline",
			values: []float32{0},
			corruptLen: func(data []byte, varlenOffset int) {
				data[varlenOffset] = 3
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			source := NewVec(types.New(types.T_array_float32, 10, 0), mp)
			require.NoError(t, AppendArray(source, test.values, false, mp))
			data, err := source.MarshalBinary()
			require.NoError(t, err)
			source.Free(mp)

			// The array payload remains in bounds, but cannot be decoded as a
			// []float32. Cover both Varlena storage forms.
			corrupted := append([]byte(nil), data...)
			varlenOffset := 1 + types.TSize + 4 + 4
			test.corruptLen(corrupted, varlenOffset)

			target := new(Vector)
			var unmarshalErr error
			require.NotPanics(t, func() {
				unmarshalErr = target.UnmarshalBinary(corrupted)
				if unmarshalErr == nil {
					_ = GetArrayAt[float32](target, 0)
				}
			})
			require.Error(t, unmarshalErr)
		})
	}
}

func TestUnmarshalBinaryRejectsUnsupportedZeroSizeType(t *testing.T) {
	for _, oid := range []types.T{types.T_interval, types.T_tuple} {
		t.Run(oid.String(), func(t *testing.T) {
			source := NewVec(types.Type{Oid: oid}, mpool.MustNewZero())
			data, err := source.MarshalBinary()
			require.NoError(t, err)

			target := new(Vector)
			var unmarshalErr error
			require.NotPanics(t, func() {
				unmarshalErr = target.UnmarshalBinary(data)
			})
			require.Error(t, unmarshalErr)
		})
	}
}

func TestUnmarshalBinaryTrustedKeepsStructuralChecks(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_varchar.ToType(), mp)
	require.NoError(t, AppendBytes(source, []byte("value longer than inline storage"), false, mp))
	data, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Free(mp)

	for end := len(data) - 1; end >= 0; end-- {
		target := new(Vector)
		var unmarshalErr error
		require.NotPanics(t, func() {
			unmarshalErr = target.UnmarshalBinaryTrusted(data[:end])
		}, "truncation at %d bytes", end)
		require.Error(t, unmarshalErr, "truncation at %d bytes", end)
	}
}

func TestUnmarshalBinaryTrustedRequiresPriorSemanticValidation(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_varchar.ToType(), mp)
	require.NoError(t, AppendBytes(source, []byte("value longer than inline storage"), false, mp))
	data, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Free(mp)

	// Preserve the complete frame while forging an out-of-range Varlena
	// offset. The checked boundary rejects it; the trusted bind intentionally
	// relies on a previous checked decode and immutable bytes.
	corrupted := append([]byte(nil), data...)
	varlenOffset := 1 + types.TSize + 4 + 4
	invalidOffset := uint32(len(data) + 1)
	copy(corrupted[varlenOffset+4:varlenOffset+8], types.EncodeUint32(&invalidOffset))

	checked := new(Vector)
	require.Error(t, checked.UnmarshalBinary(corrupted))

	trusted := new(Vector)
	require.NoError(t, trusted.UnmarshalBinaryTrusted(corrupted))
}

// Every vector wire format (protobuf, MarshalBinary with and without copying
// on decode, and legacy V1) keeps a string type's collation identity and
// revision, and every decoder rejects an unknown revision.
func TestVectorWireFormatsKeepCollationMetadata(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, domain := range []struct{ id, revision uint8 }{{0, 0}, {1, 0}, {2, 0}, {3, 0}, {4, 1}, {5, 1}, {7, 1}, {12, 1}} {
		typ := types.NewWithCharset(types.T_varchar, 32, 0, domain.id)
		typ.CollationVersion = domain.revision
		src := NewVec(typ, mp)
		require.NoError(t, AppendBytes(src, []byte("😀"), false, mp))
		proto, err := VectorToProtoVector(src)
		require.NoError(t, err)
		fromProto, err := ProtoVectorToVector(proto, mp)
		require.NoError(t, err)
		require.Equal(t, typ, *fromProto.GetType())
		require.Equal(t, "😀", fromProto.GetStringAt(0))
		fromProto.Free(mp)
		wire, err := src.MarshalBinary()
		require.NoError(t, err)
		for _, copyData := range []bool{false, true} {
			restored := NewVec(types.T_any.ToType(), mp)
			if copyData {
				err = restored.UnmarshalBinaryWithCopy(wire, mp)
			} else {
				err = restored.UnmarshalBinary(wire)
			}
			require.NoError(t, err)
			require.Equal(t, typ, *restored.GetType())
			require.Equal(t, "😀", restored.GetStringAt(0))
			restored.Free(mp)
		}
		var legacy bytes.Buffer
		require.NoError(t, src.MarshalBinaryWithBufferV1(&legacy))
		restored := NewVec(types.T_any.ToType(), mp)
		require.NoError(t, restored.UnmarshalBinaryV1(legacy.Bytes()))
		require.Equal(t, typ, *restored.GetType())
		restored.Free(mp)
		proto.Type.CollationVersion = 257
		bad, err := ProtoVectorToVector(proto, mp)
		require.Error(t, err)
		require.Nil(t, bad)
		// Byte 3 within the native Type is its revision, after the vector class.
		wire[1+3] = 2
		for _, decode := range []func(*Vector) error{
			func(v *Vector) error { return v.UnmarshalBinary(wire) },
			func(v *Vector) error { return v.UnmarshalBinaryTrusted(wire) },
			func(v *Vector) error { return v.UnmarshalBinaryWithCopy(wire, mp) },
			func(v *Vector) error { legacy.Bytes()[1+3] = 2; return v.UnmarshalBinaryV1(legacy.Bytes()) },
		} {
			v := NewVec(types.T_any.ToType(), mp)
			require.Error(t, decode(v))
			v.Free(mp)
		}
		src.Free(mp)
	}
	require.Zero(t, mp.CurrNB())
}

func encodeJSONText(t testing.TB, text string) []byte {
	t.Helper()
	value, err := bytejson.ParseFromString(text)
	require.NoError(t, err)
	raw, err := value.Marshal()
	require.NoError(t, err)
	return raw
}

// Every decoder round-trips each vector shape, rejects an unknown class, and
// never rewrites the caller's input bytes.
func TestVectorDecodersRoundTrip(t *testing.T) {
	type encodedClass struct {
		name    string
		tag     byte
		invalid bool
	}
	for _, kind := range []string{"fixed", "varlen", "json", "constant", "constant_null"} {
		t.Run(kind, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() {
				defer mpool.DeleteMPool(mp)
				require.Zero(t, mp.CurrNB())
			})
			var source *Vector
			var err error
			t.Cleanup(func() {
				if source != nil {
					source.Free(mp)
				}
			})
			switch kind {
			case "constant":
				source, err = NewConstFixed(types.T_int64.ToType(), int64(11), 3, mp)
				require.NoError(t, err)
			case "constant_null":
				source = NewConstNull(types.T_int64.ToType(), 3, mp)
			case "fixed":
				source = NewVec(types.T_int64.ToType(), mp)
				for row, value := range []int64{11, 22, 33} {
					require.NoError(t, AppendFixed(source, value, row == 1, mp))
				}
			default:
				typ := types.T_varchar.ToType()
				values := [][]byte{[]byte("short"), nil, []byte("a value longer than the inline string representation")}
				if kind == "json" {
					typ = types.T_json.ToType()
					values = [][]byte{encodeJSONText(t, `[1]`), nil, encodeJSONText(t, `{"value":"longer than inline storage"}`)}
				}
				source = NewVec(typ, mp)
				for row, value := range values {
					require.NoError(t, AppendBytes(source, value, row == 1, mp))
				}
			}
			wire, err := source.MarshalBinary()
			require.NoError(t, err)
			var legacy bytes.Buffer
			require.NoError(t, source.MarshalBinaryWithBufferV1(&legacy))
			classes := []encodedClass{{name: "original", tag: byte(source.class)}}
			if kind == "fixed" {
				classes = append(classes, encodedClass{name: "unknown", tag: CONSTANT + 1, invalid: true})
			}
			for _, class := range classes {
				t.Run(class.name, func(t *testing.T) {
					for _, mode := range []string{"binary", "trusted", "copy", "legacy_v1", "reader"} {
						t.Run(mode, func(t *testing.T) {
							encoded := bytes.Clone(wire)
							if mode == "legacy_v1" {
								encoded = bytes.Clone(legacy.Bytes())
							}
							encoded[0] = class.tag
							before := bytes.Clone(encoded)
							target := NewVec(types.T_any.ToType(), mp)
							t.Cleanup(func() { target.Free(mp) })
							var decodeErr error
							switch mode {
							case "binary":
								decodeErr = target.UnmarshalBinary(encoded)
							case "trusted":
								if !class.invalid {
									checked := NewVec(types.T_any.ToType(), mp)
									t.Cleanup(func() { checked.Free(mp) })
									require.NoError(t, checked.UnmarshalBinary(encoded))
								}
								decodeErr = target.UnmarshalBinaryTrusted(encoded)
							case "copy":
								decodeErr = target.UnmarshalBinaryWithCopy(encoded, mp)
							case "legacy_v1":
								decodeErr = target.UnmarshalBinaryV1(encoded)
							case "reader":
								decodeErr = target.UnmarshalWithReader(bytes.NewReader(encoded), mp)
							}
							require.Equal(t, before, encoded, "decoding must not rewrite shared input bytes")
							if class.invalid {
								require.Error(t, decodeErr)
								return
							}
							require.NoError(t, decodeErr)
							require.Equal(t, source.class, target.class)
							require.Equal(t, *source.GetType(), *target.GetType())
							require.Equal(t, source.Length(), target.Length())
							for row := 0; row < source.Length(); row++ {
								require.Equal(t, source.IsNull(uint64(row)), target.IsNull(uint64(row)))
								if !source.IsNull(uint64(row)) {
									if source.GetType().IsVarlen() {
										require.Equal(t, source.GetBytesAt(row), target.GetBytesAt(row))
									} else {
										require.Equal(t, source.GetRawBytesAt(row), target.GetRawBytesAt(row))
									}
								}
							}
						})
					}
				})
			}
		})
	}
}

// BenchmarkMarshalBinaryOwnedVarlena measures MarshalBinaryWithBuffer on a
// varchar vector built through ordinary owned appends.
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
// batches, which materialize bucket selections through Union before
// serialization.
func BenchmarkMarshalBinarySelectedVarlena(b *testing.B) {
	const rows = 8192
	mp := mpool.MustNewZero()
	source := NewVec(types.T_varchar.ToType(), mp)
	value := bytes.Repeat([]byte{'x'}, 49)
	sels := make([]int64, rows)
	for row := 0; row < rows; row++ {
		sels[row] = int64(row)
		if err := AppendBytes(source, value, false, mp); err != nil {
			b.Fatal(err)
		}
	}
	defer source.Free(mp)

	selected := NewVec(types.T_varchar.ToType(), mp)
	if err := selected.PreExtend(rows, mp); err != nil {
		b.Fatal(err)
	}
	if err := selected.Union(source, sels, mp); err != nil {
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

func BenchmarkVectorUnmarshalBinary(b *testing.B) {
	const rows = 8192
	mp := mpool.MustNewZero()

	cases := []struct {
		name  string
		typ   types.Type
		build func(*Vector) error
	}{
		{
			name: "fixed_int64",
			typ:  types.T_int64.ToType(),
			build: func(vec *Vector) error {
				return AppendFixedList(vec, make([]int64, rows), nil, mp)
			},
		},
		{
			name: "inline_varchar",
			typ:  types.T_varchar.ToType(),
			build: func(vec *Vector) error {
				values := make([][]byte, rows)
				for i := range values {
					values[i] = []byte("value")
				}
				return AppendBytesList(vec, values, nil, mp)
			},
		},
		{
			name: "null_heavy_int64",
			typ:  types.T_int64.ToType(),
			build: func(vec *Vector) error {
				nulls := make([]bool, rows)
				for i := range nulls {
					nulls[i] = i%2 == 0
				}
				return AppendFixedList(vec, make([]int64, rows), nulls, mp)
			},
		},
		{
			name: "array_float32",
			typ:  types.T_array_float32.ToType(),
			build: func(vec *Vector) error {
				for range rows {
					if err := AppendArray(vec, []float32{1, 2, 3, 4}, false, mp); err != nil {
						return err
					}
				}
				return nil
			},
		},
	}

	for _, test := range cases {
		source := NewVec(test.typ, mp)
		if err := test.build(source); err != nil {
			b.Fatal(err)
		}
		data, err := source.MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
		source.Free(mp)

		b.Run(test.name+"/checked", func(b *testing.B) {
			var target Vector
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := target.UnmarshalBinary(data); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(test.name+"/trusted", func(b *testing.B) {
			var target Vector
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := target.UnmarshalBinaryTrusted(data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
