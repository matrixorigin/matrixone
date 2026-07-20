// Copyright 2021 Matrix Origin
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
	"errors"
	"testing"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

type testByteJsonEncoder struct {
	value bytejson.ByteJson
	size  uint32
	write int
	err   error
}

func (e testByteJsonEncoder) TypeCode() byte { return e.value.Type }

func (e testByteJsonEncoder) DataSize() uint32 {
	if e.size != 0 {
		return e.size
	}
	return uint32(len(e.value.Data))
}

func (e testByteJsonEncoder) EncodeDataInto(dst []byte) (int, error) {
	if e.write > 0 {
		copy(dst, e.value.Data[:min(e.write, len(e.value.Data))])
	}
	if e.err != nil {
		return 0, e.err
	}
	return copy(dst, e.value.Data), nil
}

func BenchmarkGetStrValue1(b *testing.B) {
	mp := mpool.MustNewZeroNoFixed()

	vecSize := uint64(50000)
	vec := NewVec(types.T_varchar.ToType())
	for i := uint64(0); i < vecSize; i++ {
		err := appendOneBytes(vec, []byte("x"), false, mp)
		require.NoError(b, err)
	}

	g1 := GenerateFunctionStrParameter(vec)

	vv, nn := []byte(nil), false
	for i := 0; i < b.N; i++ {
		for j := uint64(0); j < vecSize; j++ {
			v, n := g1.GetStrValue(j)
			vv, nn = v, n
		}
	}
	_, _ = vv, nn
}

func BenchmarkGetStrValue2(b *testing.B) {
	mp := mpool.MustNewZeroNoFixed()

	vecSize := uint64(50000)
	t2 := types.T_varchar.ToType()
	t2.Width = 10
	vec2 := NewVec(t2)
	for i := uint64(0); i < vecSize; i++ {
		err := appendOneBytes(vec2, []byte("x"), false, mp)
		require.NoError(b, err)
	}

	g2 := GenerateFunctionStrParameter(vec2)

	vv, nn := []byte(nil), false
	for i := 0; i < b.N; i++ {
		for j := uint64(0); j < vecSize; j++ {
			v, n := g2.GetStrValue(j)
			vv, nn = v, n
		}
	}
	_, _ = vv, nn
}

func TestPreExtendAndReset(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()

	wrapper := NewFunctionResultWrapper(types.T_bool.ToType(), mp)

	result := MustFunctionResult[bool](wrapper)
	require.NoError(t, wrapper.PreExtendAndReset(10))
	require.Equal(t, 10, len(result.cols))
	require.Equal(t, 10, result.vec.Length())

	lastCapacity := result.vec.Capacity()
	if lastCapacity > 20 {
		require.NoError(t, wrapper.PreExtendAndReset(20))
		require.Equal(t, 20, len(result.cols))
		require.Equal(t, 20, result.vec.Length())
		require.Equal(t, lastCapacity, result.vec.Capacity())
	} else if lastCapacity > 11 {
		nextLength := lastCapacity - 1
		require.NoError(t, wrapper.PreExtendAndReset(nextLength))
		require.Equal(t, nextLength, len(result.cols))
		require.Equal(t, nextLength, result.vec.Length())
		require.Equal(t, lastCapacity, result.vec.Capacity())
	} else {
		require.NoError(t, wrapper.PreExtendAndReset(20))
		require.Equal(t, 20, len(result.cols))
		require.Equal(t, 20, result.vec.Length())
	}

	wrapper.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestAppendByteJsonEncoded(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	wrapper := NewFunctionResultWrapper(types.T_json.ToType(), mp)
	require.NoError(t, wrapper.PreExtendAndReset(3))
	result := MustFunctionResult[types.Varlena](wrapper)

	inline, err := bytejson.ParseFromString(`null`)
	require.NoError(t, err)
	require.NoError(t, result.AppendByteJsonEncoded(testByteJsonEncoder{value: inline}))

	nonInline, err := bytejson.ParseFromString(`{"long":"abcdefghijklmnopqrstuvwxyz"}`)
	require.NoError(t, err)
	require.NoError(t, result.AppendByteJsonEncoded(testByteJsonEncoder{value: nonInline}))

	inlineStorage, err := inline.Marshal()
	require.NoError(t, err)
	nonInlineStorage, err := nonInline.Marshal()
	require.NoError(t, err)
	require.Equal(t, inlineStorage, result.vec.GetBytesAt(0))
	require.Equal(t, nonInlineStorage, result.vec.GetBytesAt(1))

	oldLength := result.vec.Length()
	oldAreaLength := len(result.vec.area)
	oldAreaData := uintptr(unsafe.Pointer(&result.vec.area[0]))
	values := toSliceOfLengthNoTypeCheck[types.Varlena](result.vec, oldLength+1)
	var sentinel types.Varlena
	sentinel[0] = 1
	sentinel[1] = 0x7f
	values[oldLength] = sentinel
	result.vec.nsp.Add(uint64(oldLength))
	err = result.AppendByteJsonEncoded(testByteJsonEncoder{
		value: nonInline,
		size:  uint32(cap(result.vec.area) + 1024),
		write: 8,
		err:   errors.New("encode failed"),
	})
	require.ErrorContains(t, err, "encode failed")
	require.Equal(t, oldLength, result.vec.Length())
	require.Equal(t, oldAreaLength, len(result.vec.area))
	require.NotEqual(t, oldAreaData, uintptr(unsafe.Pointer(&result.vec.area[0])))
	require.Equal(t, sentinel, values[oldLength])
	require.True(t, result.vec.nsp.Contains(uint64(oldLength)))

	err = result.AppendByteJsonEncoded(testByteJsonEncoder{
		value: nonInline,
		size:  uint32(len(nonInline.Data) + 1),
	})
	require.ErrorContains(t, err, "encoder size mismatch")
	require.Equal(t, oldLength, result.vec.Length())
	require.Equal(t, oldAreaLength, len(result.vec.area))
	require.Equal(t, sentinel, values[oldLength])
	require.True(t, result.vec.nsp.Contains(uint64(oldLength)))

	require.NoError(t, result.AppendByteJsonEncoded(testByteJsonEncoder{value: nonInline}))
	require.Equal(t, nonInlineStorage, result.vec.GetBytesAt(2))
	require.False(t, result.vec.IsNull(uint64(oldLength)))

	wrapper.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestAppendByteJsonEncodedConstResult(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	wrapper := NewFunctionResultWrapper(types.T_json.ToType(), mp)
	require.NoError(t, wrapper.PreExtendAndReset(4))
	result := MustFunctionResult[types.Varlena](wrapper)
	result.vec.SetLength(4)
	result.vec.ToConst()

	value, err := bytejson.ParseFromString(`{"long":"abcdefghijklmnopqrstuvwxyz"}`)
	require.NoError(t, err)
	err = result.AppendByteJsonEncoded(testByteJsonEncoder{
		value: value,
		write: 8,
		err:   errors.New("encode failed"),
	})
	require.ErrorContains(t, err, "encode failed")
	require.True(t, result.vec.IsConst())
	require.Equal(t, 4, result.vec.Length())
	require.Empty(t, result.vec.area)

	require.NoError(t, result.AppendByteJsonEncoded(testByteJsonEncoder{value: value}))
	want, err := value.Marshal()
	require.NoError(t, err)
	require.True(t, result.vec.IsConst())
	require.Equal(t, 4, result.vec.Length())
	require.Equal(t, want, result.vec.GetBytesAt(0))
	require.Equal(t, want, result.vec.GetBytesAt(3))

	wrapper.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestReuseFunctionParameterStr(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	vec := NewVec(types.T_varchar.ToType())
	for i := uint64(0); i < 10; i++ {
		err := appendOneBytes(vec, []byte("x"), false, mp)
		require.NoError(t, err)
	}
	g1 := GenerateFunctionStrParameter(vec)
	ok := ReuseFunctionStrParameter(vec, g1)
	require.Equal(t, true, ok)

	err := appendOneBytes(vec, []byte("x"), true, mp)
	require.NoError(t, err)
	ok = ReuseFunctionStrParameter(vec, g1)
	require.Equal(t, false, ok)
	g1 = GenerateFunctionStrParameter(vec)
	ok = ReuseFunctionStrParameter(vec, g1)
	require.Equal(t, true, ok)

	vec = NewConstNull(types.T_varchar.ToType(), 0, mp)
	ok = ReuseFunctionStrParameter(vec, g1)
	require.Equal(t, false, ok)
	g1 = GenerateFunctionStrParameter(vec)
	ok = ReuseFunctionStrParameter(vec, g1)
	require.Equal(t, true, ok)

	err = appendOneBytes(vec, []byte("x"), false, mp)
	require.Error(t, err)

	ok = ReuseFunctionStrParameter(vec, g1)
	require.Equal(t, true, ok)

	g1 = GenerateFunctionStrParameter(vec)
	ok = ReuseFunctionStrParameter(vec, g1)
	require.Equal(t, true, ok)
}

func TestReuseFunctionParameterFixed(t *testing.T) {
	mp := mpool.MustNewZero()
	var err error
	vec1 := NewVec(types.T_int32.ToType())
	for i := uint64(0); i < 10; i++ {
		err = appendOneFixed(vec1, int32(i), false, mp)
		require.NoError(t, err)
	}
	g2 := GenerateFunctionFixedTypeParameter[int32](vec1)
	ok := ReuseFunctionFixedTypeParameter(vec1, g2)
	require.Equal(t, true, ok)

	err = appendOneFixed(vec1, 0, true, mp)
	require.NoError(t, err)
	ok = ReuseFunctionFixedTypeParameter(vec1, g2)
	require.Equal(t, false, ok)
	g2 = GenerateFunctionFixedTypeParameter[int32](vec1)
	ok = ReuseFunctionFixedTypeParameter(vec1, g2)
	require.Equal(t, true, ok)

	vec1 = NewConstNull(types.T_int32.ToType(), 1, mp)
	ok = ReuseFunctionFixedTypeParameter(vec1, g2)
	require.Equal(t, false, ok)
	g2 = GenerateFunctionFixedTypeParameter[int32](vec1)
	ok = ReuseFunctionFixedTypeParameter(vec1, g2)
	require.Equal(t, true, ok)

	err = appendOneFixed(vec1, int32(0), false, mp)
	require.Error(t, err)

	ok = ReuseFunctionFixedTypeParameter(vec1, g2)
	require.Equal(t, true, ok)
	g2 = GenerateFunctionFixedTypeParameter[int32](vec1)
	ok = ReuseFunctionFixedTypeParameter(vec1, g2)
	require.Equal(t, true, ok)
}
