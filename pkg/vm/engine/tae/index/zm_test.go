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

package index

import (
	"bytes"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

type testArithRes struct {
	zm ZM
	ok bool
}

type testCase struct {
	v1 ZM
	v2 ZM

	// gt,lt,ge,le,inter,and,or,
	expects [][2]bool
	// +,-,*
	arithExpects []*testArithRes
	idx          int
}

var testCases = []*testCase{
	{
		v1: makeZM(types.T_int64, 0, int64(-10), int64(10)),
		v2: makeZM(types.T_int32, 0, int32(-10), int32(-10)),
		expects: [][2]bool{
			{false, false}, {false, false}, {false, false}, {false, false},
			{false, false}, {false, false}, {false, false},
		},
		arithExpects: []*testArithRes{
			{ZM{}, false}, {ZM{}, false}, {ZM{}, false},
		},
		idx: 0,
	},
	{
		v1: makeZM(types.T_int32, 0, int32(-10), int32(10)),
		v2: makeZM(types.T_int32, 0, int32(5), int32(20)),
		expects: [][2]bool{
			{true, true}, {true, true}, {true, true}, {true, true},
			{true, true}, {false, false}, {false, false},
		},
		arithExpects: []*testArithRes{
			{makeZM(types.T_int32, 0, int32(-5), int32(30)), true},
			{makeZM(types.T_int32, 0, int32(-30), int32(5)), true},
			{makeZM(types.T_int32, 0, int32(-200), int32(200)), true},
		},
		idx: 1,
	},
	{
		v1: makeZM(types.T_int16, 0, int16(-10), int16(10)),
		v2: makeZM(types.T_int16, 0, int16(10), int16(20)),
		expects: [][2]bool{
			{false, true}, {true, true}, {true, true}, {true, true},
			{true, true}, {false, false}, {false, false},
		},
		arithExpects: []*testArithRes{
			{makeZM(types.T_int16, 0, int16(0), int16(30)), true},
			{makeZM(types.T_int16, 0, int16(-30), int16(0)), true},
			{makeZM(types.T_int16, 0, int16(-200), int16(200)), true},
		},
		idx: 2,
	},
	{
		v1: makeZM(types.T_decimal128, 5, types.Decimal128{B0_63: 3, B64_127: 0}, types.Decimal128{B0_63: 20, B64_127: 0}),
		v2: makeZM(types.T_decimal128, 8, types.Decimal128{B0_63: 1, B64_127: 0}, types.Decimal128{B0_63: 4, B64_127: 0}),
		expects: [][2]bool{
			{true, true}, {false, true}, {true, true}, {false, true},
			{false, true}, {false, false}, {false, false},
		},
		arithExpects: []*testArithRes{
			{makeZM(types.T_decimal128, 8, types.Decimal128{B0_63: 3001, B64_127: 0}, types.Decimal128{B0_63: 20004, B64_127: 0}), true},
			{makeZM(types.T_decimal128, 8, types.Decimal128{B0_63: 2996, B64_127: 0}, types.Decimal128{B0_63: 19999, B64_127: 0}), true},
			{makeZM(types.T_decimal128, 12, types.Decimal128{B0_63: 0, B64_127: 0}, types.Decimal128{B0_63: 8, B64_127: 0}), true},
		},
		idx: 3,
	},
	{
		v1: makeZM(types.T_decimal64, 5, types.Decimal64(3), types.Decimal64(20)),
		v2: makeZM(types.T_decimal64, 8, types.Decimal64(1), types.Decimal64(4)),
		expects: [][2]bool{
			{true, true}, {false, true}, {true, true}, {false, true},
			{false, true}, {false, false}, {false, false},
		},
		arithExpects: []*testArithRes{
			{makeZM(types.T_decimal64, 8, types.Decimal64(3001), types.Decimal64(20004)), true},
			{makeZM(types.T_decimal64, 8, types.Decimal64(2996), types.Decimal64(19999)), true},
			{makeZM(types.T_decimal64, 12, types.Decimal64(3), types.Decimal64(80)), true},
		},
		idx: 4,
	},
}

func makeZM(t types.T, scale int32, minv, maxv any) ZM {
	zm := NewZM(t, scale)
	zm.Update(minv)
	zm.Update(maxv)
	return zm
}

func runCompare(tc *testCase) [][2]bool {
	r := make([][2]bool, 0)

	res, ok := tc.v1.AnyGT(tc.v2)
	r = append(r, [2]bool{res, ok})
	res, ok = tc.v1.AnyLT(tc.v2)
	r = append(r, [2]bool{res, ok})
	res, ok = tc.v1.AnyGE(tc.v2)
	r = append(r, [2]bool{res, ok})
	res, ok = tc.v1.AnyLE(tc.v2)
	r = append(r, [2]bool{res, ok})
	res, ok = tc.v1.Intersect(tc.v2)
	r = append(r, [2]bool{res, ok})
	res, ok = tc.v1.And(tc.v2)
	r = append(r, [2]bool{res, ok})
	res, ok = tc.v1.Or(tc.v2)
	r = append(r, [2]bool{res, ok})

	return r
}

func runArith(tc *testCase) []*testArithRes {
	r := make([]*testArithRes, 0)
	res := ZMPlus(tc.v1, tc.v2, nil)
	r = append(r, &testArithRes{res, res.IsInited()})
	res = ZMMinus(tc.v1, tc.v2, nil)
	r = append(r, &testArithRes{res, res.IsInited()})
	res = ZMMulti(tc.v1, tc.v2, nil)
	r = append(r, &testArithRes{res, res.IsInited()})
	return r
}

func TestZMOp(t *testing.T) {
	for _, tc := range testCases[0:5] {
		res1 := runCompare(tc)
		for i := range tc.expects {
			require.Equalf(t, tc.expects[i], res1[i], "[%d]compare-%d", tc.idx, i)
		}
		res2 := runArith(tc)
		for i := range tc.arithExpects {
			expect, actual := tc.arithExpects[i], res2[i]
			if expect.ok {
				require.Truef(t, actual.ok, "[%d]arith-%d", tc.idx, i)
				t.Log(expect.zm.String())
				t.Log(actual.zm.String())
				require.Equalf(t, expect.zm, actual.zm, "[%d]arith-%d", tc.idx, i)
			} else {
				require.Falsef(t, actual.ok, "[%d]arith-%d", tc.idx, i)
			}
		}
	}
}

func TestVectorZM(t *testing.T) {
	m := mpool.MustNewNoFixed(t.Name())
	zm := NewZM(types.T_uint32, 0)
	zm.Update(uint32(12))
	zm.Update(uint32(22))

	vec, err := ZMToVector(zm, nil, m)
	require.NoError(t, err)
	require.Equal(t, 2, vec.Length())
	require.False(t, vec.IsConst())
	require.False(t, vec.GetNulls().Any())
	require.Equal(t, uint32(12), vector.GetFixedAt[uint32](vec, 0))
	require.Equal(t, uint32(22), vector.GetFixedAt[uint32](vec, 1))

	zm2 := VectorToZM(vec, nil)
	require.Equal(t, zm, zm2)
	vec.Free(m)

	zm = NewZM(types.T_char, 0)
	zm.Update([]byte("abc"))
	zm.Update([]byte("xyz"))

	vec, err = ZMToVector(zm, nil, m)
	require.NoError(t, err)
	require.Equal(t, 2, vec.Length())
	require.False(t, vec.IsConst())
	require.False(t, vec.GetNulls().Any())
	require.Equal(t, []byte("abc"), vec.GetBytesAt(0))
	require.Equal(t, []byte("xyz"), vec.GetBytesAt(1))

	zm2 = VectorToZM(vec, nil)
	require.Equal(t, zm, zm2)
	vec.Free(m)

	zm.Update(MaxBytesValue)
	require.True(t, zm.MaxTruncated())

	vec, err = ZMToVector(zm, nil, m)
	require.NoError(t, err)
	require.Equal(t, 2, vec.Length())
	require.False(t, vec.IsConst())
	require.False(t, vec.GetNulls().Contains(0))
	require.True(t, vec.GetNulls().Contains(1))
	require.Equal(t, []byte("abc"), vec.GetBytesAt(0))

	zm2 = VectorToZM(vec, nil)
	require.True(t, zm2.MaxTruncated())
	require.Equal(t, []byte("abc"), zm2.GetMinBuf())
	require.Equal(t, zm, zm2)

	vec.Free(m)

	zm = NewZM(types.T_uint16, 0)
	vec, err = ZMToVector(zm, vec, m)

	require.NoError(t, err)
	require.Equal(t, 2, vec.Length())
	require.True(t, vec.IsConstNull())

	zm2 = VectorToZM(vec, nil)
	require.False(t, zm2.IsInited())

	vec.Free(m)

	require.Zero(t, m.CurrNB())
}

func TestZMNull(t *testing.T) {
	zm := NewZM(types.T_int64, 0)
	x := zm.GetMin()
	require.Nil(t, x)
	y := zm.GetMax()
	require.Nil(t, y)

	require.Equal(t, 8, len(zm.GetMinBuf()))
	require.Equal(t, 8, len(zm.GetMaxBuf()))

	require.False(t, zm.Contains(int64(-1)))
	require.False(t, zm.Contains(int64(0)))
	require.False(t, zm.Contains(int64(1)))
}

func TestZM(t *testing.T) {
	int64v := int64(100)
	zm1 := BuildZM(types.T_int64, types.EncodeInt64(&int64v))
	require.Equal(t, int64v, zm1.GetMin())
	require.Equal(t, int64v, zm1.GetMax())
	require.Equal(t, int64v, zm1.GetSum())

	i64l := int64v - 200
	i64h := int64v + 100
	require.True(t, zm1.ContainsKey(types.EncodeInt64(&int64v)))
	require.False(t, zm1.ContainsKey(types.EncodeInt64(&i64l)))
	require.False(t, zm1.ContainsKey(types.EncodeInt64(&i64h)))

	UpdateZMAny(zm1, i64l)
	t.Log(zm1.String())
	require.True(t, zm1.ContainsKey(types.EncodeInt64(&int64v)))
	require.True(t, zm1.ContainsKey(types.EncodeInt64(&i64l)))
	require.False(t, zm1.ContainsKey(types.EncodeInt64(&i64h)))
	require.Equal(t, int64v+i64l, zm1.GetSum())

	UpdateZMAny(zm1, i64h)
	t.Log(zm1.String())
	require.True(t, zm1.ContainsKey(types.EncodeInt64(&int64v)))
	require.True(t, zm1.ContainsKey(types.EncodeInt64(&i64l)))
	require.True(t, zm1.ContainsKey(types.EncodeInt64(&i64h)))
	require.Equal(t, int64v+i64l+i64h, zm1.GetSum())

	minv := bytes.Repeat([]byte{0x00}, 31)
	maxv := bytes.Repeat([]byte{0xff}, 31)
	maxv[3] = 0x00

	v2 := bytes.Repeat([]byte{0x00}, 29)
	v3 := bytes.Repeat([]byte{0x00}, 30)

	zm2 := BuildZM(types.T_varchar, minv)
	require.False(t, zm2.ContainsKey([]byte("")))
	require.False(t, zm2.ContainsKey(v2))
	require.True(t, zm2.ContainsKey(v3))

	UpdateZM(zm2, maxv)
	require.False(t, zm2.MaxTruncated())
	t.Log(zm2.String())
	require.True(t, zm2.ContainsKey(maxv))

	maxv[3] = 0xff
	UpdateZM(zm2, maxv)
	t.Log(zm2.String())
	require.True(t, zm2.MaxTruncated())

	v4 := bytes.Repeat([]byte{0xff}, 100)
	require.True(t, zm2.ContainsKey(v4))

	buf, _ := zm2.Marshal()
	zm3 := DecodeZM(buf)
	t.Log(zm3.String())
	require.Equal(t, zm2.GetMinBuf(), zm3.GetMinBuf())
	require.Equal(t, zm2.GetMaxBuf(), zm3.GetMaxBuf())
	require.True(t, zm3.MaxTruncated())
}

func TestZMSumOverflow(t *testing.T) {
	maxi64 := int64(math.MaxInt64)
	zm1 := BuildZM(types.T_int64, types.EncodeInt64(&maxi64))
	t.Log(zm1.String())
	require.Equal(t, maxi64, zm1.GetSum())

	i64l := int64(1)
	UpdateZMAny(zm1, i64l)
	t.Log(zm1.String())
	require.Equal(t, int64(0), zm1.GetSum())

	UpdateZMAny(zm1, i64l)
	require.Equal(t, int64(0), zm1.GetSum())

	maxf64 := math.MaxFloat64
	zm2 := BuildZM(types.T_float64, types.EncodeFloat64(&maxf64))
	t.Log(zm2.String())
	require.Equal(t, maxf64, zm2.GetSum())

	f64l := float64(1)
	UpdateZMAny(zm2, f64l)
	t.Log(zm2.String())
	require.Equal(t, maxf64, zm2.GetSum())

	UpdateZMAny(zm2, maxf64)
	t.Log(zm2.String())
	require.Equal(t, float64(0), zm2.GetSum())

	UpdateZMAny(zm2, math.SmallestNonzeroFloat64)
	t.Log(zm2.String())
	require.Equal(t, float64(0), zm2.GetSum())

	minf64 := -maxf64
	zm3 := BuildZM(types.T_float64, types.EncodeFloat64(&minf64))
	t.Log(zm3.String())
	require.Equal(t, minf64, zm3.GetSum())
	UpdateZMAny(zm3, minf64)
	t.Log(zm3.String())
	require.Equal(t, float64(0), zm3.GetSum())
}

func BenchmarkZM(b *testing.B) {
	vec := containers.MockVector(types.T_char.ToType(), 10000, true, nil)
	defer vec.Close()
	var bs [][]byte
	for i := 0; i < vec.Length(); i++ {
		bs = append(bs, vec.Get(i).([]byte))
	}

	zm := NewZM(vec.GetType().Oid, 0)
	b.Run("build-bytes-zm", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			UpdateZM(zm, bs[i%vec.Length()])
		}
	})
	b.Run("get-bytes-zm", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			zm.GetMin()
		}
	})

	vec = containers.MockVector(types.T_float64.ToType(), 10000, true, nil)
	defer vec.Close()
	var vs []float64
	for i := 0; i < vec.Length(); i++ {
		vs = append(vs, vec.Get(i).(float64))
	}

	zm = NewZM(vec.GetType().Oid, 0)
	b.Run("build-f64-zm", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N*5; i++ {
			k := types.EncodeFloat64(&vs[i%vec.Length()])
			UpdateZM(zm, k)
		}
	})
	b.Run("get-f64-zm", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N*5; i++ {
			zm.GetMax()
		}
	})
}

func BenchmarkUpdateZMVector(b *testing.B) {
	zm := NewZM(types.T_int64, 0)
	dnVec := containers.MockVector(types.T_int64.ToType(), 10000, false, nil)
	defer dnVec.Close()
	vec := dnVec.GetDownstreamVector()

	b.Run("update-vector", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			BatchUpdateZM(zm, vec)
		}
	})
}
