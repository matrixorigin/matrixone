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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

func TestZMNull(t *testing.T) {
	zm := NewZM(types.T_datetime)
	x := zm.GetMin()
	require.Nil(t, x)
	y := zm.GetMax()
	require.Nil(t, y)
}

func TestZM(t *testing.T) {
	int64v := int64(100)
	zm1 := BuildZM(types.T_int64, types.EncodeInt64(&int64v))
	require.Equal(t, int64v, zm1.GetMin())
	require.Equal(t, int64v, zm1.GetMax())

	i64l := int64v - 200
	i64h := int64v + 100
	require.True(t, zm1.ContainsKey(types.EncodeInt64(&int64v)))
	require.False(t, zm1.ContainsKey(types.EncodeInt64(&i64l)))
	require.False(t, zm1.ContainsKey(types.EncodeInt64(&i64h)))

	UpdateZMAny(&zm1, i64l)
	t.Log(zm1.String())
	require.True(t, zm1.ContainsKey(types.EncodeInt64(&int64v)))
	require.True(t, zm1.ContainsKey(types.EncodeInt64(&i64l)))
	require.False(t, zm1.ContainsKey(types.EncodeInt64(&i64h)))

	UpdateZMAny(&zm1, i64h)
	t.Log(zm1.String())
	require.True(t, zm1.ContainsKey(types.EncodeInt64(&int64v)))
	require.True(t, zm1.ContainsKey(types.EncodeInt64(&i64l)))
	require.True(t, zm1.ContainsKey(types.EncodeInt64(&i64h)))

	minv := bytes.Repeat([]byte{0x00}, 31)
	maxv := bytes.Repeat([]byte{0xff}, 31)
	maxv[3] = 0x00

	v2 := bytes.Repeat([]byte{0x00}, 29)
	v3 := bytes.Repeat([]byte{0x00}, 30)

	zm2 := BuildZM(types.T_varchar, minv)
	require.False(t, zm2.ContainsKey([]byte("")))
	require.False(t, zm2.ContainsKey(v2))
	require.True(t, zm2.ContainsKey(v3))

	UpdateZM(&zm2, maxv)
	require.False(t, zm2.MaxTruncated())
	t.Log(zm2.String())
	require.True(t, zm2.ContainsKey(maxv))

	maxv[3] = 0xff
	UpdateZM(&zm2, maxv)
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

func BenchmarkZM(b *testing.B) {
	vec := containers.MockVector(types.T_char.ToType(), 10000, true, nil)
	defer vec.Close()
	var bs [][]byte
	for i := 0; i < vec.Length(); i++ {
		bs = append(bs, vec.Get(i).([]byte))
	}

	zm := NewZM(vec.GetType().Oid)
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

	zm = NewZM(vec.GetType().Oid)
	b.Run("build-f64-zm", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := types.EncodeFloat64(&vs[i%vec.Length()])
			UpdateZM(zm, k)
		}
	})
	b.Run("get-f64-zm", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			zm.GetMax()
		}
	})
}
