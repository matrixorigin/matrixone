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
	"strconv"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/require"
)

func TestZoneMapNumeric(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	typ := types.T_int32.ToType()
	zm := NewZoneMap(typ)
	var yes bool
	var err error
	var visibility *roaring.Bitmap
	yes = zm.Contains(int32(0))
	require.False(t, yes)

	rows := 1000
	ctx := new(KeysCtx)
	ctx.Keys = containers.MockVector2(typ, rows, 0)
	ctx.Count = rows
	defer ctx.Keys.Close()
	err = zm.BatchUpdate(ctx)
	require.NoError(t, err)

	yes = zm.Contains(int32(0))
	require.True(t, yes)

	yes = zm.Contains(int32(999))
	require.True(t, yes)

	yes = zm.Contains(int32(555))
	require.True(t, yes)

	yes = zm.Contains(int32(1000))
	require.False(t, yes)

	yes = zm.Contains(int32(-1))
	require.False(t, yes)

	rows = 500
	ctx.Keys = containers.MockVector2(typ, rows, 700)
	ctx.Count = rows
	defer ctx.Keys.Close()
	err = zm.BatchUpdate(ctx)
	require.NoError(t, err)

	yes = zm.Contains(int32(1001))
	require.True(t, yes)

	yes = zm.Contains(int32(1199))
	require.True(t, yes)

	yes = zm.Contains(int32(1200))
	require.False(t, yes)

	rows = 500
	ctx.Keys = containers.MockVector2(typ, rows, -200)
	ctx.Count = rows
	defer ctx.Keys.Close()
	err = zm.BatchUpdate(ctx)
	require.NoError(t, err)

	yes = zm.Contains(int32(-201))
	require.False(t, yes)

	yes = zm.Contains(int32(-100))
	require.True(t, yes)

	buf, err := zm.Marshal()
	require.NoError(t, err)
	zm1 := NewZoneMap(typ)
	err = zm1.Unmarshal(buf)
	require.NoError(t, err)

	rows = 500
	typ1 := typ
	typ1.Oid = types.T_int64
	ctx.Keys = containers.MockVector2(typ1, rows, 2000)
	ctx.Count = rows
	defer ctx.Keys.Close()
	err = zm.BatchUpdate(ctx)
	require.Error(t, err)

	yes = zm1.Contains(int32(1234))
	require.False(t, yes)

	yes = zm1.Contains(int32(1199))
	require.True(t, yes)

	typ1.Oid = types.T_int32
	vec := containers.MockVector2(typ1, rows, 3000)
	defer vec.Close()
	visibility, yes = zm1.ContainsAny(vec)
	require.False(t, yes)
	require.Equal(t, uint64(0), visibility.GetCardinality())

	vec = containers.MockVector2(typ1, rows, 0)
	defer vec.Close()
	visibility, yes = zm1.ContainsAny(vec)
	require.True(t, yes)
	require.Equal(t, uint64(rows), visibility.GetCardinality())

	err = zm1.Update(int32(999999))
	require.NoError(t, err)

	yes = zm1.Contains(int32(99999))
	require.True(t, yes)
}

func TestZoneMapString(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	typ := types.T_char.ToType()
	zm := NewZoneMap(typ)
	var yes bool
	var err error
	yes = zm.Contains([]byte(strconv.Itoa(0)))
	require.False(t, yes)

	rows := 1000
	ctx := new(KeysCtx)
	ctx.Keys = containers.MockVector2(typ, rows, 0)
	ctx.Count = rows
	defer ctx.Keys.Close()
	err = zm.BatchUpdate(ctx)
	require.NoError(t, err)

	yes = zm.Contains([]byte(strconv.Itoa(500)))
	require.True(t, yes)

	yes = zm.Contains([]byte(strconv.Itoa(9999)))
	require.False(t, yes)

	yes = zm.Contains([]byte("/"))
	require.False(t, yes)

	err = zm.Update([]byte("z"))
	require.NoError(t, err)

	yes = zm.Contains([]byte(strconv.Itoa(999999)))
	require.True(t, yes)

	yes = zm.Contains([]byte("abcdefghijklmn"))
	require.True(t, yes)

	yes = zm.Contains([]byte("ydasdasda"))
	require.True(t, yes)

	yes = zm.Contains([]byte("z1"))
	require.False(t, yes)

	buf, err := zm.Marshal()
	require.NoError(t, err)
	zm1 := NewZoneMap(typ)
	err = zm1.Unmarshal(buf)
	require.NoError(t, err)

	yes = zm.Contains([]byte("z1"))
	require.False(t, yes)

	yes = zm.Contains([]byte("z"))
	require.True(t, yes)

	yes = zm.Contains([]byte("/"))
	require.False(t, yes)
}

func TestZMEmptyString(t *testing.T) {
	defer testutils.AfterTest(t)()
	typ := types.T_varchar.ToType()
	zm := NewZoneMap(typ)
	require.Equal(t, typ.Oid, zm.GetType().Oid)
	// check not inited
	require.False(t, zm.Contains(nil))
	_, existed := zm.ContainsAny(nil)
	require.False(t, existed)

	zmNoInit := NewZoneMap(typ)
	data, _ := zm.Marshal()
	_ = zmNoInit.Unmarshal(data)
	require.False(t, zm.Contains(nil))
	_, existed = zm.ContainsAny(nil)
	require.False(t, existed)

	zm.SetMin([]byte{0, 0, 0})
	zm.SetMin([]byte(""))

	require.Equal(t, []byte{0, 0, 0}, zm.GetMax())
	require.Equal(t, []byte{}, zm.GetMin())
	require.True(t, zm.Contains([]byte("")))
	require.True(t, zm.Contains([]byte{0, 0}))
	require.False(t, zm.Contains([]byte{0, 0, 0, 0}))

	data, _ = zm.Marshal()
	zm1 := NewZoneMap(typ)
	_ = zm1.Unmarshal(data)

	require.True(t, zm1.Contains([]byte("")))
	require.True(t, zm1.Contains([]byte{0, 0}))
	// this is a false positive
	require.True(t, zm1.Contains([]byte{0, 0, 0, 0}))
}

func TestZMTruncatedString(t *testing.T) {
	defer testutils.AfterTest(t)()
	mockBytes := func(init byte, size int) []byte {
		ret := make([]byte, size)
		for i := 0; i < size; i++ {
			ret[i] = init
		}
		return ret
	}

	typ := types.T_varchar.ToType()
	zm := NewZoneMap(typ)

	minv := mockBytes(0x00, 33)
	maxv := mockBytes(0xff, 33)
	maxv[3], maxv[32] = 0x00, 0x00

	zm.SetMax(maxv)
	zm.SetMin(minv)

	require.False(t, zm.Contains([]byte("")))
	require.False(t, zm.Contains([]byte{0, 0}))

	// not in original range, but in deserialized range
	edgeMin := mockBytes(0x00, 32)
	edgeMax := mockBytes(0xff, 33)
	edgeMax[3], edgeMax[32] = 0x00, 0x11

	require.False(t, zm.Contains(edgeMin))
	require.False(t, zm.Contains(edgeMax))

	data, _ := zm.Marshal()
	zm1 := NewZoneMap(typ)
	_ = zm1.Unmarshal(data)

	require.True(t, zm1.Contains(edgeMin))
	require.True(t, zm1.Contains(edgeMax))

	zm.SetMax(mockBytes(0xff, 33)) // isInf is true
	data, _ = zm.Marshal()
	zm2 := NewZoneMap(typ)
	zm2.Unmarshal(data)
	require.True(t, zm2.isInf)
	require.True(t, zm2.Contains(mockBytes(0xff, 100)))

}
