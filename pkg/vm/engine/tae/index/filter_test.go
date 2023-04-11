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

func TestStaticFilterNumeric(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	typ := types.T_int32.ToType()
	data := containers.MockVector2(typ, 40000, 0)
	defer data.Close()
	sf, err := NewBinaryFuseFilter(data)
	require.NoError(t, err)
	var positive *roaring.Bitmap
	var res bool
	var exist bool

	res, err = sf.MayContainsKey(types.EncodeValue(int32(1209), typ.Oid))
	require.NoError(t, err)
	require.True(t, res)

	res, err = sf.MayContainsKey(types.EncodeValue(int32(5555), typ.Oid))
	require.NoError(t, err)
	require.True(t, res)

	res, err = sf.MayContainsKey(types.EncodeValue(int32(40000), typ.Oid))
	require.NoError(t, err)
	require.False(t, res)

	require.Panics(t, func() {
		res, err = sf.MayContainsKey(types.EncodeValue(int16(0), typ.Oid))
	})

	query := containers.MockVector2(typ, 2000, 1000)
	defer query.Close()
	exist, positive, err = sf.MayContainsAnyKeys(query)
	require.NoError(t, err)
	require.Equal(t, uint64(2000), positive.GetCardinality())
	require.True(t, exist)

	query = containers.MockVector2(typ, 20000, 40000)
	defer query.Close()
	_, positive, err = sf.MayContainsAnyKeys(query)
	require.NoError(t, err)
	fpRate := float32(positive.GetCardinality()) / float32(20000)
	require.True(t, fpRate < float32(0.01))

	var buf []byte
	buf, err = sf.Marshal()
	require.NoError(t, err)

	vec := containers.MockVector2(typ, 0, 0)
	defer vec.Close()
	sf1, err := NewBinaryFuseFilter(vec)
	require.NoError(t, err)
	err = sf1.Unmarshal(buf)
	require.NoError(t, err)

	query = containers.MockVector2(typ, 40000, 0)
	defer query.Close()
	exist, positive, err = sf.MayContainsAnyKeys(query)
	require.NoError(t, err)
	require.Equal(t, uint64(40000), positive.GetCardinality())
	require.True(t, exist)
}

func TestNewBinaryFuseFilter(t *testing.T) {
	testutils.EnsureNoLeak(t)
	typ := types.T_uint32.ToType()
	data := containers.MockVector3(typ, 2000)
	defer data.Close()
	_, err := NewBinaryFuseFilter(data)
	require.NoError(t, err)
}

func BenchmarkCreateFilter(b *testing.B) {
	rows := 1000
	data := containers.MockVector2(types.T_int64.ToType(), rows, 0)
	defer data.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewBinaryFuseFilter(data)
	}
}

func TestStaticFilterString(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	typ := types.T_varchar.ToType()
	data := containers.MockVector2(typ, 40000, 0)
	defer data.Close()
	sf, err := NewBinaryFuseFilter(data)
	require.NoError(t, err)
	var positive *roaring.Bitmap
	var res bool
	var exist bool

	res, err = sf.MayContainsKey([]byte(strconv.Itoa(1209)))
	require.NoError(t, err)
	require.True(t, res)

	res, err = sf.MayContainsKey([]byte(strconv.Itoa(40000)))
	require.NoError(t, err)
	require.False(t, res)

	query := containers.MockVector2(typ, 2000, 1000)
	defer query.Close()
	exist, positive, err = sf.MayContainsAnyKeys(query)
	require.NoError(t, err)
	require.Equal(t, uint64(2000), positive.GetCardinality())
	require.True(t, exist)

	query = containers.MockVector2(typ, 20000, 40000)
	defer query.Close()
	_, positive, err = sf.MayContainsAnyKeys(query)
	require.NoError(t, err)
	fpRate := float32(positive.GetCardinality()) / float32(20000)
	require.True(t, fpRate < float32(0.01))

	var buf []byte
	buf, err = sf.Marshal()
	require.NoError(t, err)

	query = containers.MockVector2(typ, 0, 0)
	defer query.Close()
	sf1, err := NewBinaryFuseFilter(query)
	require.NoError(t, err)
	err = sf1.Unmarshal(buf)
	require.NoError(t, err)

	query = containers.MockVector2(typ, 40000, 0)
	defer query.Close()
	exist, positive, err = sf.MayContainsAnyKeys(query)
	require.NoError(t, err)
	require.Equal(t, uint64(40000), positive.GetCardinality())
	require.True(t, exist)
}
