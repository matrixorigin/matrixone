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

package objectio

import (
	"context"
	"fmt"
	"time"

	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/assert"
)

const (
	ModuleName = "ObjectIo"
)

func GetDefaultTestPath(module string, t *testing.T) string {
	return filepath.Join("/tmp", module, t.Name())
}

func MakeDefaultTestPath(module string, t *testing.T) string {
	path := GetDefaultTestPath(module, t)
	err := os.MkdirAll(path, os.FileMode(0755))
	assert.Nil(t, err)
	return path
}

func RemoveDefaultTestPath(module string, t *testing.T) {
	path := GetDefaultTestPath(module, t)
	os.RemoveAll(path)
}

func InitTestEnv(module string, t *testing.T) string {
	RemoveDefaultTestPath(module, t)
	return MakeDefaultTestPath(module, t)
}

func TestNewObjectWriter(t *testing.T) {
	dir := InitTestEnv(ModuleName, t)
	dir = path.Join(dir, "/local")
	id := 1
	name := fmt.Sprintf("%d.blk", id)
	mp := mpool.MustNewZero()
	bat := newBatch(mp)
	defer bat.Clean(mp)
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(c)
	assert.Nil(t, err)

	objectWriter, err := NewObjectWriter(name, service)
	assert.Nil(t, err)
	fd, err := objectWriter.Write(bat)
	assert.Nil(t, err)
	for i := range bat.Vecs {
		buf := fmt.Sprintf("test index %d", i)
		index := NewBloomFilter(uint16(i), 0, []byte(buf))
		err = objectWriter.WriteIndex(fd, index)
		assert.Nil(t, err)

		zbuf := make([]byte, 64)
		zbuf[31] = 1
		zbuf[63] = 10
		index, err = NewZoneMap(uint16(i), zbuf)
		assert.Nil(t, err)
		err = objectWriter.WriteIndex(fd, index)
		assert.Nil(t, err)
	}
	_, err = objectWriter.Write(bat)
	assert.Nil(t, err)
	ts := time.Now()
	option := WriteOptions{
		Type: WriteTS,
		Val:  ts,
	}
	blocks, err := objectWriter.WriteEnd(context.Background(), option)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(blocks))
	assert.Nil(t, objectWriter.(*ObjectWriter).buffer)

	objectReader, _ := NewObjectReader(name, service)
	extents := make([]Extent, 2)
	for i, blk := range blocks {
		extents[i] = NewExtent(blk.GetID(), blk.GetExtent().offset, blk.GetExtent().length, blk.GetExtent().originSize)
	}
	pool, err := mpool.NewMPool("objectio_test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	nb0 := pool.CurrNB()
	bs, err := objectReader.ReadMeta(context.Background(), extents, pool)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(bs))
	idxs := make([]uint16, 3)
	idxs[0] = 0
	idxs[1] = 2
	idxs[2] = 3
	vec, err := objectReader.Read(context.Background(), blocks[0].GetExtent(), idxs, pool)
	assert.Nil(t, err)
	vector1 := newVector(types.Type{Oid: types.T_int8}, vec.Entries[0].Object.([]byte))
	assert.Equal(t, int8(3), vector1.Col.([]int8)[3])
	vector2 := newVector(types.Type{Oid: types.T_int32}, vec.Entries[1].Object.([]byte))
	assert.Equal(t, int32(3), vector2.Col.([]int32)[3])
	vector3 := newVector(types.Type{Oid: types.T_int64}, vec.Entries[2].Object.([]byte))
	assert.Equal(t, int64(3), vector3.Col.([]int64)[3])
	indexes, err := objectReader.ReadIndex(context.Background(), blocks[0].GetExtent(), idxs, ZoneMapType, pool)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(indexes))
	assert.Equal(t, uint8(0x1), indexes[0].(*ZoneMap).buf[31])
	assert.Equal(t, uint8(0xa), indexes[0].(*ZoneMap).buf[63])
	indexes, err = objectReader.ReadIndex(context.Background(), blocks[0].GetExtent(), idxs, BloomFilterType, pool)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(indexes))
	assert.Equal(t, "test index 0", string(indexes[0].(*BloomFilter).buf))
	assert.True(t, nb0 == pool.CurrNB())

	fs := NewObjectFS(service, dir)
	dirs, err := fs.ListDir("")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(dirs))
	objectReader, err = NewObjectReader(name, service)
	assert.Nil(t, err)
	bs, err = objectReader.ReadAllMeta(context.Background(), dirs[0].Size, pool)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(bs))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(bs))
	idxs = make([]uint16, 3)
	idxs[0] = 0
	idxs[1] = 2
	idxs[2] = 3
	vec, err = objectReader.Read(context.Background(), bs[0].GetExtent(), idxs, pool)
	assert.Nil(t, err)
	vector1 = newVector(types.Type{Oid: types.T_int8}, vec.Entries[0].Object.([]byte))
	assert.Equal(t, int8(3), vector1.Col.([]int8)[3])
	vector2 = newVector(types.Type{Oid: types.T_int32}, vec.Entries[1].Object.([]byte))
	assert.Equal(t, int32(3), vector2.Col.([]int32)[3])
	vector3 = newVector(types.Type{Oid: types.T_int64}, vec.Entries[2].Object.([]byte))
	assert.Equal(t, int64(3), vector3.Col.([]int64)[3])
	indexes, err = objectReader.ReadIndex(context.Background(), bs[0].GetExtent(), idxs, ZoneMapType, pool)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(indexes))
	assert.Equal(t, uint8(0x1), indexes[0].(*ZoneMap).buf[31])
	assert.Equal(t, uint8(0xa), indexes[0].(*ZoneMap).buf[63])
	indexes, err = objectReader.ReadIndex(context.Background(), bs[0].GetExtent(), idxs, BloomFilterType, pool)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(indexes))
	assert.Equal(t, "test index 0", string(indexes[0].(*BloomFilter).buf))
	assert.True(t, nb0 == pool.CurrNB())

}

func newBatch(mp *mpool.MPool) *batch.Batch {
	types := []types.Type{
		{Oid: types.T_int8},
		{Oid: types.T_int16},
		{Oid: types.T_int32},
		{Oid: types.T_int64},
		{Oid: types.T_uint16},
		{Oid: types.T_uint32},
		{Oid: types.T_uint8},
		{Oid: types.T_uint64},
	}
	return testutil.NewBatch(types, false, int(40000*2), mp)
}

func newVector(tye types.Type, buf []byte) *vector.Vector {
	vector := vector.New(tye)
	vector.Read(buf)
	return vector
}
