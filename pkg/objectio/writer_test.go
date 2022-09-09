package objectio

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/stretchr/testify/assert"
	"path"
	"testing"
)

const (
	ModuleName = "ObjectIo"
)

func TestNewObjectWriter(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	dir = path.Join(dir, "/local")
	id := common.NextGlobalSeqNum()
	name := fmt.Sprintf("%d.blk", id)
	bat := newBatch()

	objectWriter, err := NewObjectWriter(name)
	assert.Nil(t, err)
	fd, err := objectWriter.Write(bat)
	assert.Nil(t, err)
	for i, _ := range bat.Vecs {
		buf := fmt.Sprintf("test index %d", i)
		err = objectWriter.WriteIndex(fd, uint16(i), []byte(buf))
		assert.Nil(t, err)
	}
	_, err = objectWriter.Write(bat)
	assert.Nil(t, err)
	extents, err := objectWriter.WriteEnd()
	assert.Nil(t, err)
	assert.Equal(t, 2, len(extents))
	// Sync is for testing
	err = objectWriter.(*ObjectWriter).Sync(dir)
	assert.Nil(t, err)

	objectReader, _ := NewObjectReader(name, dir)
	idxs := make([]uint16, 3)
	idxs[0] = 0
	idxs[1] = 2
	idxs[2] = 3
	vec, err := objectReader.Read(extents[0], idxs)
	assert.Nil(t, err)
	vector1 := newVector(types.Type{Oid: types.T_int8}, vec.Entries[0].Data)
	assert.Equal(t, int8(3), vector1.Col.([]int8)[3])
	vector2 := newVector(types.Type{Oid: types.T_int32}, vec.Entries[1].Data)
	assert.Equal(t, int32(3), vector2.Col.([]int32)[3])
	vector3 := newVector(types.Type{Oid: types.T_int64}, vec.Entries[2].Data)
	assert.Equal(t, int64(3), vector3.Col.([]int64)[3])
	vec, err = objectReader.ReadIndex(extents[0], idxs)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(vec.Entries))
	assert.Equal(t, "test index 0", string(vec.Entries[0].Data))
}

func newBatch() *batch.Batch {
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	mp := mheap.New(gm)
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
	return testutil.NewBatch(types, false, int(options.DefaultBlockMaxRows*2), mp)
}

func newVector(tye types.Type, buf []byte) *vector.Vector {
	vector := vector.New(tye)
	vector.Read(buf)
	return vector
}
