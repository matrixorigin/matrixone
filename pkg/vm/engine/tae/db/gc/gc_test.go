package gc

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"path/filepath"
	"testing"
)

const (
	ModuleName = "dbgc"
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

func TestGcTable_Merge(t *testing.T) {
	dir := InitTestEnv(ModuleName, t)
	dir = path.Join(dir, "/local")
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(c)
	assert.Nil(t, err)
	mp := mpool.MustNewZero()
	fs := objectio.NewObjectFS(service, dir)
	manger := NewManager(fs)
	for i := 1; i < 10; i++ {
		id := i
		name := fmt.Sprintf("%d.seg", id)
		bat := newBatch(mp)
		defer bat.Clean(mp)
		objectWriter, err := objectio.NewObjectWriter(name, service)
		assert.Nil(t, err)
		_, err = objectWriter.Write(bat)
		assert.Nil(t, err)
		_, err = objectWriter.Write(bat)
		assert.Nil(t, err)
		blocks, err := objectWriter.WriteEnd(context.Background())
		blockid := common.ID{
			SegmentID: uint64(id),
			TableID:   2,
			PartID:    1,
		}
		table := NewGcTable()
		blockid.BlockID = uint64(blocks[0].GetID() + uint32(i))
		table.addBlock(blockid, name)
		blockid.BlockID = uint64(blocks[1].GetID() + uint32(i))
		table.addBlock(blockid, name)
		manger.AddTable(table)
		if i < 2 {
			continue
		}
		blockid.BlockID = uint64(blocks[0].GetID() + uint32(i-1))
		name = fmt.Sprintf("%d.seg", id-1)
		table.deleteBlock(blockid, name)
		if i < 3 {
			continue
		}
		blockid.BlockID = uint64(blocks[1].GetID() + uint32(i-2))
		name = fmt.Sprintf("%d.seg", id-2)
		table.deleteBlock(blockid, name)
	}
	manger.MergeTable()
}
