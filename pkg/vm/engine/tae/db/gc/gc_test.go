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

package gc

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
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

func MockEntry(
	mid common.ID,
	count int,
	manger *diskCleaner,
	t *testing.T,
	mp *mpool.MPool,
	service fileservice.FileService,
	suffix string) {
	bid := uint64(1)
	did := uint64(1)
	for i := 1; i < count; i++ {
		id := i
		name := fmt.Sprintf("%d.%v", id, suffix)
		bat := newBatch(mp)
		defer bat.Clean(mp)
		objectWriter, err := objectio.NewObjectWriter(name, service)
		assert.Nil(t, err)
		_, err = objectWriter.Write(bat)
		assert.Nil(t, err)
		_, err = objectWriter.Write(bat)
		assert.Nil(t, err)
		_, err = objectWriter.WriteEnd(context.Background())
		assert.Nil(t, err)
		blockid := common.ID{
			SegmentID: uint64(id),
			TableID:   mid.TableID,
			PartID:    mid.PartID,
		}
		table := NewGCTable()
		blockid.BlockID = bid
		bid++
		table.addBlock(blockid, name)
		blockid.BlockID = bid
		bid++
		table.addBlock(blockid, name)
		if i < 2 {
			manger.updateInputs(table)
			continue
		}
		blockid.BlockID = did
		name = fmt.Sprintf("%d.seg", id-1)
		table.deleteBlock(blockid, name)
		did += 2
		if i < 3 {
			manger.updateInputs(table)
			continue
		}
		blockid.BlockID = uint64((id - 2) * 2)
		name = fmt.Sprintf("%d.seg", id-2)
		table.deleteBlock(blockid, name)
		manger.updateInputs(table)
	}
}

func TestGCTable_Merge(t *testing.T) {
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
	manger := NewDiskCleaner(fs, nil, nil)
	id := common.ID{
		TableID: 1,
		PartID:  0,
	}
	MockEntry(id, 5, manger, t, mp, service, "seg")
	for _, tb := range manger.inputs.tables {
		logutil.Infof("manger string %v", tb.String())
	}
	gc := manger.softGC()
	assert.Equal(t, 2, len(gc))

	task := NewGCTask(fs, manger)
	err = task.ExecDelete(gc)
	assert.Nil(t, err)
}

func TestGCDropTable(t *testing.T) {
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
	manger := NewDiskCleaner(fs, nil, nil)
	id := common.ID{
		TableID: 1,
		PartID:  0,
	}
	MockEntry(id, 5, manger, t, mp, service, "seg")
	id = common.ID{
		TableID: 2,
		PartID:  1,
	}
	MockEntry(id, 5, manger, t, mp, service, "blk")
	manger.inputs.tables[len(manger.inputs.tables)-1].dbs[1].tables[2].drop = true
	gc := manger.softGC()
	assert.Equal(t, 9, len(gc))

	task := NewGCTask(fs, manger)
	err = task.ExecDelete(gc)
	assert.Nil(t, err)
}
