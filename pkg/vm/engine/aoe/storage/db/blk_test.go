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

package db

import (
	"bytes"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	bmgr "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	ldio "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"

	"github.com/stretchr/testify/assert"
)

func TestBlock(t *testing.T) {
	segCnt := uint64(4)
	blkCnt := uint64(4)
	rowCount := uint64(64)
	dir := initTestEnv(t)
	schema := metadata.MockSchema(2)
	opts := new(storage.Options)
	cfg := &storage.MetaCfg{
		BlockMaxRows:     rowCount,
		SegmentMaxBlocks: blkCnt,
	}
	var err error
	opts.Meta.Conf = cfg
	opts.FillDefaults(dir)
	opts.Meta.Catalog, err = opts.CreateCatalog(dir)
	assert.Nil(t, err)
	opts.Meta.Catalog.Start()

	typeSize := uint64(schema.ColDefs[0].Type.Size)
	capacity := typeSize * rowCount * 10000
	bufMgr := bmgr.MockBufMgr(capacity)
	fsMgr := ldio.NewManager("/tmp", true)

	tables := table.NewTables(opts, new(sync.RWMutex), fsMgr, bufMgr, bufMgr, bufMgr, nil)

	dbName := "db1"
	shardId := uint64(0)
	gen := shard.NewMockIndexAllocator()

	tableMeta := metadata.MockDBTable(opts.Meta.Catalog, dbName, schema, nil, segCnt*blkCnt, gen.Shard(shardId))
	tableData, err := tables.RegisterTable(tableMeta)
	assert.Nil(t, err)
	segIds := table.MockSegments(tableMeta, tableData)

	segs := make([]*Segment, 0)
	for _, id := range segIds {
		seg := &Segment{
			Data: tableData.StrongRefSegment(id),
			Ids:  new(atomic.Value),
		}
		segs = append(segs, seg)
	}

	blk1 := segs[0].Block(string(encoding.EncodeUint64(uint64(1))), nil)
	assert.NotNil(t, blk1)
	assert.Equal(t, string(encoding.EncodeUint64(uint64(1))), blk1.ID())

	assert.Equal(t, int64(0), blk1.Rows())
	assert.Equal(t, int64(rowCount*typeSize), blk1.Size("mock_0"))
	assert.NotPanics(t, func() {
		blk1.Prefetch([]string{"mock_1"})
	})
	assert.Panics(t, func() {
		blk1.Prefetch([]string{"xxxx"})
	})
	_, err = blk1.Read([]uint64{uint64(1)}, []string{"xxxx"}, []*bytes.Buffer{bytes.NewBuffer(nil)}, []*bytes.Buffer{bytes.NewBuffer(nil)})
	assert.NotNil(t, err)
	_, err = blk1.Read([]uint64{uint64(1)}, []string{"mock_0"}, []*bytes.Buffer{bytes.NewBuffer(nil)}, []*bytes.Buffer{bytes.NewBuffer(nil)})
	assert.Nil(t, err)

	segs[0].Data = table.NewSimpleSegment(segs[0].Data.GetType(), &metadata.Segment{}, nil, nil)

	_, err = blk1.Read([]uint64{uint64(1)}, []string{"mock_0"}, []*bytes.Buffer{bytes.NewBuffer(nil)}, []*bytes.Buffer{bytes.NewBuffer(nil)})
	assert.NotNil(t, err)
	assert.NotPanics(t, func() {
		blk1.Prefetch([]string{"mock_1"})
	})
	assert.Equal(t, int64(-1), blk1.Size("mock_1"))
	assert.Equal(t, int64(-1), blk1.Rows())
}
