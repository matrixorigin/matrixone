package db

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vm/engine/aoe/storage"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"sync"
	"sync/atomic"
	"testing"
)

func TestBlock(t *testing.T) {
	schema := md.MockSchema(2)
	opts := new(storage.Options)
	opts.FillDefaults("/tmp")
	typeSize := uint64(schema.ColDefs[0].Type.Size)
	rowCount := uint64(64)
	capacity := typeSize * rowCount * 10000
	bufMgr := bmgr.MockBufMgr(capacity)
	fsMgr := ldio.NewManager("/tmp", true)
	segCnt := uint64(4)
	blkCnt := uint64(4)

	tables := table.NewTables(new(sync.RWMutex), fsMgr, bufMgr, bufMgr, bufMgr)
	info := md.MockInfo(&opts.Mu, rowCount, blkCnt)
	tableMeta := md.MockTable(info, schema, segCnt*blkCnt)
	tableData, err := tables.RegisterTable(tableMeta)
	assert.Nil(t, err)
	segIds := table.MockSegments(tableMeta, tableData)

	segs := make([]*Segment, 0)
	for _, id := range segIds {
		seg := &Segment{
			Data: tableData.StrongRefSegment(id),
			Ids: new(atomic.Value),
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

	segs[0].Data = table.NewSimpleSegment(segs[0].Data.GetType(), &md.Segment{}, nil, nil)

	_, err = blk1.Read([]uint64{uint64(1)}, []string{"mock_0"}, []*bytes.Buffer{bytes.NewBuffer(nil)}, []*bytes.Buffer{bytes.NewBuffer(nil)})
	assert.NotNil(t, err)
	assert.NotPanics(t, func() {
		blk1.Prefetch([]string{"mock_1"})
	})
	assert.Equal(t, int64(-1), blk1.Size("mock_1"))
	assert.Equal(t, int64(-1), blk1.Rows())
}
