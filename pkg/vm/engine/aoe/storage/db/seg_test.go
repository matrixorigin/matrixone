package db

import (
	"matrixone/pkg/encoding"
	"matrixone/pkg/vm/engine/aoe/storage"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v2"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSegment(t *testing.T) {
	rowCount := uint64(64)
	segCnt := uint64(4)
	blkCnt := uint64(4)
	dir := "/tmp/testsegment"
	os.RemoveAll(dir)
	schema := metadata.MockSchema(2)
	opts := new(storage.Options)
	cfg := &storage.MetaCfg{
		BlockMaxRows:     rowCount,
		SegmentMaxBlocks: blkCnt,
	}
	opts.Meta.Conf = cfg
	opts.FillDefaults(dir)
	typeSize := uint64(schema.ColDefs[0].Type.Size)
	capacity := typeSize * rowCount * 10000
	bufMgr := bmgr.MockBufMgr(capacity)
	fsMgr := ldio.NewManager(dir, true)

	tables := table.NewTables(new(sync.RWMutex), fsMgr, bufMgr, bufMgr, bufMgr)
	tableMeta := metadata.MockTable(opts.Meta.Catalog, schema, segCnt*blkCnt, nil)
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

	//for _, seg := range segs {
	//	t.Log(fmt.Sprintf("%v\n", seg))
	//}
	assert.Equal(t, uint64(1), encoding.DecodeUint64([]byte(segs[0].ID())))
	assert.Equal(t, uint64(5), encoding.DecodeUint64([]byte(segs[1].Blocks()[0])))
	assert.Equal(t, uint64(6), encoding.DecodeUint64([]byte(segs[1].Blocks()[1])))
	assert.NotNil(t, segs[0].Block(string(encoding.EncodeUint64(uint64(1)))))
	assert.Nil(t, segs[0].Block(string(encoding.EncodeUint64(uint64(999)))))
	assert.Equal(t, int64(0), segs[0].Rows())

	segs[0].Data.GetIndexHolder().Inited = false
	assert.NotNil(t, segs[0].NewSparseFilter())
	segs[0].Data.GetIndexHolder().Inited = false
	assert.NotNil(t, segs[0].NewFilter())
	segs[0].Data.GetIndexHolder().Inited = false
	assert.NotNil(t, segs[0].NewSummarizer())

	assert.Equal(t, int64(rowCount*blkCnt*typeSize), segs[0].Size("mock_0"))
	opts.Meta.Catalog.Close()
}
