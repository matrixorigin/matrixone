package db

import (
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

func TestSegment(t *testing.T) {
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

	//for _, seg := range segs {
	//	t.Log(fmt.Sprintf("%v\n", seg))
	//}
	assert.Equal(t, uint64(1), encoding.DecodeUint64([]byte(segs[0].ID())))
	assert.Equal(t, uint64(5), encoding.DecodeUint64([]byte(segs[1].Blocks()[0])))
	assert.Equal(t, uint64(6), encoding.DecodeUint64([]byte(segs[1].Blocks()[1])))
	assert.NotNil(t, segs[0].Block(string(encoding.EncodeUint64(uint64(1))), nil))
	assert.Nil(t, segs[0].Block(string(encoding.EncodeUint64(uint64(999))), nil))
	assert.Equal(t, int64(0), segs[0].Rows())

	segs[0].Data.GetIndexHolder().Inited = false
	assert.NotNil(t, segs[0].NewSparseFilter())
	segs[0].Data.GetIndexHolder().Inited = false
	assert.NotNil(t, segs[0].NewFilter())
	segs[0].Data.GetIndexHolder().Inited = false
	assert.NotNil(t, segs[0].NewSummarizer())

	assert.Equal(t, int64(rowCount*blkCnt*typeSize), segs[0].Size("mock_0"))
}
