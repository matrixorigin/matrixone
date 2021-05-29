package col

import (
	"github.com/stretchr/testify/assert"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	mgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	dio "matrixone/pkg/vm/engine/aoe/storage/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout"
	mock "matrixone/pkg/vm/engine/aoe/storage/mock/type"
	w "matrixone/pkg/vm/engine/aoe/storage/worker"
	"runtime"
	"sync"
	"testing"
	"time"
	"unsafe"
)

var WORK_DIR = "/tmp/layout/blk_test"

func init() {
	dio.WRITER_FACTORY.Init(nil, WORK_DIR)
	dio.READER_FACTORY.Init(nil, WORK_DIR)
}

func TestStdColumnBlock(t *testing.T) {
	baseid := layout.ID{}
	var prev_seg IColumnSegment
	var first_seg IColumnSegment
	seg_cnt := 5
	colType := mock.INTEGER
	for i := 0; i < seg_cnt; i++ {
		seg_id := baseid.NextSegment()
		seg := NewColumnSegment(seg_id, 0, colType, UNSORTED_SEG)
		assert.Nil(t, seg.GetNext())
		assert.Nil(t, seg.GetBlockRoot())
		blk_0_id := seg_id.NextBlock()
		blk_0 := NewStdColumnBlock(seg, blk_0_id, MOCK_BLK)
		assert.Nil(t, blk_0.GetNext())
		assert.Equal(t, blk_0, seg.GetBlockRoot())
		blk_1_id := seg_id.NextBlock()
		blk_1 := NewStdColumnBlock(seg, blk_1_id, MOCK_BLK)
		assert.Nil(t, blk_1.GetNext())
		assert.Equal(t, blk_1, blk_0.GetNext())
		if prev_seg != nil {
			prev_seg.SetNext(seg)
		} else {
			first_seg = seg
		}
		prev_seg = seg
	}
	t.Log(first_seg.ToString(true))
	var cnt int
	loopSeg := first_seg
	for loopSeg != nil {
		blk := loopSeg.GetBlockRoot()
		assert.NotNil(t, blk)
		for blk != nil {
			t.Log(blk.GetID())
			blk = blk.GetNext()
			cnt++
		}
		loopSeg = loopSeg.GetNext()
	}
	assert.Equal(t, seg_cnt*2, cnt)
}

func TestStdColumnBlock2(t *testing.T) {
	typeSize := uint64(unsafe.Sizeof(uint64(0)))
	row_count := uint64(64)
	capacity := typeSize * row_count
	bufMgr := makeBufMagr(capacity)
	t.Log(bufMgr.GetCapacity())
	baseid := layout.ID{}
	var prev_seg IColumnSegment
	var first_seg IColumnSegment
	seg_cnt := 5
	colType := mock.INTEGER
	for i := 0; i < seg_cnt; i++ {
		seg_id := baseid.NextSegment()
		seg := NewColumnSegment(seg_id, 0, colType, UNSORTED_SEG)
		assert.Nil(t, seg.GetNext())
		assert.Nil(t, seg.GetBlockRoot())
		blk_0_id := seg_id.NextBlock()
		blk_0 := NewStdColumnBlock(seg, blk_0_id, MOCK_BLK)
		part_0 := NewColumnPart(bufMgr, blk_0, blk_0_id, row_count, typeSize)
		assert.Nil(t, part_0.GetNext())
		assert.Equal(t, part_0, blk_0.GetPartRoot())
		blk_1_id := seg_id.NextBlock()
		blk_1 := NewStdColumnBlock(seg, blk_1_id, MOCK_BLK)
		part_1 := NewColumnPart(bufMgr, blk_1, blk_1_id, row_count, typeSize)
		assert.Nil(t, part_1.GetNext())
		assert.Equal(t, part_1, blk_1.GetPartRoot())
		// assert.Equal(t, row_count*2, seg.GetRowCount())
		if prev_seg != nil {
			prev_seg.SetNext(seg)
		} else {
			first_seg = seg
		}
		prev_seg = seg
	}
	t.Log(first_seg.ToString(true))
	var cnt int
	loopSeg := first_seg
	for loopSeg != nil {
		blk := loopSeg.GetBlockRoot()
		assert.NotNil(t, blk)
		for blk != nil {
			t.Log(blk.GetID())
			blk = blk.GetNext()
			cnt++
		}
		loopSeg = loopSeg.GetNext()
	}
	assert.Equal(t, seg_cnt*2, cnt)

	first_part := first_seg.GetPartRoot()
	assert.NotNil(t, first_part)
	cursor := ScanCursor{
		CurrSeg: first_seg,
		Current: first_part,
	}

	for {
		err := cursor.Init()
		assert.Nil(t, err)
		if !cursor.Next() {
			break
		}
	}

	cursor.Close()
}

func TestStrColumnBlock(t *testing.T) {
	typeSize := uint64(unsafe.Sizeof(uint64(0)))
	row_count := uint64(1)
	capacity := typeSize * row_count
	bufMgr := makeBufMagr(capacity)
	t.Log(bufMgr.GetCapacity())
	baseid := layout.ID{}
	var prev_seg IColumnSegment
	var first_seg IColumnSegment
	seg_cnt := 5
	colType := mock.INTEGER
	for i := 0; i < seg_cnt; i++ {
		seg_id := baseid.NextSegment()
		seg := NewColumnSegment(seg_id, 0, colType, UNSORTED_SEG)
		assert.Nil(t, seg.GetNext())
		assert.Nil(t, seg.GetBlockRoot())
		blk_0_id := seg_id.NextBlock()
		blk_0 := NewStrColumnBlock(seg, blk_0_id, MOCK_BLK)
		part_0_0_id := blk_0_id.NextPart()
		part_0 := NewColumnPart(bufMgr, blk_0, part_0_0_id, row_count, typeSize)
		assert.Nil(t, part_0.GetNext())
		assert.Equal(t, part_0, blk_0.GetPartRoot())
		part_0_1_id := blk_0_id.NextPart()
		part_0_1 := NewColumnPart(bufMgr, blk_0, part_0_1_id, row_count, typeSize)
		assert.Nil(t, part_0_1.GetNext())
		// assert.Equal(t, part_0, blk_0.GetPartRoot())
		if prev_seg != nil {
			prev_seg.SetNext(seg)
		} else {
			first_seg = seg
		}
		prev_seg = seg
	}
	t.Log(first_seg.ToString(true))
	var cnt int

	loopSeg := first_seg
	for loopSeg != nil {
		part := loopSeg.GetPartRoot()
		assert.NotNil(t, part)
		for part != nil {
			t.Log(part.GetID())
			part = part.GetNext()
			cnt++
		}
		loopSeg = loopSeg.GetNext()
	}
	assert.Equal(t, seg_cnt*2, cnt)

	first_part := first_seg.GetPartRoot()
	assert.NotNil(t, first_part)
	cursor := ScanCursor{
		Current: first_part,
	}
	for {
		err := cursor.Init()
		assert.Nil(t, err)
		if !cursor.Next() {
			break
		}
	}

	cursor.Close()
}

type MockType struct {
}

func (t *MockType) Size() uint64 {
	return uint64(4)
}

func TestStdSegmentTree(t *testing.T) {
	baseid := layout.ID{}
	col_idx := 0
	colType := mock.INTEGER
	col_data := NewColumnData(colType, col_idx)

	seg_cnt := 5
	for i := 0; i < seg_cnt; i++ {
		seg_id := baseid.NextSegment()
		seg := NewColumnSegment(seg_id, 0, colType, UNSORTED_SEG)
		assert.Nil(t, seg.GetNext())
		assert.Nil(t, seg.GetBlockRoot())
		err := col_data.Append(seg)
		assert.Nil(t, err)
	}
	assert.Equal(t, uint64(seg_cnt), col_data.SegmentCount())
	t.Log(col_data.String())
	seg := col_data.GetSegmentRoot()
	assert.NotNil(t, seg)
	cnt := 0
	for {
		cnt++
		seg = seg.GetNext()
		if seg == nil {
			break
		}
	}
	assert.Equal(t, seg_cnt, cnt)
	{
		seg := col_data.GetSegmentRoot()
		dseg, err := col_data.DropSegment(seg.GetID())
		assert.Nil(t, err)
		assert.Equal(t, seg.GetID(), dseg.GetID())
	}
	for i := 0; i < 5; i++ {
		runtime.GC()
		time.Sleep(time.Duration(1) * time.Millisecond)
	}
	seg2 := col_data.GetSegmentRoot()
	t.Log(seg2)
}

func TestRegisterNode(t *testing.T) {
	typeSize := uint64(unsafe.Sizeof(uint64(0)))
	row_count := uint64(64)
	capacity := typeSize * row_count
	bufMgr := makeBufMagr(capacity)
	baseid := layout.ID{}
	var prev_seg IColumnSegment
	var first_seg IColumnSegment
	seg_cnt := 5
	colType := mock.INTEGER
	for i := 0; i < seg_cnt; i++ {
		seg_id := baseid.NextSegment()
		seg := NewColumnSegment(seg_id, 0, colType, UNSORTED_SEG)
		assert.Nil(t, seg.GetNext())
		assert.Nil(t, seg.GetBlockRoot())
		blk_0_id := seg_id.NextBlock()
		blk_0 := NewStdColumnBlock(seg, blk_0_id, MOCK_BLK)
		part_0 := NewColumnPart(bufMgr, blk_0, blk_0_id, row_count, typeSize)
		assert.Nil(t, part_0.GetNext())
		assert.Equal(t, part_0, blk_0.GetPartRoot())
		blk_1_id := seg_id.NextBlock()
		blk_1 := NewStdColumnBlock(seg, blk_1_id, MOCK_BLK)
		part_1 := NewColumnPart(bufMgr, blk_1, blk_1_id, row_count, typeSize)
		assert.Nil(t, part_1.GetNext())
		assert.Equal(t, part_1, blk_1.GetPartRoot())
		if prev_seg != nil {
			prev_seg.SetNext(seg)
		} else {
			first_seg = seg
		}
		prev_seg = seg
	}
	t.Log(first_seg.ToString(true))
	blk := first_seg.GetBlockRoot()
	assert.NotNil(t, blk)
	first_part := first_seg.GetPartRoot()
	assert.NotNil(t, first_part)
	cursor := ScanCursor{
		Current: first_part,
	}

	for {
		err := cursor.Init()
		assert.Nil(t, err)
		// t.Logf("%v--%d", cursor.Current.GetID(), cursor.Current.GetBlock().GetBlockType())
		if !cursor.Next() {
			break
		}
	}

	cursor.Close()
}

func makeSegment(bufMgr mgrif.IBufferManager, id layout.ID, blkCnt int, rowCount, typeSize uint64, t *testing.T) IColumnSegment {
	colType := mock.INTEGER
	seg := NewColumnSegment(id, 0, colType, UNSORTED_SEG)
	blk_id := id
	for i := 0; i < blkCnt; i++ {
		_, err := seg.RegisterBlock(bufMgr, blk_id.NextBlock(), rowCount)
		assert.Nil(t, err)
	}
	return seg
}

func makeSegments(bufMgr mgrif.IBufferManager, segCnt, blkCnt int, rowCount, typeSize uint64, t *testing.T) []IColumnSegment {
	baseid := layout.ID{}
	var segs []IColumnSegment
	var rootSeg IColumnSegment
	var prevSeg IColumnSegment
	for i := 0; i < segCnt; i++ {
		seg_id := baseid.NextSegment()
		seg := makeSegment(bufMgr, seg_id, blkCnt, rowCount, typeSize, t)
		segs = append(segs, seg)
		if prevSeg != nil {
			prevSeg.SetNext(seg)
		}
		if rootSeg == nil {
			rootSeg = seg
		}
		prevSeg = seg
	}
	return segs
}

func makeBufMagr(capacity uint64) mgrif.IBufferManager {
	flusher := w.NewOpWorker()
	bufMgr := bmgr.NewBufferManager(capacity, flusher)
	return bufMgr
}

func TestUpgradeStdSegment(t *testing.T) {
	typeSize := uint64(unsafe.Sizeof(uint64(0)))
	row_count := uint64(64)
	capacity := typeSize * row_count * 10000
	bufMgr := makeBufMagr(capacity)
	seg_cnt := 5
	blk_cnt := 4
	segs := makeSegments(bufMgr, seg_cnt, blk_cnt, row_count, typeSize, t)
	rootSeg := segs[0]

	for _, seg := range segs {
		cursor := ScanCursor{}
		err := seg.InitScanCursor(&cursor)
		assert.Nil(t, err)
		assert.False(t, cursor.Inited)
		err = cursor.Init()
		assert.Nil(t, err)
		assert.True(t, cursor.Inited)
	}

	pools := 1
	var savedStrings []*[]string
	var wg sync.WaitGroup
	for i := 0; i < pools; i++ {
		wg.Add(1)
		var strings []string
		savedStrings = append(savedStrings, &strings)
		go func(wgp *sync.WaitGroup, strs *[]string) {
			defer wgp.Done()
			cursor := ScanCursor{CurrSeg: rootSeg}
			err := rootSeg.InitScanCursor(&cursor)
			assert.Nil(t, err)
			cursor.Init()
			cnt := 0
			// prevType := TRANSIENT_BLK
			for cursor.Current != nil {
				cnt += 1
				err = cursor.Init()
				assert.Nil(t, err)
				// assert.True(t, cursor.Current.GetBlock().GetBlockType() >= prevType, cursor.Current.GetBlock().String())
				// prevType = cursor.Current.GetBlock().GetBlockType()
				cursor.Next()
			}
			assert.Equal(t, seg_cnt*blk_cnt, cnt)
		}(&wg, &strings)
	}

	currSeg := rootSeg
	for currSeg != nil {
		ids := currSeg.GetBlockIDs()
		for _, id := range ids {
			oldBlk := currSeg.GetBlock(id)
			assert.NotNil(t, oldBlk)
			assert.Equal(t, TRANSIENT_BLK, oldBlk.GetBlockType())
			blk, err := currSeg.UpgradeBlock(id)
			assert.Nil(t, err)
			assert.Equal(t, PERSISTENT_BLK, blk.GetBlockType())
		}
		currSeg = currSeg.GetNext()
	}

	currSeg = rootSeg
	for currSeg != nil {
		ids := currSeg.GetBlockIDs()
		for _, id := range ids {
			oldBlk := currSeg.GetBlock(id)
			assert.NotNil(t, oldBlk)
			assert.Equal(t, PERSISTENT_BLK, oldBlk.GetBlockType())
			blk, err := currSeg.UpgradeBlock(id)
			assert.Nil(t, err)
			assert.Equal(t, PERSISTENT_SORTED_BLK, blk.GetBlockType())
		}
		currSeg = currSeg.GetNext()
	}
	wg.Wait()
	// currSeg = rootSeg
	// for currSeg != nil {
	// 	ids := currSeg.GetBlockIDs()
	// 	for _, id := range ids {
	// 		blk := currSeg.GetBlock(id)
	// 		t.Log(blk.String())
	// 	}
	// 	currSeg = currSeg.GetNext()
	// }
	// for _, strings := range savedStrings {
	// 	t.Log("---------------------------")
	// 	for _, str := range *strings {
	// 		t.Log(str)
	// 	}
	// 	assert.Equal(t, seg_cnt*blk_cnt, len(*strings))
	// }
}
