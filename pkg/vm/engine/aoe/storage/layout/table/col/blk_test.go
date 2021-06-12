package col

import (
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	mgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	dio "matrixone/pkg/vm/engine/aoe/storage/dataio"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"runtime"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

var WORK_DIR = "/tmp/layout/blk_test"

func init() {
	dio.WRITER_FACTORY.Init(nil, WORK_DIR)
	dio.READER_FACTORY.Init(nil, WORK_DIR)
}

func TestStdColumnBlock(t *testing.T) {
	blkRows := uint64(10)
	blks := uint64(10)
	info := md.MockInfo(blkRows, blks)
	schema := md.MockSchema(1)
	seg_cnt := 5
	meta := md.MockTable(info, schema, uint64(seg_cnt)*blks)
	var prev_seg IColumnSegment
	var first_seg IColumnSegment
	mtBufMgr := bmgr.MockBufMgr(10000)
	sstBufMgr := bmgr.MockBufMgr(10000)
	for i := 0; i < seg_cnt; i++ {
		segMeta := meta.Segments[i]
		seg := NewColumnSegment(ldio.DefaultFsMgr, mtBufMgr, sstBufMgr, 0, segMeta)
		assert.Nil(t, seg.GetNext())
		assert.Nil(t, seg.GetBlockRoot())

		bMeta0 := segMeta.Blocks[0]
		blk_0 := NewStdColumnBlock(seg, bMeta0)
		assert.Nil(t, blk_0.GetNext())
		assert.Equal(t, blk_0, seg.GetBlockRoot())
		bMeta1 := segMeta.Blocks[1]
		blk_1 := NewStdColumnBlock(seg, bMeta1)
		assert.Nil(t, blk_1.GetNext())
		assert.Equal(t, blk_1, blk_0.GetNext())
		if prev_seg != nil {
			prev_seg.SetNext(seg)
		} else {
			first_seg = seg
		}
		prev_seg = seg
	}
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
	seg_cnt := 5
	blkRows := uint64(10)
	blks := uint64(10)
	info := md.MockInfo(blkRows, blks)
	schema := md.MockSchema(1)
	meta := md.MockTable(info, schema, uint64(seg_cnt)*blks)

	typeSize := uint64(unsafe.Sizeof(uint64(0)))
	row_count := info.Conf.BlockMaxRows
	capacity := typeSize * row_count * uint64(seg_cnt) * 2
	bufMgr := bmgr.MockBufMgr(capacity)
	var prev_seg IColumnSegment
	var first_seg IColumnSegment
	for i := 0; i < seg_cnt; i++ {
		segMeta := meta.Segments[i]
		seg := NewColumnSegment(ldio.DefaultFsMgr, bufMgr, bufMgr, 0, segMeta)
		assert.Nil(t, seg.GetNext())
		assert.Nil(t, seg.GetBlockRoot())
		blkMeta0 := segMeta.Blocks[0]
		blk_0 := NewStdColumnBlock(seg, blkMeta0)
		part_0 := NewColumnPart(ldio.DefaultFsMgr, bufMgr, blk_0, *blkMeta0.AsCommonID(), row_count, typeSize)
		assert.Nil(t, part_0.GetNext())
		assert.Equal(t, part_0, blk_0.GetPartRoot())
		blkMeta1 := segMeta.Blocks[1]
		blk_1 := NewStdColumnBlock(seg, blkMeta1)
		part_1 := NewColumnPart(ldio.DefaultFsMgr, bufMgr, blk_1, *blkMeta1.AsCommonID(), row_count, typeSize)
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
	seg_cnt := 5
	blkRows := uint64(10)
	blks := uint64(10)
	info := md.MockInfo(blkRows, blks)
	schema := md.MockSchema(1)
	meta := md.MockTable(info, schema, uint64(seg_cnt)*blks)

	typeSize := uint64(schema.ColDefs[0].Type.Size)
	row_count := info.Conf.BlockMaxRows
	capacity := uint64(typeSize) * row_count * 10
	bufMgr := bmgr.MockBufMgr(capacity)
	fsMgr := ldio.MockFsMgr
	var prev_seg IColumnSegment
	var first_seg IColumnSegment
	for i := 0; i < seg_cnt; i++ {
		segMeta := meta.Segments[i]
		seg := NewColumnSegment(fsMgr, bufMgr, bufMgr, 0, segMeta)
		assert.Nil(t, seg.GetNext())
		assert.Nil(t, seg.GetBlockRoot())
		blkMeta0 := segMeta.Blocks[0]
		blk0Id := *blkMeta0.AsCommonID()
		blk_0 := NewStrColumnBlock(seg, blk0Id, TRANSIENT_BLK)
		part_0_0_id := blk0Id.NextPart()
		part_0 := NewColumnPart(fsMgr, bufMgr, blk_0, part_0_0_id, row_count, typeSize)
		assert.Nil(t, part_0.GetNext())
		assert.Equal(t, part_0, blk_0.GetPartRoot())
		part_0_1_id := blk0Id.NextPart()
		part_0_1 := NewColumnPart(fsMgr, bufMgr, blk_0, part_0_1_id, row_count, typeSize)
		assert.Nil(t, part_0_1.GetNext())
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

func TestStdSegmentTree(t *testing.T) {
	seg_cnt := 5
	blkRows := uint64(10)
	blks := uint64(10)
	info := md.MockInfo(blkRows, blks)
	schema := md.MockSchema(1)
	meta := md.MockTable(info, schema, uint64(seg_cnt)*blks)
	fsMgr := ldio.DefaultFsMgr

	col_idx := 0
	bufMgr := bmgr.MockBufMgr(1000000)
	col_data := NewColumnData(fsMgr, bufMgr, bufMgr, schema.ColDefs[0].Type, col_idx)

	for i := 0; i < seg_cnt; i++ {
		segMeta := meta.Segments[i]
		seg := NewColumnSegment(fsMgr, bufMgr, bufMgr, col_idx, segMeta)
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
	seg_cnt := 5
	blkRows := uint64(10)
	blks := uint64(10)
	info := md.MockInfo(blkRows, blks)
	schema := md.MockSchema(1)
	meta := md.MockTable(info, schema, uint64(seg_cnt)*blks)
	fsMgr := ldio.DefaultFsMgr

	col_idx := 0
	typeSize := uint64(schema.ColDefs[col_idx].Type.Size)
	row_count := uint64(64)
	capacity := typeSize * row_count * uint64(seg_cnt) * 2
	bufMgr := bmgr.MockBufMgr(capacity)
	var prev_seg IColumnSegment
	var first_seg IColumnSegment
	for i := 0; i < seg_cnt; i++ {
		segMeta := meta.Segments[i]
		seg := NewColumnSegment(fsMgr, bufMgr, bufMgr, col_idx, segMeta)
		assert.Nil(t, seg.GetNext())
		assert.Nil(t, seg.GetBlockRoot())
		blk0Meta := segMeta.Blocks[0]
		blk0Id := *blk0Meta.AsCommonID()
		blk_0 := NewStdColumnBlock(seg, blk0Meta)
		part_0 := NewColumnPart(ldio.DefaultFsMgr, bufMgr, blk_0, blk0Id, row_count, typeSize)
		assert.Nil(t, part_0.GetNext())
		assert.Equal(t, part_0, blk_0.GetPartRoot())
		blk1Meta := segMeta.Blocks[1]
		blk_1 := NewStdColumnBlock(seg, blk1Meta)
		part_1 := NewColumnPart(ldio.DefaultFsMgr, bufMgr, blk_1, *blk1Meta.AsCommonID(), row_count, typeSize)
		assert.Nil(t, part_1.GetNext())
		assert.Equal(t, part_1, blk_1.GetPartRoot())
		if prev_seg != nil {
			prev_seg.SetNext(seg)
		} else {
			first_seg = seg
		}
		prev_seg = seg
		t.Log(bufMgr.String())
	}
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

func makeSegment(fsMgr ldio.IManager, mtBufMgr, sstBufMgr mgrif.IBufferManager, colIdx int, meta *md.Segment, t *testing.T) IColumnSegment {
	seg := NewColumnSegment(fsMgr, mtBufMgr, sstBufMgr, colIdx, meta)
	for _, blkMeta := range meta.Blocks {
		blk, err := seg.RegisterBlock(blkMeta)
		assert.Nil(t, err)
		blk.UnRef()
	}
	return seg
}

func makeSegments(fsMgr ldio.IManager, mtBufMgr, sstBufMgr mgrif.IBufferManager, meta *md.Table, t *testing.T) []IColumnSegment {
	var segs []IColumnSegment
	var rootSeg IColumnSegment
	var prevSeg IColumnSegment
	for _, segMeta := range meta.Segments {
		seg := makeSegment(fsMgr, mtBufMgr, sstBufMgr, 0, segMeta, t)
		segs = append(segs, seg)
		if prevSeg != nil {
			prevSeg.SetNext(seg.Ref())
		}
		if rootSeg == nil {
			rootSeg = seg
		}
		prevSeg = seg
	}
	return segs
}

func TestUpgradeStdSegment(t *testing.T) {
	seg_cnt := uint64(5)
	blkRows := uint64(10)
	blks := uint64(4)
	info := md.MockInfo(blkRows, blks)
	schema := md.MockSchema(1)
	meta := md.MockTable(info, schema, seg_cnt*blks)

	typeSize := uint64(schema.ColDefs[0].Type.Size)
	capacity := typeSize * info.Conf.BlockMaxRows * 10000
	mtBufMgr := bmgr.MockBufMgr(capacity)
	sstBufMgr := bmgr.MockBufMgr(capacity)
	fsMgr := ldio.MockFsMgr
	segs := makeSegments(fsMgr, mtBufMgr, sstBufMgr, meta, t)
	rootSeg := segs[0].Ref()

	// pools := 1
	// var savedStrings []*[]string
	// var wg sync.WaitGroup
	// for i := 0; i < pools; i++ {
	// 	wg.Add(1)
	// 	var strings []string
	// 	savedStrings = append(savedStrings, &strings)
	// 	go func(wgp *sync.WaitGroup, strs *[]string) {
	// 		defer wgp.Done()
	// 		cursor := ScanCursor{CurrSeg: rootSeg}
	// 		err := rootSeg.InitScanCursor(&cursor)
	// 		assert.Nil(t, err)
	// 		cursor.Init()
	// 		cnt := 0
	// 		// prevType := TRANSIENT_BLK
	// 		for cursor.Current != nil {
	// 			cnt += 1
	// 			err = cursor.Init()
	// 			assert.Nil(t, err)
	// 			cursor.Next()
	// 		}
	// 		assert.Equal(t, seg_cnt*blk_cnt, cnt)
	// 	}(&wg, &strings)
	// }

	// wg.Wait()
	assert.Equal(t, int(blks*seg_cnt), mtBufMgr.NodeCount())
	assert.Equal(t, 0, sstBufMgr.NodeCount())

	currSeg := rootSeg.Ref()
	for currSeg != nil {
		ids := currSeg.GetBlockIDs()
		segMeta := currSeg.GetMeta()
		for _, id := range ids {
			oldBlk := currSeg.GetBlock(id)
			assert.NotNil(t, oldBlk)
			assert.Equal(t, TRANSIENT_BLK, oldBlk.GetBlockType())
			blkMeta, err := segMeta.ReferenceBlock(oldBlk.GetID().BlockID)
			assert.Nil(t, err)
			newMeta := blkMeta.Copy()
			assert.Equal(t, md.EMPTY, blkMeta.DataState)
			newMeta.DataState = md.FULL
			oldBlk.UnRef()
			blk, err := currSeg.UpgradeBlock(newMeta)
			assert.Nil(t, err)
			assert.Equal(t, PERSISTENT_BLK, blk.GetBlockType())
			blk.UnRef()
		}
		currSeg.UnRef()
		currSeg = currSeg.GetNext()
	}

	currSeg = rootSeg.Ref()
	for currSeg != nil {
		ids := currSeg.GetBlockIDs()
		segMeta := currSeg.GetMeta()
		for _, id := range ids {
			oldBlk := currSeg.GetBlock(id)
			assert.NotNil(t, oldBlk)
			assert.Equal(t, PERSISTENT_BLK, oldBlk.GetBlockType())
			blkMeta, err := segMeta.ReferenceBlock(oldBlk.GetID().BlockID)
			assert.Nil(t, err)
			newMeta := blkMeta.Copy()
			oldBlk.UnRef()
			assert.Equal(t, md.EMPTY, blkMeta.DataState)
			newMeta.DataState = md.FULL
			blk, err := currSeg.UpgradeBlock(newMeta)
			assert.Nil(t, err)
			assert.Equal(t, PERSISTENT_SORTED_BLK, blk.GetBlockType())
			blk.UnRef()
		}
		currSeg.UnRef()
		currSeg = currSeg.GetNext()
	}
	currSeg = rootSeg.Ref()
	for currSeg != nil {
		currSeg.UnRef()
		currSeg = currSeg.GetNext()
	}
	rootSeg.UnRef()
	t.Log(mtBufMgr.String())
	t.Log(sstBufMgr.String())
	assert.Equal(t, 0, mtBufMgr.NodeCount())
	assert.Equal(t, int(blks*seg_cnt), sstBufMgr.NodeCount())

	for _, seg := range segs {
		seg.UnRef()
	}
}
