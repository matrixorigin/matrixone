package db

import (
	roaring "github.com/RoaringBitmap/roaring/roaring64"
	"github.com/stretchr/testify/assert"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
	table2 "matrixone/pkg/vm/engine/aoe/storage/layout/table/v2"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

func TestSegmentSparseFilterInt32(t *testing.T) {
	if !dataio.FlushIndex {
		return
	}
	mu := &sync.RWMutex{}
	rowCount, blkCount := uint64(10), uint64(4)
	info := md.MockInfo(mu, rowCount, blkCount)
	schema := md.MockSchema(2)
	segCnt, blkCnt := uint64(4), uint64(4)
	table := md.MockTable(info, schema, segCnt*blkCnt)
	segment, err := table.CreateSegment()
	assert.Nil(t, err)
	err = table.RegisterSegment(segment)
	assert.Nil(t, err)
	batches := make([]*batch.Batch, 0)
	blkIds := make([]uint64, 0)
	for i := 0; i < int(blkCount); i++ {
		block, err := segment.CreateBlock()
		assert.Nil(t, err)
		blkIds = append(blkIds, block.ID)
		block.SetCount(rowCount)
		err = segment.RegisterBlock(block)
		assert.Nil(t, err)
		batches = append(batches, chunk.MockBatch(schema.Types(), rowCount))
	}
	path := "/tmp/testwriter"
	writer := dataio.NewSegmentWriter(batches, segment, path)
	err = writer.Execute()
	assert.Nil(t, err)
	// name := writer.GetFileName()
	segFile := dataio.NewSortedSegmentFile(path, *segment.AsCommonID())
	assert.NotNil(t, segFile)
	tblHolder := index.NewTableHolder(bmgr.MockBufMgr(1000), table.ID)
	segHolder := tblHolder.RegisterSegment(*segment.AsCommonID(), base.SORTED_SEG, nil)
	segHolder.Unref()
	id := common.ID{}
	for i := 0; i < int(blkCount); i++ {
		id.BlockID = uint64(i)
		blkHolder := segHolder.RegisterBlock(id, base.PERSISTENT_BLK, nil)
		blkHolder.Unref()
		blkHolder.Init(segFile)
	}
	segHolder.Init(segFile)
	t.Log(tblHolder.String())
	t.Log(segHolder.CollectMinMax(0))
	t.Log(segHolder.CollectMinMax(1))
	t.Log(segHolder.GetBlockCount())
	seg := &table2.Segment{
		RefHelper:   common.RefHelper{},
		Type:        base.SORTED_SEG,
		Meta:        segment,
		IndexHolder: segHolder,
		SegmentFile: segFile,
	}
	s := &Segment{
		Data: seg,
		Ids:  new(atomic.Value),
	}
	ids := blkIds
	strs := make([]string, len(ids))
	for idx, id := range ids {
		strs[idx] = strconv.FormatUint(id, 10)
	}
	s.Ids.Store(strs)
	filter := NewSegmentSparseFilter(s)
	t.Log(s.Data.GetSegmentFile().GetIndicesMeta())
	t.Log(filter.(*SegmentSparseFilter).segment.Data.GetIndexHolder().CollectMinMax(0))
	res, _ := filter.Eq("mock_0", int32(-1))
	assert.Equal(t, res, []string{})
	res, _ = filter.Ne("mock_0", int32(-1))
	assert.Equal(t, res, []string{"17", "18", "19", "20"})
	res, _ = filter.Btw("mock_0", int32(1), int32(7))
	assert.Equal(t, res, []string{"17", "18", "19", "20"})
	res, _ = filter.Btw("mock_0", int32(-1), int32(8))
	assert.Equal(t, res, []string{})
	res, _ = filter.Lt("mock_0", int32(0))
	assert.Equal(t, res, []string{})
	res, _ = filter.Gt("mock_0", int32(8))
	assert.Equal(t, res, []string{"17", "18", "19", "20"})
}


func TestSegmentSparseFilterVarchar(t *testing.T) {
	if !dataio.FlushIndex {
		return
	}
	mu := &sync.RWMutex{}
	rowCount, blkCount := uint64(10), uint64(4)
	info := md.MockInfo(mu, rowCount, blkCount)
	schema := md.MockVarCharSchema(2)
	segCnt, blkCnt := uint64(4), uint64(4)
	table := md.MockTable(info, schema, segCnt*blkCnt)
	segment, err := table.CreateSegment()
	assert.Nil(t, err)
	err = table.RegisterSegment(segment)
	assert.Nil(t, err)
	batches := make([]*batch.Batch, 0)
	blkIds := make([]uint64, 0)
	for i := 0; i < int(blkCount); i++ {
		block, err := segment.CreateBlock()
		assert.Nil(t, err)
		blkIds = append(blkIds, block.ID)
		block.SetCount(rowCount)
		err = segment.RegisterBlock(block)
		assert.Nil(t, err)
		batches = append(batches, chunk.MockBatch(schema.Types(), rowCount))
		t.Log(batches[i])
	}
	path := "/tmp/testwriter"
	writer := dataio.NewSegmentWriter(batches, segment, path)
	err = writer.Execute()
	assert.Nil(t, err)
	// name := writer.GetFileName()
	segFile := dataio.NewSortedSegmentFile(path, *segment.AsCommonID())
	assert.NotNil(t, segFile)
	tblHolder := index.NewTableHolder(bmgr.MockBufMgr(1000), table.ID)
	segHolder := tblHolder.RegisterSegment(*segment.AsCommonID(), base.SORTED_SEG, nil)
	segHolder.Unref()
	id := common.ID{}
	for i := 0; i < int(blkCount); i++ {
		id.BlockID = uint64(i)
		blkHolder := segHolder.RegisterBlock(id, base.PERSISTENT_BLK, nil)
		blkHolder.Unref()
		blkHolder.Init(segFile)
	}
	segHolder.Init(segFile)
	t.Log(tblHolder.String())
	t.Log(segHolder.CollectMinMax(0))
	t.Log(segHolder.CollectMinMax(1))
	t.Log(segHolder.GetBlockCount())
	seg := &table2.Segment{
		RefHelper:   common.RefHelper{},
		Type:        base.SORTED_SEG,
		Meta:        segment,
		IndexHolder: segHolder,
		SegmentFile: segFile,
	}
	s := &Segment{
		Data: seg,
		Ids:  new(atomic.Value),
	}
	ids := blkIds
	strs := make([]string, len(ids))
	for idx, id := range ids {
		strs[idx] = strconv.FormatUint(id, 10)
	}
	s.Ids.Store(strs)
	filter := NewSegmentSparseFilter(s)
	t.Log(s.Data.GetSegmentFile().GetIndicesMeta())
	t.Log(filter.(*SegmentSparseFilter).segment.Data.GetIndexHolder().CollectMinMax(0))
	res, _ := filter.Eq("mock_0", []byte("str/"))
	assert.Equal(t, res, []string{})
	res, _ = filter.Ne("mock_0", []byte("str/"))
	assert.Equal(t, res, []string{"17", "18", "19", "20"})
	res, _ = filter.Btw("mock_0", []byte("str1"), []byte("str8"))
	assert.Equal(t, res, []string{"17", "18", "19", "20"})
	res, _ = filter.Btw("mock_0", []byte("str/"), []byte("str8"))
	assert.Equal(t, res, []string{})
	res, _ = filter.Lt("mock_0", []byte("str0"))
	assert.Equal(t, res, []string{})
	res, _ = filter.Gt("mock_0", []byte("str8"))
	assert.Equal(t, res, []string{"17", "18", "19", "20"})
}


func TestSegmentSparseFilterDate(t *testing.T) {
	if !dataio.FlushIndex {
		return
	}
	mu := &sync.RWMutex{}
	rowCount, blkCount := uint64(10), uint64(4)
	info := md.MockInfo(mu, rowCount, blkCount)
	schema := md.MockDateSchema(2)
	segCnt, blkCnt := uint64(4), uint64(4)
	table := md.MockTable(info, schema, segCnt*blkCnt)
	segment, err := table.CreateSegment()
	assert.Nil(t, err)
	err = table.RegisterSegment(segment)
	assert.Nil(t, err)
	batches := make([]*batch.Batch, 0)
	blkIds := make([]uint64, 0)
	for i := 0; i < int(blkCount); i++ {
		block, err := segment.CreateBlock()
		assert.Nil(t, err)
		blkIds = append(blkIds, block.ID)
		block.SetCount(rowCount)
		err = segment.RegisterBlock(block)
		assert.Nil(t, err)
		batches = append(batches, chunk.MockBatch(schema.Types(), rowCount))
		t.Log(batches[i])
	}
	path := "/tmp/testwriter"
	writer := dataio.NewSegmentWriter(batches, segment, path)
	err = writer.Execute()
	assert.Nil(t, err)
	// name := writer.GetFileName()
	segFile := dataio.NewSortedSegmentFile(path, *segment.AsCommonID())
	assert.NotNil(t, segFile)
	tblHolder := index.NewTableHolder(bmgr.MockBufMgr(10000), table.ID)
	segHolder := tblHolder.RegisterSegment(*segment.AsCommonID(), base.SORTED_SEG, nil)
	segHolder.Unref()
	id := common.ID{}
	for i := 0; i < int(blkCount); i++ {
		id.BlockID = uint64(i)
		blkHolder := segHolder.RegisterBlock(id, base.PERSISTENT_BLK, nil)
		blkHolder.Unref()
		blkHolder.Init(segFile)
	}
	segHolder.Init(segFile)
	//t.Log(tblHolder.String())
	//t.Log(segHolder.CollectMinMax(0))
	//t.Log(segHolder.CollectMinMax(1))
	//t.Log(segHolder.GetBlockCount())
	seg := &table2.Segment{
		RefHelper:   common.RefHelper{},
		Type:        base.SORTED_SEG,
		Meta:        segment,
		IndexHolder: segHolder,
		SegmentFile: segFile,
	}
	s := &Segment{
		Data: seg,
		Ids:  new(atomic.Value),
	}
	ids := blkIds
	strs := make([]string, len(ids))
	for idx, id := range ids {
		strs[idx] = strconv.FormatUint(id, 10)
	}
	s.Ids.Store(strs)
	filter := NewSegmentSparseFilter(s)
	//t.Log(s.Data.GetSegmentFile().GetIndicesMeta())
	//t.Log(filter.(*SegmentSparseFilter).segment.Data.GetIndexHolder().CollectMinMax(0))
	res, _ := filter.Eq("mock_0", types.FromCalendar(0, 1, 1))
	assert.Equal(t, res, []string{})
	res, _ = filter.Ne("mock_0", types.FromCalendar(0, 1, 1))
	assert.Equal(t, res, []string{"17", "18", "19", "20"})
	res, _ = filter.Btw("mock_0", types.FromCalendar(100, 1, 1), types.FromCalendar(300, 1, 1))
	assert.Equal(t, res, []string{"17", "18", "19", "20"})
	res, _ = filter.Btw("mock_0", types.FromCalendar(0, 1, 1), types.FromCalendar(100, 1, 1))
	assert.Equal(t, res, []string{})
	res, _ = filter.Lt("mock_1", types.FromClock(300, 1, 1, 1, 1, 1, 1))
	assert.Equal(t, res, []string{"17", "18", "19", "20"})
	res, _ = filter.Gt("mock_1", types.FromClock(1000, 1, 1, 1, 1, 1, 1))
	assert.Equal(t, res, []string{})
}


func TestSegmentFilterInt32(t *testing.T) {
	if !dataio.FlushIndex {
		return
	}
	mu := &sync.RWMutex{}
	rowCount, blkCount := uint64(10), uint64(4)
	info := md.MockInfo(mu, rowCount, blkCount)
	schema := md.MockSchema(2)
	segCnt, blkCnt := uint64(4), uint64(4)
	table := md.MockTable(info, schema, segCnt*blkCnt)
	segment, err := table.CreateSegment()
	assert.Nil(t, err)
	err = table.RegisterSegment(segment)
	assert.Nil(t, err)
	batches := make([]*batch.Batch, 0)
	blkIds := make([]uint64, 0)
	for i := 0; i < int(blkCount); i++ {
		block, err := segment.CreateBlock()
		assert.Nil(t, err)
		blkIds = append(blkIds, block.ID)
		block.SetCount(rowCount)
		err = segment.RegisterBlock(block)
		assert.Nil(t, err)
		batches = append(batches, chunk.MockBatch(schema.Types(), rowCount))
	}
	path := "/tmp/testwriter"
	writer := dataio.NewSegmentWriter(batches, segment, path)
	err = writer.Execute()
	assert.Nil(t, err)
	// name := writer.GetFileName()
	segFile := dataio.NewSortedSegmentFile(path, *segment.AsCommonID())
	assert.NotNil(t, segFile)
	tblHolder := index.NewTableHolder(bmgr.MockBufMgr(1000), table.ID)
	segHolder := tblHolder.RegisterSegment(*segment.AsCommonID(), base.SORTED_SEG, nil)
	segHolder.Unref()
	id := common.ID{}
	for i := 0; i < int(blkCount); i++ {
		id.BlockID = uint64(i)
		blkHolder := segHolder.RegisterBlock(id, base.PERSISTENT_BLK, nil)
		blkHolder.Unref()
		blkHolder.Init(segFile)
	}
	segHolder.Init(segFile)
	t.Log(tblHolder.String())
	t.Log(segHolder.GetBlockCount())
	seg := &table2.Segment{
		RefHelper:   common.RefHelper{},
		Type:        base.SORTED_SEG,
		Meta:        segment,
		IndexHolder: segHolder,
		SegmentFile: segFile,
	}
	s := &Segment{
		Data: seg,
		Ids:  new(atomic.Value),
	}
	ids := blkIds
	strs := make([]string, len(ids))
	for idx, id := range ids {
		strs[idx] = strconv.FormatUint(id, 10)
	}
	s.Ids.Store(strs)
	filter := NewSegmentFilter(s)
	mockBM := roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	t.Log(s.Data.GetSegmentFile().GetIndicesMeta())
	t.Log(filter.(*SegmentFilter).segment.Data.GetIndexHolder().CollectMinMax(0))
	res, _ := filter.Eq("mock_0", int32(-1))
	assert.Equal(t, true, roaring.NewBitmap().Equals(res))
	res, _ = filter.Eq("mock_0", int32(3))
	//t.Log(res)
	mockBM = roaring.NewBitmap()
	mockBM.Add(3)
	mockBM.Add(13)
	mockBM.Add(23)
	mockBM.Add(33)
	mockBM.Xor(res)
	assert.Equal(t, true, mockBM.IsEmpty())
	res, _ = filter.Gt("mock_0", int32(4))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(5, 10)
	mockBM.AddRange(15, 20)
	mockBM.AddRange(25, 30)
	mockBM.AddRange(35, 40)
	mockBM.Xor(res)
	assert.Equal(t, true, mockBM.IsEmpty())
	res, _ = filter.Le("mock_0", int32(3))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 4)
	mockBM.AddRange(10, 14)
	mockBM.AddRange(20, 24)
	mockBM.AddRange(30, 34)
	//t.Log(res)
	//t.Log(mockBM)
	mockBM.Xor(res)
	assert.Equal(t, true, mockBM.IsEmpty())
	res, _ = filter.Btw("mock_0", int32(3), int32(8))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(3, 9)
	mockBM.AddRange(13, 19)
	mockBM.AddRange(23, 29)
	mockBM.AddRange(33, 39)
	mockBM.Xor(res)
	assert.Equal(t, true, mockBM.IsEmpty())
}

func TestSummarizerInt32(t *testing.T) {
	if !dataio.FlushIndex {
		return
	}
	mu := &sync.RWMutex{}
	rowCount, blkCount := uint64(10), uint64(4)
	info := md.MockInfo(mu, rowCount, blkCount)
	schema := md.MockSchema(2)
	segCnt, blkCnt := uint64(4), uint64(4)
	table := md.MockTable(info, schema, segCnt*blkCnt)
	segment, err := table.CreateSegment()
	assert.Nil(t, err)
	err = table.RegisterSegment(segment)
	assert.Nil(t, err)
	batches := make([]*batch.Batch, 0)
	blkIds := make([]uint64, 0)
	for i := 0; i < int(blkCount); i++ {
		block, err := segment.CreateBlock()
		assert.Nil(t, err)
		blkIds = append(blkIds, block.ID)
		block.SetCount(rowCount)
		err = segment.RegisterBlock(block)
		assert.Nil(t, err)
		batches = append(batches, chunk.MockBatch(schema.Types(), rowCount))
	}
	path := "/tmp/testwriter"
	writer := dataio.NewSegmentWriter(batches, segment, path)
	err = writer.Execute()
	assert.Nil(t, err)
	// name := writer.GetFileName()
	segFile := dataio.NewSortedSegmentFile(path, *segment.AsCommonID())
	assert.NotNil(t, segFile)
	tblHolder := index.NewTableHolder(bmgr.MockBufMgr(1000), table.ID)
	segHolder := tblHolder.RegisterSegment(*segment.AsCommonID(), base.SORTED_SEG, nil)
	segHolder.Unref()
	id := common.ID{}
	for i := 0; i < int(blkCount); i++ {
		id.BlockID = uint64(i)
		blkHolder := segHolder.RegisterBlock(id, base.PERSISTENT_BLK, nil)
		blkHolder.Unref()
		blkHolder.Init(segFile)
	}
	segHolder.Init(segFile)
	t.Log(tblHolder.String())
	t.Log(segHolder.GetBlockCount())
	seg := &table2.Segment{
		RefHelper:   common.RefHelper{},
		Type:        base.SORTED_SEG,
		Meta:        segment,
		IndexHolder: segHolder,
		SegmentFile: segFile,
	}
	s := &Segment{
		Data: seg,
		Ids:  new(atomic.Value),
	}
	ids := blkIds
	strs := make([]string, len(ids))
	for idx, id := range ids {
		strs[idx] = strconv.FormatUint(id, 10)
	}
	s.Ids.Store(strs)
	filter := NewSegmentSummarizer(s)
	mockBM := roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	sum, cnt, err := filter.Sum("mock_0", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, sum, int64(45*4))
	assert.Equal(t, cnt, uint64(40))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(3, 7)
	min, err := filter.Min("mock_0", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, min, int32(3))
	max, err := filter.Max("mock_0", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, max, int32(6))
	nullCnt, err := filter.NullCount("mock_0", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, nullCnt, uint64(0))
	cnt, err = filter.Count("mock_0", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(4))
}

