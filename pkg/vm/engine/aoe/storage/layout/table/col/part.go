package col

import (
	"errors"
	"fmt"
	"io"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	nif "matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	dio "matrixone/pkg/vm/engine/aoe/storage/dataio"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"sync"
	// "runtime"
	// log "github.com/sirupsen/logrus"
)

var (
	initUBN = initUnsortedBlkNode
	initBN  = initSortedBlkNode
)

func init() {
}

func initUnsortedBlkNode(part *ColumnPart) {
	sf := ldio.NewUnsortedSegmentFile(dio.READER_FACTORY.Dirname, part.ID.AsSegmentID())
	csf := ldio.ColSegmentFile{
		SegmentFile: sf,
		ColIdx:      uint64(part.ColIdx),
	}
	bf := ldio.NewBlockFile(dio.READER_FACTORY.Dirname, part.ID.AsBlockID())
	sf.(*ldio.UnsortedSegmentFile).AddBlock(part.ID.AsBlockID(), bf)
	part.BufNode = part.BufMgr.RegisterNode(part.Capacity, part.NodeID, &csf)
}

func initMockUnsortedBlkNode(part *ColumnPart) {
	csf := ldio.MockColSegmentFile{}
	part.BufNode = part.BufMgr.RegisterNode(part.Capacity, part.NodeID, &csf)
}

func initSortedBlkNode(part *ColumnPart) {
	sf := ldio.NewSortedSegmentFile(dio.READER_FACTORY.Dirname, part.ID.AsSegmentID())
	csf := ldio.ColSegmentFile{
		SegmentFile: sf,
		ColIdx:      uint64(part.ColIdx),
	}
	part.BufNode = part.BufMgr.RegisterNode(part.Capacity, part.NodeID, &csf)
}

type ColumnPartAllocator struct {
}

func (alloc *ColumnPartAllocator) Malloc() (buf []byte, err error) {
	return buf, err
}

type IColumnPart interface {
	io.Closer
	GetNext() IColumnPart
	SetNext(IColumnPart)
	InitScanCursor(cursor *ScanCursor) error
	GetID() common.ID
	// GetBlock() IColumnBlock
	GetBuf() []byte
	GetColIdx() int
	CloneWithUpgrade(IColumnBlock, bmgrif.IBufferManager) IColumnPart
	GetNodeID() common.ID
}

type ColumnPart struct {
	sync.RWMutex
	ID          common.ID
	Next        IColumnPart
	BufMgr      bmgrif.IBufferManager
	BufNode     nif.INodeHandle
	TypeSize    uint64
	MaxRowCount uint64
	RowCount    uint64
	Size        uint64
	Capacity    uint64
	NodeID      common.ID
	BlockType   BlockType
	ColIdx      int
}

func NewColumnPart(bmgr bmgrif.IBufferManager, blk IColumnBlock, id common.ID,
	rowCount uint64, typeSize uint64) IColumnPart {
	defer blk.UnRef()
	part := &ColumnPart{
		BufMgr:      bmgr,
		ID:          id,
		TypeSize:    typeSize,
		MaxRowCount: rowCount,
		NodeID:      id,
		BlockType:   blk.GetBlockType(),
		ColIdx:      blk.GetColIdx(),
		Capacity:    typeSize * rowCount,
	}
	part.NodeID.Idx = uint16(blk.GetColIdx())

	switch blk.GetBlockType() {
	case TRANSIENT_BLK:
		bNode := bmgr.RegisterSpillableNode(typeSize*rowCount, part.NodeID)
		if bNode == nil {
			return nil
		}
		part.BufNode = bNode
	case PERSISTENT_BLK:
		initUBN(part)
	case PERSISTENT_SORTED_BLK:
		initBN(part)
	case MOCK_BLK:
		csf := ldio.MockColSegmentFile{}
		part.BufNode = bmgr.RegisterNode(typeSize*rowCount, part.NodeID, &csf)
	default:
		panic("not support")
	}

	blk.Append(part)
	return part
}

func (part *ColumnPart) GetNodeID() common.ID {
	return part.NodeID
}

func (part *ColumnPart) CloneWithUpgrade(blk IColumnBlock, sstBufMgr bmgrif.IBufferManager) IColumnPart {
	cloned := &ColumnPart{
		ID:          part.ID,
		BufMgr:      sstBufMgr,
		TypeSize:    part.TypeSize,
		MaxRowCount: part.MaxRowCount,
		RowCount:    part.RowCount,
		Size:        part.Size,
		Capacity:    part.Capacity,
		NodeID:      part.NodeID.NextIter(),
		ColIdx:      part.GetColIdx(),
	}
	switch part.BlockType {
	case TRANSIENT_BLK:
		// csf := ldio.MockColSegmentFile{}
		// cloned.BufNode = part.BufMgr.RegisterNode(part.TypeSize*part.MaxRowCount, cloned.NodeID, &csf)
		cloned.BlockType = PERSISTENT_BLK
		initMockUnsortedBlkNode(cloned)
		// initUBN(cloned)
	case PERSISTENT_BLK:
		initMockUnsortedBlkNode(cloned)
		cloned.BlockType = PERSISTENT_SORTED_BLK
		// sf := ldio.NewSortedSegmentFile(dio.READER_FACTORY.Dirname, part.ID.AsSegmentID())
		// csf := ldio.ColSegmentFile{
		// 	SegmentFile: sf,
		// 	ColIdx:      uint64(part.ColIdx),
		// }
		// cloned.BufNode = part.BufMgr.RegisterNode(part.MaxRowCount*part.TypeSize, cloned.NodeID, &csf)
	case PERSISTENT_SORTED_BLK:
		panic("logic error")
	case MOCK_BLK:
		csf := ldio.MockColSegmentFile{}
		cloned.BufNode = cloned.BufMgr.RegisterNode(part.TypeSize*part.MaxRowCount, cloned.NodeID, &csf)
		cloned.BlockType = MOCK_PERSISTENT_BLK
	case MOCK_PERSISTENT_BLK:
		csf := ldio.MockColSegmentFile{}
		cloned.BufNode = cloned.BufMgr.RegisterNode(part.TypeSize*part.MaxRowCount, cloned.NodeID, &csf)
		cloned.BlockType = MOCK_PERSISTENT_SORTED_BLK

	default:
		panic("not supported")
	}

	blk.UnRef()
	return cloned
}

func (part *ColumnPart) GetColIdx() int {
	return part.ColIdx
}

func (part *ColumnPart) GetBuf() []byte {
	part.RLock()
	defer part.RUnlock()
	return part.BufNode.GetBuffer().GetDataNode().Data
}

func (part *ColumnPart) SetRowCount(cnt uint64) {
	if cnt > part.MaxRowCount {
		panic("logic error")
	}
	part.Lock()
	defer part.Unlock()
	part.RowCount = cnt
}

func (part *ColumnPart) SetSize(size uint64) {
	if size > part.Capacity {
		panic("logic error")
	}
	part.Lock()
	defer part.Unlock()
	part.Size = size
}

func (part *ColumnPart) GetID() common.ID {
	return part.ID
}

func (part *ColumnPart) SetNext(next IColumnPart) {
	part.Lock()
	defer part.Unlock()
	part.Next = next
}

func (part *ColumnPart) GetNext() IColumnPart {
	part.RLock()
	defer part.RUnlock()
	return part.Next
}

func (part *ColumnPart) Close() error {
	if part.BufNode != nil {
		err := part.BufNode.Close()
		if err != nil {
			panic("logic error")
		}
		part.BufNode = nil
	}
	return nil
}

func (part *ColumnPart) InitScanCursor(cursor *ScanCursor) error {
	bufMgr := part.BufMgr
	part.RLock()
	bufNode := part.BufNode
	part.RUnlock()
	cursor.Handle = bufMgr.Pin(bufNode)
	if cursor.Handle == nil {
		return errors.New(fmt.Sprintf("Cannot pin part %v", part.ID))
	}
	// cursor.Inited = true
	return nil
}
