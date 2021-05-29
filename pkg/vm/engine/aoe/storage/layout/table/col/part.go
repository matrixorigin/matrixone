package col

import (
	"errors"
	"fmt"
	"io"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	nif "matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"runtime"
	"sync"

	log "github.com/sirupsen/logrus"
)

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
	GetID() layout.ID
	// GetBlock() IColumnBlock
	GetBuf() []byte
	GetColIdx() int
	CloneWithUpgrade(blk IColumnBlock) IColumnPart
	GetNodeID() layout.ID
}

type ColumnPart struct {
	sync.RWMutex
	ID          layout.ID
	Next        IColumnPart
	BufMgr      bmgrif.IBufferManager
	BufNode     nif.INodeHandle
	TypeSize    uint64
	MaxRowCount uint64
	RowCount    uint64
	Size        uint64
	Capacity    uint64
	NodeID      layout.ID
	BlockType   BlockType
	ColIdx      int
}

func NewColumnPart(bmgr bmgrif.IBufferManager, blk IColumnBlock, id layout.ID,
	rowCount uint64, typeSize uint64) IColumnPart {
	part := &ColumnPart{
		BufMgr:      bmgr,
		ID:          id,
		TypeSize:    typeSize,
		MaxRowCount: rowCount,
		NodeID:      id,
		ColIdx:      blk.GetColIdx(),
	}

	switch blk.GetBlockType() {
	case TRANSIENT_BLK:
		bNode := bmgr.RegisterSpillableNode(typeSize*rowCount, id)
		if bNode == nil {
			return nil
		}
		part.BufNode = bNode
	case PERSISTENT_BLK:
		csf := ldio.MockColSegmentFile{}
		part.BufNode = bmgr.RegisterNode(typeSize*rowCount, id, &csf)
		// sf := ldio.NewUnsortedSegmentFile(dio.READER_FACTORY.Dirname, id.AsSegmentID())
		// csf := ldio.ColSegmentFile{
		// 	SegmentFile: sf,
		// 	ColIdx:      uint64(part.Block.GetColIdx()),
		// }
		// part.BufNode = bmgr.RegisterNode(typeSize*rowCount, id, &csf)
	case PERSISTENT_SORTED_BLK:
		csf := ldio.MockColSegmentFile{}
		part.BufNode = bmgr.RegisterNode(typeSize*rowCount, id, &csf)
		// sf := ldio.NewSortedSegmentFile(dio.READER_FACTORY.Dirname, id.AsSegmentID())
		// csf := ldio.ColSegmentFile{
		// 	SegmentFile: sf,
		// 	ColIdx:      uint64(part.Block.GetColIdx()),
		// }
		// part.BufNode = bmgr.RegisterNode(typeSize*rowCount, id, &csf)
	case MOCK_BLK:
		csf := ldio.MockColSegmentFile{}
		part.BufNode = bmgr.RegisterNode(typeSize*rowCount, id, &csf)
	default:
		panic("not support")
	}
	runtime.SetFinalizer(part, func(p IColumnPart) {
		p.SetNext(nil)
		id := p.GetID()
		log.Infof("GC ColumnPart %s", id.String())
		p.Close()
	})

	blk.Append(part)
	return part
}

func (part *ColumnPart) GetNodeID() layout.ID {
	return part.NodeID
}

func (part *ColumnPart) CloneWithUpgrade(blk IColumnBlock) IColumnPart {
	cloned := &ColumnPart{
		ID:          part.ID,
		BufMgr:      part.BufMgr,
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
		csf := ldio.MockColSegmentFile{}
		cloned.BufNode = part.BufMgr.RegisterNode(part.TypeSize*part.MaxRowCount, cloned.NodeID, &csf)
		cloned.BlockType = PERSISTENT_BLK
		// sf := ldio.NewUnsortedSegmentFile(dio.READER_FACTORY.Dirname, part.ID.AsSegmentID())
		// csf := ldio.ColSegmentFile{
		// 	SegmentFile: sf,
		// 	ColIdx:      uint64(part.Block.GetColIdx()),
		// }
		// cloned.BufNode = part.BufMgr.RegisterNode(part.MaxRowCount*part.TypeSize, part.ID, &csf)
	case PERSISTENT_BLK:
		csf := ldio.MockColSegmentFile{}
		cloned.BufNode = part.BufMgr.RegisterNode(part.TypeSize*part.MaxRowCount, cloned.NodeID, &csf)
		cloned.BlockType = PERSISTENT_SORTED_BLK
		// sf := ldio.NewSortedSegmentFile(dio.READER_FACTORY.Dirname, part.ID.AsSegmentID())
		// csf := ldio.ColSegmentFile{
		// 	SegmentFile: sf,
		// 	ColIdx:      uint64(part.Block.GetColIdx()),
		// }
		// cloned.BufNode = part.BufMgr.RegisterNode(part.MaxRowCount*part.TypeSize, part.ID, &csf)
	case PERSISTENT_SORTED_BLK:
		panic("logic error")
	default:
		panic("not supported")
	}

	// cloned.Next = part.Next
	runtime.SetFinalizer(cloned, func(p IColumnPart) {
		id := p.GetID()
		log.Infof("GC ColumnPart %s", id.String())
		p.SetNext(nil)
		p.Close()
	})
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

func (part *ColumnPart) GetID() layout.ID {
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
