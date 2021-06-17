package col

import (
	"errors"
	"fmt"
	"io"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	nif "matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"sync"
	// log "github.com/sirupsen/logrus"
)

func initUnsortedBlkNode(part *ColumnPart, fsMgr ldio.IManager) {
	part.VFile = fsMgr.GetUnsortedFile(part.ID.AsSegmentID()).MakeVirtualPartFile(&part.ID)
	part.VFile.Ref()
	part.BufNode = part.BufMgr.RegisterNode(part.Capacity, part.NodeID, part.VFile, buf.RawMemoryNodeConstructor)
}

func initSortedBlkNode(part *ColumnPart, fsMgr ldio.IManager) {
	part.VFile = fsMgr.GetSortedFile(part.ID.AsSegmentID()).MakeVirtualPartFile(&part.ID)
	part.VFile.Ref()
	part.BufNode = part.BufMgr.RegisterNode(part.Capacity, part.NodeID, part.VFile, buf.RawMemoryNodeConstructor)
}

type IColumnPart interface {
	io.Closer
	GetNext() IColumnPart
	SetNext(IColumnPart)
	InitScanCursor(cursor *ScanCursor) error
	GetID() common.ID
	GetDataNode() buf.IMemoryNode
	GetColIdx() int
	CloneWithUpgrade(IColumnBlock, bmgrif.IBufferManager, ldio.IManager) IColumnPart
	GetNodeID() uint64
}

type ColumnPart struct {
	sync.RWMutex
	ID       common.ID
	Next     IColumnPart
	BufMgr   bmgrif.IBufferManager
	BufNode  nif.INodeHandle
	Size     uint64
	Capacity uint64
	NodeID   uint64
	VFile    base.IVirtaulFile
}

func NewColumnPart(fsMgr ldio.IManager, bmgr bmgrif.IBufferManager, blk IColumnBlock, id common.ID,
	capacity uint64) IColumnPart {
	defer blk.UnRef()
	part := &ColumnPart{
		BufMgr:   bmgr,
		ID:       id,
		NodeID:   bmgr.GetNextID(),
		Capacity: capacity,
	}

	switch blk.GetBlockType() {
	case base.TRANSIENT_BLK:
		bNode := bmgr.RegisterSpillableNode(capacity, part.NodeID, buf.RawMemoryNodeConstructor)
		if bNode == nil {
			return nil
		}
		part.BufNode = bNode
	case base.PERSISTENT_BLK:
		initUnsortedBlkNode(part, fsMgr)
	case base.PERSISTENT_SORTED_BLK:
		initSortedBlkNode(part, fsMgr)
	default:
		panic("not support")
	}

	blk.Append(part)
	return part
}

func (part *ColumnPart) GetNodeID() uint64 {
	return part.NodeID
}

func (part *ColumnPart) CloneWithUpgrade(blk IColumnBlock, sstBufMgr bmgrif.IBufferManager, fsMgr ldio.IManager) IColumnPart {
	cloned := &ColumnPart{
		ID:       part.ID,
		BufMgr:   sstBufMgr,
		Size:     part.Size,
		Capacity: part.Capacity,
		NodeID:   sstBufMgr.GetNextID(),
	}
	switch blk.GetBlockType() {
	case base.TRANSIENT_BLK:
		panic("logic error")
	case base.PERSISTENT_BLK:
		initUnsortedBlkNode(cloned, fsMgr)
	case base.PERSISTENT_SORTED_BLK:
		initSortedBlkNode(cloned, fsMgr)
	default:
		panic("not supported")
	}

	blk.UnRef()
	return cloned
}

func (part *ColumnPart) GetColIdx() int {
	return int(part.ID.Idx)
}

func (part *ColumnPart) GetDataNode() buf.IMemoryNode {
	return part.BufNode.GetBuffer().GetDataNode()
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
	if part.VFile != nil {
		part.VFile.Unref()
	}
	return nil
}

func (part *ColumnPart) InitScanCursor(cursor *ScanCursor) error {
	cursor.Handle = part.BufMgr.Pin(part.BufNode)
	for cursor.Handle == nil {
		cursor.Handle = part.BufMgr.Pin(part.BufNode)
	}
	if cursor.Handle == nil {
		return errors.New(fmt.Sprintf("Cannot pin part %v", part.ID))
	}
	return nil
}
