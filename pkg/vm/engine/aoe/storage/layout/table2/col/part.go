package col

// import (
// 	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
// 	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
// 	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
// 	"matrixone/pkg/vm/engine/aoe/storage/common"
// 	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
// 	"sync"
// 	// log "github.com/sirupsen/logrus"
// )

// type IColumnPart interface {
// 	bmgrif.INode
// 	GetNext() IColumnPart
// 	SetNext(IColumnPart)
// 	InitScanCursor(cursor *ScanCursor) error
// 	GetID() common.ID
// 	GetColIdx() int
// 	CloneWithUpgrade(IColumnBlock, bmgrif.IBufferManager, base.IManager) IColumnPart
// }

// type ColumnPart struct {
// 	sync.RWMutex
// 	*bmgr.Node
// 	ID   common.ID
// 	Next IColumnPart
// }

// func NewColumnPart(fsMgr base.IManager, bufMgr bmgrif.IBufferManager, blk IColumnBlock, id common.ID,
// 	capacity uint64) IColumnPart {
// 	defer blk.UnRef()
// 	part := &ColumnPart{ID: id}
// 	var vf bmgrif.IVFile
// 	switch blk.GetBlockType() {
// 	case base.TRANSIENT_BLK:
// 	case base.PERSISTENT_BLK:
// 		vf = fsMgr.GetUnsortedFile(part.ID.AsSegmentID()).MakeVirtualPartFile(&part.ID)
// 	case base.PERSISTENT_SORTED_BLK:
// 		vf = fsMgr.GetSortedFile(part.ID.AsSegmentID()).MakeVirtualPartFile(&part.ID)
// 	default:
// 		panic("not support")
// 	}
// 	part.Node = bufMgr.CreateNode(vf, buf.RawMemoryNodeConstructor, capacity).(*bmgr.Node)
// 	if part.Node == nil {
// 		return nil
// 	}

// 	blk.Append(part)
// 	return part
// }

// func (part *ColumnPart) CloneWithUpgrade(blk IColumnBlock, sstBufMgr bmgrif.IBufferManager, fsMgr base.IManager) IColumnPart {
// 	defer blk.UnRef()
// 	cloned := &ColumnPart{ID: part.ID}
// 	var vf bmgrif.IVFile
// 	switch blk.GetBlockType() {
// 	case base.TRANSIENT_BLK:
// 		panic("logic error")
// 	case base.PERSISTENT_BLK:
// 		vf = fsMgr.GetUnsortedFile(cloned.ID.AsSegmentID()).MakeVirtualPartFile(&cloned.ID)
// 	case base.PERSISTENT_SORTED_BLK:
// 		vf = fsMgr.GetSortedFile(cloned.ID.AsSegmentID()).MakeVirtualPartFile(&cloned.ID)
// 	default:
// 		panic("not supported")
// 	}
// 	cloned.Node = sstBufMgr.CreateNode(vf, buf.RawMemoryNodeConstructor, part.Capacity).(*bmgr.Node)

// 	return cloned
// }

// func (part *ColumnPart) GetColIdx() int {
// 	return int(part.ID.Idx)
// }

// func (part *ColumnPart) GetID() common.ID {
// 	return part.ID
// }

// func (part *ColumnPart) SetNext(next IColumnPart) {
// 	part.Lock()
// 	defer part.Unlock()
// 	part.Next = next
// }

// func (part *ColumnPart) GetNext() IColumnPart {
// 	part.RLock()
// 	defer part.RUnlock()
// 	return part.Next
// }

// func (part *ColumnPart) InitScanCursor(cursor *ScanCursor) error {
// 	cursor.Node = part.GetManagedNode()
// 	return nil
// }
