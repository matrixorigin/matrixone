package table

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
	mock "matrixone/pkg/vm/engine/aoe/storage/mock/type"
	"runtime"
	"sync"
)

type ITableData interface {
	sync.Locker
	GetRowCount() uint64
	GetID() uint64
	GetCollumns() []col.IColumnData
	GetColTypeSize(idx int) uint64
	GetColTypes() []mock.ColType
	GetBufMgr() bmgrif.IBufferManager
	GetSegmentCount() uint64

	UpgradeBlock(blkID layout.ID) (blks []col.IColumnBlock)
	UpgradeSegment(segID layout.ID) (segs []col.IColumnSegment)
	AppendColSegments(colSegs []col.IColumnSegment)
	// Scan()
}

// type IColumnDef interface {
// 	GetType() interface{}
// 	TypeSize() uint64
// }

// type MockColumnDef struct {
// }

// func (c *MockColumnDef) GetType() interface{} {
// 	return nil
// }

// func (c *MockColumnDef) TypeSize() uint64 {
// 	return uint64(4)
// }

func NewTableData(bufMgr bmgrif.IBufferManager, id uint64, colTypes []mock.ColType) ITableData {
	data := &TableData{
		ID:      id,
		Columns: make([]col.IColumnData, 0),
		ColType: colTypes,
		BufMgr:  bufMgr,
	}
	for idx, colType := range colTypes {
		data.Columns = append(data.Columns, col.NewColumnData(colType, idx))
	}
	runtime.SetFinalizer(data, func(o ITableData) {
		id := o.GetID()
		log.Infof("[GC]: TableData: %d", id)
	})
	return data
}

type TableData struct {
	sync.Mutex
	ID       uint64
	RowCount uint64
	Columns  []col.IColumnData
	ColType  []mock.ColType
	BufMgr   bmgrif.IBufferManager
}

func (td *TableData) GetRowCount() uint64 {
	return td.RowCount
}

func (td *TableData) GetID() uint64 {
	return td.ID
}

func (td *TableData) GetColTypes() []mock.ColType {
	return td.ColType
}

func (td *TableData) GetCollumns() []col.IColumnData {
	return td.Columns
}

func (td *TableData) GetColTypeSize(idx int) uint64 {
	return td.ColType[idx].Size()
}

func (td *TableData) GetBufMgr() bmgrif.IBufferManager {
	return td.BufMgr
}

func (td *TableData) GetSegmentCount() uint64 {
	return td.Columns[0].SegmentCount()
}

func (td *TableData) UpgradeBlock(blkID layout.ID) (blks []col.IColumnBlock) {
	for _, column := range td.Columns {
		blk := column.UpgradeBlock(blkID)
		blks = append(blks, blk)
	}
	return blks
}

func (td *TableData) UpgradeSegment(segID layout.ID) (segs []col.IColumnSegment) {
	for _, column := range td.Columns {
		seg := column.UpgradeSegment(segID)
		segs = append(segs, seg)
	}
	return segs
}

// Only be called at engine startup.
func (td *TableData) AppendColSegments(colSegs []col.IColumnSegment) {
	if len(colSegs) != len(td.ColType) {
		panic("logic error")
	}
	for idx, column := range td.Columns {
		if idx != column.GetColIdx() {
			panic("logic error")
		}
		err := column.Append(colSegs[idx])
		if err != nil {
			panic(fmt.Sprintf("logic error: %s", err))
		}
	}
}
