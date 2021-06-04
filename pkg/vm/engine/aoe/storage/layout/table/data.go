package table

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"matrixone/pkg/container/types"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
	"runtime"
	"sync"
)

type ITableData interface {
	sync.Locker
	GetRowCount() uint64
	GetID() uint64
	GetCollumns() []col.IColumnData
	GetCollumn(int) col.IColumnData
	GetColTypeSize(idx int) uint64
	GetColTypes() []types.Type
	GetBufMgr() bmgrif.IBufferManager
	GetSegmentCount() uint64

	UpgradeBlock(blkID common.ID) (blks []col.IColumnBlock)
	UpgradeSegment(segID common.ID) (segs []col.IColumnSegment)
	AppendColSegments(colSegs []col.IColumnSegment)
}

func NewTableData(bufMgr bmgrif.IBufferManager, id uint64, colTypes []types.Type) ITableData {
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
	ColType  []types.Type
	BufMgr   bmgrif.IBufferManager
}

func (td *TableData) GetRowCount() uint64 {
	return td.RowCount
}

func (td *TableData) GetID() uint64 {
	return td.ID
}

func (td *TableData) GetColTypes() []types.Type {
	return td.ColType
}

func (td *TableData) GetCollumn(idx int) col.IColumnData {
	if idx >= len(td.ColType) {
		panic("logic error")
	}
	return td.Columns[idx]
}

func (td *TableData) GetCollumns() []col.IColumnData {
	return td.Columns
}

func (td *TableData) GetColTypeSize(idx int) uint64 {
	return uint64(td.ColType[idx].Size)
}

func (td *TableData) GetBufMgr() bmgrif.IBufferManager {
	return td.BufMgr
}

func (td *TableData) GetSegmentCount() uint64 {
	return td.Columns[0].SegmentCount()
}

func (td *TableData) UpgradeBlock(blkID common.ID) (blks []col.IColumnBlock) {
	for _, column := range td.Columns {
		blk := column.UpgradeBlock(blkID)
		blks = append(blks, blk)
	}
	return blks
}

func (td *TableData) UpgradeSegment(segID common.ID) (segs []col.IColumnSegment) {
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

type Tables struct {
	sync.RWMutex
	Data      map[uint64]ITableData
	Ids       map[uint64]bool
	Tombstone map[uint64]ITableData
}

func (ts *Tables) TableIds() (ids map[uint64]bool) {
	ts.RLock()
	defer ts.RUnlock()
	return ts.Ids
}

func (ts *Tables) DropTable(tid uint64) (err error) {
	ts.Lock()
	defer ts.Unlock()
	return ts.DropTableNoLock(tid)
}

func (ts *Tables) DropTableNoLock(tid uint64) (err error) {
	tbl, ok := ts.Data[tid]
	if !ok {
		return errors.New(fmt.Sprintf("Specified table %d not found", tid))
	}
	ts.Tombstone[tid] = tbl
	delete(ts.Ids, tid)
	delete(ts.Data, tid)
	return nil
}

func (ts *Tables) GetTableNoLock(tid uint64) (tbl ITableData, err error) {
	tbl, ok := ts.Data[tbl.GetID()]
	if !ok {
		return nil, errors.New(fmt.Sprintf("Specified table %d not found", tid))
	}
	return tbl, err
}

func (ts *Tables) GetTable(tid uint64) (tbl ITableData, err error) {
	ts.RLock()
	defer ts.RUnlock()
	return ts.GetTableNoLock(tid)
}

func (ts *Tables) CreateTable(tbl ITableData) (err error) {
	ts.Lock()
	defer ts.Unlock()
	return ts.CreateTableNoLock(tbl)
}

func (ts *Tables) CreateTableNoLock(tbl ITableData) (err error) {
	_, ok := ts.Data[tbl.GetID()]
	if ok {
		return errors.New(fmt.Sprintf("Dup table %d found", tbl.GetID()))
	}
	ts.Ids[tbl.GetID()] = true
	ts.Data[tbl.GetID()] = tbl
	return nil
}
