package table

import (
	"errors"
	"fmt"
	"matrixone/pkg/container/types"
	logutil2 "matrixone/pkg/logutil"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/col"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
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
	GetMTBufMgr() bmgrif.IBufferManager
	GetSSTBufMgr() bmgrif.IBufferManager
	GetSegmentCount() uint64
	GetIndexHolder() *index.TableHolder

	UpgradeBlock(*md.Block) (blks []col.IColumnBlock)
	UpgradeSegment(segID common.ID) (segs []col.IColumnSegment)
	AppendColSegments(colSegs []col.IColumnSegment)
}

func NewTableData(fsMgr base.IManager, indexBufMgr, mtBufMgr, sstBufMgr bmgrif.IBufferManager, meta *md.Table) ITableData {
	data := &TableData{
		Columns:     make([]col.IColumnData, 0),
		MTBufMgr:    mtBufMgr,
		SSTBufMgr:   sstBufMgr,
		Meta:        meta,
		IndexHolder: index.NewTableHolder(indexBufMgr, meta.ID),
	}
	for idx, colDef := range meta.Schema.ColDefs {
		data.Columns = append(data.Columns, col.NewColumnData(data.IndexHolder, fsMgr, mtBufMgr, sstBufMgr, colDef.Type, idx))
	}
	runtime.SetFinalizer(data, func(o ITableData) {
		id := o.GetID()
		logutil2.Debugf("[GC]: TableData: %d", id)
	})
	return data
}

type TableData struct {
	sync.Mutex
	RowCount    uint64
	Columns     []col.IColumnData
	MTBufMgr    bmgrif.IBufferManager
	SSTBufMgr   bmgrif.IBufferManager
	Meta        *md.Table
	IndexHolder *index.TableHolder
}

func (td *TableData) GetRowCount() uint64 {
	return td.RowCount
}

func (td *TableData) GetIndexHolder() *index.TableHolder {
	return td.IndexHolder
}

func (td *TableData) GetID() uint64 {
	return td.Meta.ID
}

func (td *TableData) GetColTypes() []types.Type {
	return td.Meta.Schema.Types()
}

func (td *TableData) GetCollumn(idx int) col.IColumnData {
	if idx >= len(td.Meta.Schema.ColDefs) {
		panic("logic error")
	}
	return td.Columns[idx]
}

func (td *TableData) GetCollumns() []col.IColumnData {
	return td.Columns
}

func (td *TableData) GetColTypeSize(idx int) uint64 {
	return uint64(td.Meta.Schema.ColDefs[idx].Type.Size)
}

func (td *TableData) GetMTBufMgr() bmgrif.IBufferManager {
	return td.MTBufMgr
}

func (td *TableData) GetSSTBufMgr() bmgrif.IBufferManager {
	return td.SSTBufMgr
}

func (td *TableData) GetSegmentCount() uint64 {
	return td.Columns[0].SegmentCount()
}

func (td *TableData) UpgradeBlock(newMeta *md.Block) (blks []col.IColumnBlock) {
	for _, column := range td.Columns {
		blk := column.UpgradeBlock(newMeta)
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
	if len(colSegs) != len(td.Meta.Schema.ColDefs) {
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

func NewTables() *Tables {
	return &Tables{
		Data:      make(map[uint64]ITableData),
		Ids:       make(map[uint64]bool),
		Tombstone: make(map[uint64]ITableData),
	}
}

func (ts *Tables) TableIds() (ids map[uint64]bool) {
	return ts.Ids
}

func (ts *Tables) DropTable(tid uint64) (err error) {
	ts.Lock()
	err = ts.DropTableNoLock(tid)
	ts.Unlock()
	return err
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
	tbl, ok := ts.Data[tid]
	if !ok {
		return nil, errors.New(fmt.Sprintf("Specified table %d not found", tid))
	}
	return tbl, err
}

func (ts *Tables) GetTable(tid uint64) (tbl ITableData, err error) {
	ts.RLock()
	tbl, err = ts.GetTableNoLock(tid)
	ts.RUnlock()
	return tbl, err
}

func (ts *Tables) CreateTable(tbl ITableData) (err error) {
	ts.Lock()
	err = ts.CreateTableNoLock(tbl)
	ts.Unlock()
	return err
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

func (ts *Tables) Replay(fsMgr base.IManager, indexBufMgr, mtBufMgr, sstBufMgr bmgrif.IBufferManager, info *md.MetaInfo) error {
	for _, meta := range info.Tables {
		tbl := NewTableData(fsMgr, indexBufMgr, mtBufMgr, sstBufMgr, meta)
		colTypes := meta.Schema.Types()
		activeSeg := meta.GetActiveSegment()
		for _, segMeta := range meta.Segments {
			if activeSeg != nil {
				if activeSeg.ID < segMeta.ID {
					break
				}
			}
			segType := base.UNSORTED_SEG
			if segMeta.DataState == md.SORTED {
				segType = base.SORTED_SEG
			}
			activeBlk := segMeta.GetActiveBlk()
			for colIdx, colType := range colTypes {
				colSeg := col.NewColumnSegment(tbl.GetIndexHolder(), fsMgr, mtBufMgr, sstBufMgr, colIdx, segMeta)
				defer colSeg.UnRef()
				for _, blkMeta := range segMeta.Blocks {
					if activeBlk != nil {
						if activeBlk.ID <= blkMeta.ID {
							break
						}
					}
					blkId := *blkMeta.AsCommonID()
					bufMgr := mtBufMgr
					if segType == base.SORTED_SEG {
						bufMgr = sstBufMgr
					} else if blkMeta.DataState == md.FULL {
						bufMgr = sstBufMgr
					}
					// TODO: strblk
					// Only stdblk now
					colBlk := col.NewStdColumnBlock(colSeg.Ref(), blkMeta)
					colSeg.Append(colBlk.Ref())
					defer colBlk.UnRef()
					colPart := col.NewColumnPart(colSeg.GetFsManager(), bufMgr, colBlk.Ref(), blkId, info.Conf.BlockMaxRows*uint64(colType.Size))
					if colPart == nil {
						return errors.New(fmt.Sprintf("data replay error"))
					}
					// TODO: How to handle more than 1 empty blk or segment
					// if blkMeta.DataState == md.EMPTY {
					// 	break
					// }
				}
				if err := tbl.GetCollumn(colIdx).Append(colSeg.Ref()); err != nil {
					return err
				}
			}
		}
		if err := ts.CreateTable(tbl); err != nil {
			return err
		}
	}
	return nil
}

func MockSegment(indexHolder *index.TableHolder, fsMgr base.IManager, mtBufMgr, sstBufMgr bmgrif.IBufferManager, colIdx int, meta *md.Segment) col.IColumnSegment {
	seg := col.NewColumnSegment(indexHolder, fsMgr, mtBufMgr, sstBufMgr, colIdx, meta)
	for _, blkMeta := range meta.Blocks {
		blk, err := seg.RegisterBlock(blkMeta)
		if err != nil {
			panic(err)
		}
		blk.UnRef()
	}
	return seg
}

func MockSegments(fsMgr base.IManager, mtBufMgr, sstBufMgr bmgrif.IBufferManager, meta *md.Table, tblData ITableData) []common.ID {
	var segIDs []common.ID
	for _, segMeta := range meta.Segments {
		var colSegs []col.IColumnSegment
		for colIdx, _ := range segMeta.Table.Schema.ColDefs {
			colSeg := MockSegment(tblData.GetIndexHolder(), fsMgr, mtBufMgr, sstBufMgr, colIdx, segMeta)
			colSegs = append(colSegs, colSeg)
		}
		tblData.AppendColSegments(colSegs)
		segIDs = append(segIDs, *segMeta.AsCommonID())
	}
	return segIDs
}
