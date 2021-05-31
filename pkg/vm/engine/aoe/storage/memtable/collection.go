package memtable

import (
	"matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	dops "matrixone/pkg/vm/engine/aoe/storage/ops/data"
	mops "matrixone/pkg/vm/engine/aoe/storage/ops/meta"
	"sync"
	// log "github.com/sirupsen/logrus"
)

type Collection struct {
	ID        uint64
	Opts      *engine.Options
	TableData table.ITableData
	mem       struct {
		sync.RWMutex
		MemTables []imem.IMemTable
		Cursors   []col.IScanCursor
	}
}

var (
	_ imem.ICollection = (*Collection)(nil)
)

func NewCollection(tableData table.ITableData, opts *engine.Options) imem.ICollection {
	c := &Collection{
		ID:        tableData.GetID(),
		Opts:      opts,
		TableData: tableData,
	}
	c.mem.MemTables = make([]imem.IMemTable, 0)
	c.mem.Cursors = make([]col.IScanCursor, len(tableData.GetCollumns()))
	return c
}

func (c *Collection) onNoBlock() (blk *md.Block, colBlks []col.IColumnBlock, err error) {
	ctx := mops.OpCtx{Opts: c.Opts}
	op := mops.NewCreateBlkOp(&ctx, c.ID, c.TableData)
	op.Push()
	err = op.WaitDone()
	if err != nil {
		return nil, colBlks, err
	}
	blk = op.GetBlock()
	return blk, op.ColBlocks, nil
}

func (c *Collection) onNoMutableTable() (tbl imem.IMemTable, err error) {
	blk, colBlks, err := c.onNoBlock()
	if err != nil {
		return nil, err
	}

	for idx, colBlk := range colBlks {
		c.mem.Cursors[idx] = &col.ScanCursor{}
		colBlk.InitScanCursor(c.mem.Cursors[idx].(*col.ScanCursor))
		c.mem.Cursors[idx].Init()
	}

	tbl = NewMemTable(c.TableData, c.TableData.GetColTypes(), colBlks, c.mem.Cursors, c.Opts, blk)
	c.mem.MemTables = append(c.mem.MemTables, tbl)
	return tbl, err
}

func (c *Collection) Append(ck *chunk.Chunk, index *md.LogIndex) (err error) {
	var mut imem.IMemTable
	c.mem.Lock()
	defer c.mem.Unlock()
	size := len(c.mem.MemTables)
	if size == 0 {
		mut, err = c.onNoMutableTable()
		if err != nil {
			return err
		}
	} else {
		mut = c.mem.MemTables[size-1]
	}
	offset := uint64(0)
	for {
		if mut.IsFull() {
			mut, err = c.onNoMutableTable()
			if err != nil {
				c.Opts.EventListener.BackgroundErrorCB(err)
				return err
			}
			for _, cursor := range c.mem.Cursors {
				cursor.Close()
			}
			go func() {
				ctx := dops.OpCtx{Collection: c, Opts: c.Opts}
				op := dops.NewFlushBlkOp(&ctx)
				op.Push()
				op.WaitDone()
			}()
		}
		n, err := mut.Append(ck, offset, index)
		if err != nil {
			return err
		}
		offset += n
		if offset == ck.GetCount() {
			break
		}
		if index.IsApplied() {
			break
		}
		index.Start += n
		index.Count = uint64(0)
	}
	return nil
}

func (c *Collection) FetchImmuTable() imem.IMemTable {
	c.mem.Lock()
	defer c.mem.Unlock()
	if len(c.mem.MemTables) <= 1 {
		return nil
	}
	var immu imem.IMemTable
	immu, c.mem.MemTables = c.mem.MemTables[0], c.mem.MemTables[1:]
	return immu
}
