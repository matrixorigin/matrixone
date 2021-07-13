package memtable

import (
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	dops "matrixone/pkg/vm/engine/aoe/storage/ops/data"
	mops "matrixone/pkg/vm/engine/aoe/storage/ops/meta/v2"
	"sync"
	// log "github.com/sirupsen/logrus"
)

type Collection struct {
	common.RefHelper
	ID        uint64
	Opts      *engine.Options
	TableData iface.ITableData
	mem       struct {
		sync.RWMutex
		MemTables []imem.IMemTable
	}
}

var (
	_ imem.ICollection = (*Collection)(nil)
)

func NewCollection(tableData iface.ITableData, opts *engine.Options) imem.ICollection {
	c := &Collection{
		ID:        tableData.GetID(),
		Opts:      opts,
		TableData: tableData,
	}
	c.mem.MemTables = make([]imem.IMemTable, 0)
	c.OnZeroCB = c.close
	c.Ref()
	return c
}

func (c *Collection) String() string {
	c.mem.RLock()
	defer c.mem.RUnlock()
	s := fmt.Sprintf("<Collection[%d]>(Refs=%d)(MTCnt=%d)", c.ID, c.RefCount(), len(c.mem.MemTables))
	for _, mt := range c.mem.MemTables {
		s = fmt.Sprintf("%s\n\t%s", s, mt.String())
	}
	return s
}

func (c *Collection) close() {
	if c.TableData != nil {
		c.TableData.Unref()
	}
	for _, mt := range c.mem.MemTables {
		mt.Unref()
	}
	return
}

func (c *Collection) onNoBlock() (meta *md.Block, data iface.IBlock, err error) {
	ctx := mops.OpCtx{Opts: c.Opts}
	op := mops.NewCreateBlkOp(&ctx, c.ID, c.TableData)
	op.Push()
	err = op.WaitDone()
	if err != nil {
		return nil, nil, err
	}
	meta = op.GetBlock()
	return meta, op.Block, nil
}

func (c *Collection) onNoMutableTable() (tbl imem.IMemTable, err error) {
	_, data, err := c.onNoBlock()
	if err != nil {
		return nil, err
	}

	c.TableData.Ref()
	tbl = NewMemTable(c.Opts, c.TableData, data)
	c.mem.MemTables = append(c.mem.MemTables, tbl)
	tbl.Ref()
	return tbl, err
}

func (c *Collection) Append(bat *batch.Batch, index *md.LogIndex) (err error) {
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
		mut.Ref()
	}
	offset := uint64(0)
	for {
		if mut.IsFull() {
			mut.Unpin()
			mut.Unref()
			mut, err = c.onNoMutableTable()
			if err != nil {
				c.Opts.EventListener.BackgroundErrorCB(err)
				return err
			}
			go func() {
				c.Ref()
				ctx := dops.OpCtx{Collection: c, Opts: c.Opts}
				op := dops.NewFlushBlkOp(&ctx)
				op.Push()
				op.WaitDone()
			}()
		}
		n, err := mut.Append(bat, offset, index)
		if err != nil {
			mut.Unref()
			return err
		}
		offset += n
		if offset == uint64(bat.Vecs[0].Length()) {
			break
		}
		if index.IsApplied() {
			break
		}
		index.Start += n
		index.Count = uint64(0)
	}
	mut.Unref()
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
