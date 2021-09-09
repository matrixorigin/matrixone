package memtable

import (
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/logutil"
	engine "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/db/sched"
	me "matrixone/pkg/vm/engine/aoe/storage/events/meta"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"sync"
)

type collection struct {
	common.RefHelper
	id        uint64
	opts      *engine.Options
	tableData iface.ITableData
	mem       struct {
		sync.RWMutex
		memTables []imem.IMemTable
	}
}

var (
	_ imem.ICollection = (*collection)(nil)
)

func NewCollection(tableData iface.ITableData, opts *engine.Options) imem.ICollection {
	c := &collection{
		id:        tableData.GetID(),
		opts:      opts,
		tableData: tableData,
	}
	c.mem.memTables = make([]imem.IMemTable, 0)
	c.OnZeroCB = c.close
	c.Ref()
	return c
}

func (c *collection) String() string {
	c.mem.RLock()
	defer c.mem.RUnlock()
	s := fmt.Sprintf("<collection[%d]>(Refs=%d)(MTCnt=%d)", c.id, c.RefCount(), len(c.mem.memTables))
	for _, mt := range c.mem.memTables {
		s = fmt.Sprintf("%s\n\t%s", s, mt.String())
	}
	return s
}

func (c *collection) close() {
	if c.tableData != nil {
		c.tableData.Unref()
	}
	for _, mt := range c.mem.memTables {
		mt.Unref()
	}
	return
}

func (c *collection) onNoBlock() (meta *md.Block, data iface.IBlock, err error) {
	eCtx := &sched.Context{Opts: c.opts, Waitable: true}
	e := me.NewCreateBlkEvent(eCtx, c.id, c.tableData)
	if err = c.opts.Scheduler.Schedule(e); err != nil {
		return nil, nil, err
	}
	if err = e.WaitDone(); err != nil {
		return nil, nil, err
	}
	meta = e.GetBlock()
	return meta, e.Block, nil
}

func (c *collection) onNoMutableTable() (tbl imem.IMemTable, err error) {
	_, data, err := c.onNoBlock()
	if err != nil {
		return nil, err
	}

	c.tableData.Ref()
	tbl = NewMemTable(c.opts, c.tableData, data)
	c.mem.memTables = append(c.mem.memTables, tbl)
	tbl.Ref()
	return tbl, err
}

func (c *collection) Append(bat *batch.Batch, index *md.LogIndex) (err error) {
	tableMeta := c.tableData.GetMeta()
	var mut imem.IMemTable
	c.mem.Lock()
	defer c.mem.Unlock()
	size := len(c.mem.memTables)
	if size == 0 {
		mut, err = c.onNoMutableTable()
		if err != nil {
			return err
		}
	} else {
		mut = c.mem.memTables[size-1]
		mut.Ref()
	}
	offset := uint64(0)
	replayIndex := tableMeta.GetReplayIndex()
	if replayIndex != nil {
		offset = replayIndex.Count + replayIndex.Start
		tableMeta.ResetReplayIndex()
		index.Start = offset
		logutil.Infof("Table %d ReplayIndex %s", tableMeta.ID, replayIndex.String())
	}
	for {
		if mut.IsFull() {
			prevId := mut.GetMeta().AsCommonID()
			mut.Unpin()
			mut.Unref()

			ctx := &sched.Context{Opts: c.opts}
			e := sched.NewPrecommitBlockEvent(ctx, *prevId)
			if err = c.opts.Scheduler.Schedule(e); err != nil {
				panic(err)
			}

			mut, err = c.onNoMutableTable()
			if err != nil {
				c.opts.EventListener.BackgroundErrorCB(err)
				return err
			}

			{
				c.Ref()
				ctx := &sched.Context{Opts: c.opts}
				e := sched.NewFlushMemtableEvent(ctx, c)
				err = c.opts.Scheduler.Schedule(e)
				if err != nil {
					return err
				}
			}
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

func (c *collection) FetchImmuTable() imem.IMemTable {
	c.mem.Lock()
	defer c.mem.Unlock()
	if len(c.mem.memTables) <= 1 {
		return nil
	}
	var immu imem.IMemTable
	immu, c.mem.memTables = c.mem.memTables[0], c.mem.memTables[1:]
	return immu
}
