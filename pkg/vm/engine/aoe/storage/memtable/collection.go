package memtable

import (
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/logutil"
	"matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	dbsched "matrixone/pkg/vm/engine/aoe/storage/db/sched"
	me "matrixone/pkg/vm/engine/aoe/storage/events/meta"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"sync"
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
	eCtx := &dbsched.Context{Opts: c.Opts, Waitable: true}
	e := me.NewCreateBlkEvent(eCtx, c.ID, c.TableData)
	if err = c.Opts.Scheduler.Schedule(e); err != nil {
		return nil, nil, err
	}
	if err = e.WaitDone(); err != nil {
		return nil, nil, err
	}
	meta = e.GetBlock()
	return meta, e.Block, nil
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
	tableMeta := c.TableData.GetMeta()
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

			ctx := &dbsched.Context{Opts: c.Opts}
			e := dbsched.NewPrecommitBlockEvent(ctx, *prevId)
			if err = c.Opts.Scheduler.Schedule(e); err != nil {
				panic(err)
			}

			mut, err = c.onNoMutableTable()
			if err != nil {
				c.Opts.EventListener.BackgroundErrorCB(err)
				return err
			}

			{
				c.Ref()
				ctx := &dbsched.Context{Opts: c.Opts}
				e := dbsched.NewFlushMemtableEvent(ctx, c)
				err = c.Opts.Scheduler.Schedule(e)
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
