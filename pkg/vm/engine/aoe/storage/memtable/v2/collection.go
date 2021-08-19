package memtable

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/db/sched"
	me "matrixone/pkg/vm/engine/aoe/storage/events/meta"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"sync"
)

type collection struct {
	common.RefHelper
	mgr  *manager
	data iface.ITableData
	mu   *sync.RWMutex
	mut  *memTable
}

func newCollection(mgr *manager, data iface.ITableData) *collection {
	c := &collection{
		mgr:  mgr,
		data: data,
		mu:   &sync.RWMutex{},
	}
	c.Ref()
	c.OnZeroCB = c.close
	return c
}

func (c *collection) close() {
	// TODO
}

func (c *collection) String() string {
	// TODO
	return ""
}

func (c *collection) onNoBlock() (meta *metadata.Block, data iface.IBlock, err error) {
	ctx := &sched.Context{Opts: c.mgr.opts, Waitable: true}
	e := me.NewCreateBlkEvent(ctx, c.data.GetID(), c.data)
	c.mgr.opts.Scheduler.Schedule(e)
	if err = e.WaitDone(); err != nil {
		return nil, nil, err
	}
	meta = e.GetBlock()
	return meta, e.Block, nil
}

func (c *collection) onNoMut() error {
	_, data, err := c.onNoBlock()
	if err != nil {
		return err
	}
	c.data.Ref()
	c.mut = newMemTable(c.mgr, c.data, data)
	c.mgr.nodemgr.RegisterNode(c.mut)
	return nil
}

func (c *collection) onImmut() {
	c.mut.Unpin()
	ctx := &sched.Context{Opts: c.mgr.opts}
	e := sched.NewPrepareCommitBlockEvent(ctx, c.mut)
	c.mgr.opts.Scheduler.Schedule(e)
	c.onNoMut()
}

func (c *collection) Append(bat *batch.Batch, index *metadata.LogIndex) (err error) {
	tableMeta := c.data.GetMeta()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mut == nil {
		c.onNoMut()
	} else if c.mut.IsFull() {
		c.onImmut()
	}
	offset := uint64(0)
	replayIndex := tableMeta.GetReplayIndex()
	if replayIndex != nil {
		offset = replayIndex.Count
		tableMeta.ResetReplayIndex()
	}
	c.mut.Pin()
	for {
		if c.mut.IsFull() {
			c.onImmut()
		}
		n, err := c.mut.Append(bat, offset, index)
		if err != nil {
			c.mut.Unpin()
			return err
		}
		offset += n
		if offset == uint64(bat.Vecs[0].Length()) {
			break
		}
		index.Start += n
		index.Count = uint64(0)
	}
	c.mut.Unpin()

	return err
}
