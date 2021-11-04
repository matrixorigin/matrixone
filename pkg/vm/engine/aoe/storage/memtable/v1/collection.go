// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and

package memtable

import (
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/sched"
	me "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/events/meta"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	mb "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/base"
)

type collection struct {
	common.RefHelper
	mgr    *manager
	data   iface.ITableData
	mu     *sync.RWMutex
	mutBlk iface.IMutBlock
}

func newCollection(mgr *manager, data iface.ITableData) *collection {
	c := &collection{
		mgr:  mgr,
		data: data,
		mu:   &sync.RWMutex{},
	}
	mutBlk := data.StrongRefLastBlock()
	if mutBlk != nil {
		if mutBlk.GetType() == base.TRANSIENT_BLK {
			c.mutBlk = mutBlk.(iface.IMutBlock)
		} else {
			mutBlk.Unref()
		}
	}
	c.Ref()
	c.OnZeroCB = c.close
	return c
}

func (c *collection) GetMeta() *metadata.Table {
	return c.data.GetMeta()
}

func (c *collection) close() {
	if c.data != nil {
		c.data.Unref()
	}
	if c.mutBlk != nil {
		c.mutBlk.Unref()
	}
}

func (c *collection) Flush() error {
	c.mu.RLock()
	if c.mutBlk == nil {
		c.mu.RUnlock()
		return nil
	}
	blkHandle := c.mutBlk.MakeHandle()
	c.mu.RUnlock()
	defer blkHandle.Close()
	blk := blkHandle.GetNode().(mb.IMutableBlock)
	return blk.Flush()
}

func (c *collection) String() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mutBlk.String()
}

func (c *collection) onNoBlock() (meta *metadata.Block, data iface.IBlock, err error) {
	ctx := &sched.Context{Opts: c.mgr.opts, Waitable: true}
	var prevMeta *metadata.Block
	if c.mutBlk != nil {
		prevMeta = c.mutBlk.GetMeta()
	}
	e := me.NewCreateBlkEvent(ctx, c.data.GetID(), prevMeta, c.data)
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
	c.mutBlk = data.(iface.IMutBlock)
	return nil
}

func (c *collection) onImmut() {
	ctx := &sched.Context{Opts: c.mgr.opts}
	e := sched.NewFlushMemBlockEvent(ctx, c.mutBlk)
	c.mgr.opts.Scheduler.Schedule(e)
	c.onNoMut()
}

func (c *collection) doAppend(mutblk mb.IMutableBlock, bat *batch.Batch, offset uint64, index *metadata.LogIndex) (n uint64, err error) {
	var na int
	meta := mutblk.GetMeta()
	data := mutblk.GetData()
	for idx, attr := range data.GetAttrs() {
		for i, a := range bat.Attrs {
			if a == meta.Segment.Table.Schema.ColDefs[idx].Name {
				vec, err := data.GetVectorByAttr(attr)
				if err != nil {
					return 0, err
				}
				if na, err = vec.AppendVector(bat.Vecs[i], int(offset)); err != nil {
					return n, err
				}
			}
		}
	}
	n = uint64(na)
	index.Count = n
	if err = meta.SetIndex(*index); err != nil {
		return 0, err
	}
	// log.Infof("1. offset=%d, n=%d, cap=%d, index=%s, blkcnt=%d", offset, n, bat.Vecs[0].Length(), index.String(), mt.Meta.GetCount())
	if _, err = meta.AddCount(n); err != nil {
		return 0, err
	}
	c.data.AddRows(n)
	// log.Infof("2. offset=%d, n=%d, cap=%d, index=%s, blkcnt=%d", offset, n, bat.Vecs[0].Length(), index.String(), mt.Meta.GetCount())
	// if uint64(data.Length()) == meta.Segment.Table.Schema.BlockMaxRows {
	// 	meta.TryUpgrade()
	// }
	return n, nil
}

func (c *collection) Append(bat *batch.Batch, index *metadata.LogIndex) (err error) {
	// tableMeta := c.data.GetMeta()
	logutil.Infof("Append logindex: %s", index.String())
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mutBlk == nil {
		c.onNoMut()
	} else if c.mutBlk.GetMeta().HasMaxRows() {
		c.onImmut()
	}

	offset := uint64(0)
	replayIndex := c.data.GetReplayIndex()
	if replayIndex != nil {
		logutil.Infof("Table %d ReplayIndex %s", c.data.GetID(), replayIndex.String())
		logutil.Infof("Incoming Index %s", index.String())
		if !replayIndex.IsApplied() {
			if (replayIndex.Id.Id != index.Id.Id) ||
				(replayIndex.Id.Offset < index.Id.Offset) {
				panic(fmt.Sprintf("should replayIndex: %d, but %d received", replayIndex.Id, index.Id))
			}
			if replayIndex.Id.Offset > index.Id.Offset {
				logutil.Infof("Index %s has been applied", index.String())
				return nil
			}
			offset = replayIndex.Count + replayIndex.Start
			index.Start = offset
		}
		c.data.ResetReplayIndex()
	}
	blkHandle := c.mutBlk.MakeHandle()
	for {
		if c.mutBlk.GetMeta().HasMaxRows() {
			c.onImmut()
			blkHandle.Close()
			blkHandle = c.mutBlk.MakeHandle()
		}
		blk := blkHandle.GetNode().(mb.IMutableBlock)
		n, err := c.doAppend(blk, bat, offset, index)
		if err != nil {
			blkHandle.Close()
			return err
		}
		offset += n
		if offset == uint64(bat.Vecs[0].Length()) {
			break
		}
		index.Start += n
		index.Count = uint64(0)
	}
	blkHandle.Close()

	return err
}
