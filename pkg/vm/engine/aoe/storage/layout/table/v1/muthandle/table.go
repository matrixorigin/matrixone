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

package muthandle

import (
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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"
)

type mutableTable struct {
	common.RefHelper
	mgr    *manager
	data   iface.ITableData
	meta   *metadata.Table
	mu     *sync.RWMutex
	mutBlk iface.IMutBlock
}

func newMutableTable(mgr *manager, data iface.ITableData) *mutableTable {
	c := &mutableTable{
		mgr:  mgr,
		data: data,
		meta: data.GetMeta(),
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

func (c *mutableTable) GetMeta() *metadata.Table {
	return c.data.GetMeta()
}

func (c *mutableTable) close() {
	if c.data != nil {
		c.data.Unref()
	}
	if c.mutBlk != nil {
		c.mutBlk.Unref()
	}
}

func (c *mutableTable) Flush() error {
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

func (c *mutableTable) String() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mutBlk.String()
}

func (c *mutableTable) onNoBlock() (meta *metadata.Block, data iface.IBlock, err error) {
	ctx := &sched.Context{Opts: c.mgr.opts, Waitable: true}
	var prevMeta *metadata.Block
	if c.mutBlk != nil {
		prevMeta = c.mutBlk.GetMeta()
	}
	e := me.NewCreateBlkEvent(ctx, c.data.GetMeta(), prevMeta, c.data)
	c.mgr.opts.Scheduler.Schedule(e)
	if err = e.WaitDone(); err != nil {
		return nil, nil, err
	}
	meta = e.GetBlock()
	return meta, e.Block, nil
}

func (c *mutableTable) onNoMut() error {
	_, data, err := c.onNoBlock()
	if err != nil {
		return err
	}
	c.mutBlk = data.(iface.IMutBlock)
	return nil
}

func (c *mutableTable) onImmut() {
	ctx := &sched.Context{Opts: c.mgr.opts}
	e := sched.NewFlushMemBlockEvent(ctx, c.mutBlk)
	c.mgr.opts.Scheduler.Schedule(e)
	c.onNoMut()
}

func (c *mutableTable) doAppend(mutblk mb.IMutableBlock, bat *batch.Batch, offset uint64, index *shard.SliceIndex) (n uint64, err error) {
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
	meta.Lock()
	defer meta.Unlock()
	if !index.IsApplied() && !index.IsSlice() {
		left := index.Capacity - index.Start - index.Count
		slices := 1 + left/meta.Segment.Table.Schema.BlockMaxRows
		if left%meta.Segment.Table.Schema.BlockMaxRows != 0 {
			slices += 1
		}
		index.Info = &shard.SliceInfo{
			Offset: 0,
			Size:   uint32(slices),
		}
	}
	if err = meta.SetIndexLocked(index); err != nil {
		return 0, err
	}
	// log.Infof("1. offset=%d, n=%d, cap=%d, index=%s, blkcnt=%d", offset, n, bat.Vecs[0].Length(), index.String(), mt.Meta.GetCount())
	if _, err = meta.AddCountLocked(n); err != nil {
		return 0, err
	}
	c.data.AddRows(n)
	// log.Infof("2. offset=%d, n=%d, cap=%d, index=%s, blkcnt=%d", offset, n, bat.Vecs[0].Length(), index.String(), mt.Meta.GetCount())
	return n, nil
}

func (c *mutableTable) Append(bat *batch.Batch, index *shard.SliceIndex) (err error) {
	logutil.Infof("Append logindex: %s", index.String())
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mutBlk == nil {
		c.onNoMut()
	} else if c.mutBlk.GetMeta().HasMaxRowsLocked() {
		c.onImmut()
	}

	offset := index.Start
	blkHandle := c.mutBlk.MakeHandle()
	for {
		if c.mutBlk.GetMeta().HasMaxRowsLocked() {
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
		index.Info.Offset += 1
	}
	blkHandle.Close()

	return err
}
