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
// limitations under the License.

package memtable

import (
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/sched"
	me "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/events/meta"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	imem "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/memtable/v1/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

// collection is the collection of memTable
// a table has only one collection, which can
// manage multiple memeTables in the current table
type collection struct {
	common.RefHelper

	// id is collection's id
	id uint64

	// opts is the options of aoe
	opts *storage.Options

	// TableData is Table's metadata in memory
	tableData iface.ITableData

	// mem is containers of managed memTable
	mem struct {
		sync.RWMutex
		memTables []imem.IMemTable
	}

	PrevBlock *metadata.Block
}

var (
	_ imem.ICollection = (*collection)(nil)
)

func NewCollection(tableData iface.ITableData, opts *storage.Options) imem.ICollection {
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

func (c *collection) GetMeta() *metadata.Table {
	return c.tableData.GetMeta()
}

func (c *collection) String() string {
	c.mem.RLock()
	defer c.mem.RUnlock()
	s := fmt.Sprintf("<collection[%d]>(RefCount=%d)(MTCnt=%d)", c.id, c.RefCount(), len(c.mem.memTables))
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

// onNoBlock creates a logical Block
func (c *collection) onNoBlock() (meta *metadata.Block, data iface.IBlock, err error) {
	eCtx := &sched.Context{Opts: c.opts, Waitable: true}
	e := me.NewCreateBlkEvent(eCtx, c.id, c.PrevBlock, c.tableData)
	if err = c.opts.Scheduler.Schedule(e); err != nil {
		return nil, nil, err
	}
	if err = e.WaitDone(); err != nil {
		return nil, nil, err
	}
	meta = e.GetBlock()
	c.PrevBlock = meta
	return meta, e.Block, nil
}

// onNoMutableTable Create a memeTable and append it to collection.mem
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

func (c *collection) Flush() error {
	return nil
}

// Append is the external interface provided by storage.
// Receive the batch data, and then store it in aoe.
func (c *collection) Append(bat *batch.Batch, index *metadata.LogIndex) (err error) {
	logutil.Infof("Append logindex: %s", index.String())
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
	replayIndex := c.tableData.GetReplayIndex()
	if replayIndex != nil {
		logutil.Infof("Table %d ReplayIndex %s", c.tableData.GetID(), replayIndex.String())
		if !replayIndex.IsApplied() {
			if (replayIndex.Id.Id != index.Id.Id) ||
				(replayIndex.Id.Offset < index.Id.Offset) {
				panic(fmt.Sprintf("replayIndex: %d, but %d received", replayIndex.Id, index.Id))
			}
			if replayIndex.Id.Offset > index.Id.Offset {
				logutil.Infof("Index %s has been applied", index.String())
				c.opts.Wal.Checkpoint(index)
				return nil
			}
			offset = replayIndex.Count + replayIndex.Start
			if offset > 0 {
				ckpId := *index
				ckpId.Count = offset
				c.opts.Wal.Checkpoint(&ckpId)
			}
			index.Start = offset
		}
		c.tableData.ResetReplayIndex()
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
