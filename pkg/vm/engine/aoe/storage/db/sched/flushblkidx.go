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

package sched

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/sched"
	"path/filepath"
	"sync"
)

type flushBlockIndexEvent struct {
	BaseEvent
	Block  iface.IBlock
	Cols     []uint16
	FlushAll bool
}

func NewFlushBlockIndexEvent(ctx *Context, host iface.IBlock) *flushBlockIndexEvent {
	e := &flushBlockIndexEvent{Block: host, Cols: make([]uint16, 0)}
	e.BaseEvent = BaseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(e, FlushIndexTask, ctx.DoneCB, ctx.Waitable),
	}
	return e
}

func (e *flushBlockIndexEvent) Execute() error {
	if e.Block.RefCount() == 0 {
		return nil
	}
	e.Block.Ref()
	defer e.Block.Unref()
	meta := e.Block.GetMeta()
	dir := e.Block.GetSegmentFile().GetDir()
	bsiEnabled := make([]int, 0)
	schema := meta.Segment.Table.Schema
	indice := meta.Segment.Table.GetIndexSchema()
	for _, idx := range indice.Indice {
		if idx.Type == metadata.NumBsi || idx.Type == metadata.FixStrBsi {
			for _, col := range idx.Columns {
				if e.FlushAll {
					bsiEnabled = append(bsiEnabled, int(col))
					continue
				}
				for _, include := range e.Cols {
					if col == include {
						bsiEnabled = append(bsiEnabled, int(col))
					}
				}
			}
		}
	}


	// TODO(zzl): thread safe?
	if e.Block.GetType() != base.PERSISTENT_BLK {
		return nil
	}

	nodes := make([]*common.MemNode, 0)
	var wg sync.WaitGroup
	for _, colIdx := range bsiEnabled {
		vec, err := e.Block.GetVectorWrapper(colIdx)
		if err != nil {
			return err
		}
		if vector.Length(&vec.Vector) == 0 {
			panic("logic error")
		}
		nodes = append(nodes, vec.MNode)
		startPos := int(schema.BlockMaxRows * uint64(meta.Idx))
		bsi, err := index.BuildBitSlicedIndex([]*vector.Vector{&vec.Vector}, schema.ColDefs[colIdx].Type, int16(colIdx), startPos)
		if err != nil {
			panic(err)
		}
		version := e.Block.GetIndexHolder().AllocateVersion(colIdx)
		filename := common.MakeBlockBitSlicedIndexFileName(version, meta.Segment.Table.Id, meta.Segment.Id, meta.Id, uint16(colIdx))
		filename = filepath.Join(filepath.Join(dir, "data"), filename)
		if err := index.DefaultRWHelper.FlushBitSlicedIndex(bsi.(index.Index), filename); err != nil {
			panic(err)
		}
		logutil.Infof("[BLK] BSI Flushed | %s", filename)
		wg.Add(1)
		go func() {
			e.Block.GetIndexHolder().LoadIndex(e.Block.GetSegmentFile(), filename)
			wg.Done()
		}()
	}
	wg.Wait()

	release := func() {
		for _, node := range nodes {
			common.GPool.Free(node)
		}
	}
	defer release()
	return nil
}

