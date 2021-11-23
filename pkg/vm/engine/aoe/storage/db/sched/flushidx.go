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
	"path/filepath"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/sched"
)

// flushIndexEvent would generate, flush, and load index for the given segment.
// Columns are configurable. Notice that no matter how many versions of the same
// index for the same column of one segment exists, we always generate the newest
// version and try loading it on that segment. During loading phase there would
// also be a stale check to ensure never load stale versions.
type flushIndexEvent struct {
	BaseEvent
	Segment  iface.ISegment
	Cols     []uint16
	FlushAll bool
}

func NewFlushIndexEvent(ctx *Context, host iface.ISegment) *flushIndexEvent {
	e := &flushIndexEvent{Segment: host, Cols: make([]uint16, 0)}
	e.BaseEvent = BaseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(e, FlushIndexTask, ctx.DoneCB, ctx.Waitable),
	}
	return e
}

func (e *flushIndexEvent) Execute() error {
	ids := e.Segment.BlockIds()
	meta := e.Segment.GetMeta()
	dir := e.Segment.GetSegmentFile().GetDir()
	bsiEnabled := make([]int, 0)
	indice := meta.Table.GetCommit().Indice
	for _, idx := range indice.Indices3 {
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

	nodes := make([]*common.MemNode, 0)
	var wg sync.WaitGroup
	for _, colIdx := range bsiEnabled {
		vecs := make([]*vector.Vector, 0)
		for i := 0; i < len(ids); i++ {
			blk := e.Segment.WeakRefBlock(ids[i])
			vec, err := blk.GetVectorWrapper(colIdx)
			if err != nil {
				panic(err)
			}
			if vec.Vector.Length() == 0 {
				panic("logic error")
			}
			vecs = append(vecs, &vec.Vector)
			nodes = append(nodes, vec.MNode)
		}
		bsi, err := index.BuildNumericBsiIndex(vecs, meta.Table.Schema.ColDefs[colIdx].Type, int16(colIdx))
		if err != nil {
			panic(err)
		}
		version := e.Segment.GetIndexHolder().AllocateVersion(colIdx)
		filename := common.MakeBitSlicedIndexFileName(version, meta.Table.Id, meta.Id, uint16(colIdx))
		filename = filepath.Join(dir, filename)
		//logutil.Infof("%s", filename)
		if err := index.DefaultRWHelper.FlushBitSlicedIndex(bsi.(*index.NumericBsiIndex), filename); err != nil {
			panic(err)
		}
		logutil.Infof("BSI Flushed | %s", filename)
		wg.Add(1)
		go func() {
			e.Segment.GetIndexHolder().LoadIndex(e.Segment.GetSegmentFile(), filename)
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
