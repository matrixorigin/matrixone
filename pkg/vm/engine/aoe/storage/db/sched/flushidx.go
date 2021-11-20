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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	sif "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/sched/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/sched"
	"os"
	"path/filepath"
	"sync"
)

type flushIndexEvent struct {
	BaseEvent
	Segment iface.ISegment
}

func NewFlushIndexEvent(ctx *sif.Context, host iface.ISegment) *flushIndexEvent {
	e := &flushIndexEvent{Segment: host}
	e.BaseEvent = BaseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(e, sched.FlushIndexTask, ctx.DoneCB, ctx.Waitable),
	}
	return e
}

func (e *flushIndexEvent) Execute() error {
	ids := e.Segment.BlockIds()
	meta := e.Segment.GetMeta()
	dir := e.Segment.GetSegmentFile().GetDir()
	bsiEnabled := make([]int, 0)
	for _, idx := range meta.Table.Schema.Indices {
		if idx.Type == metadata.NumBsi || idx.Type == metadata.FixStrBsi {
			for _, col := range idx.Columns {
				bsiEnabled = append(bsiEnabled, int(col))
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
		version := 0
		filename := fmt.Sprintf("%d_%d_%d_%d.bsi", version, meta.Table.Id, meta.Id, colIdx)
		for {
			if _, err := os.Stat(filepath.Join(dir, filename)); os.IsNotExist(err) {
				filename = filepath.Join(dir, filename)
				break
			}
			version++
			filename = fmt.Sprintf("%d_%d_%d_%d.bsi", version, meta.Table.Id, meta.Id, colIdx)
		}
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
