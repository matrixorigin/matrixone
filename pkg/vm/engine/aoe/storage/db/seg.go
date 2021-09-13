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

package db

import (
	"matrixone/pkg/encoding"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"matrixone/pkg/vm/process"
	"sync/atomic"
)

type Segment struct {
	Data iface.ISegment
	Ids  *atomic.Value
}

func (seg *Segment) ID() string {
	id := seg.Data.GetMeta().GetID()
	return string(encoding.EncodeUint64(id))
}

func (seg *Segment) Blocks() []string {
	if ids := seg.Ids.Load(); ids != nil {
		return ids.([]string)
	}
	ids := seg.Data.BlockIds()
	strs := make([]string, len(ids))
	for idx, id := range ids {
		strs[idx] = string(encoding.EncodeUint64(id))
	}
	seg.Ids.Store(strs)
	return strs
}

func (seg *Segment) Block(id string, proc *process.Process) engine.Block {
	iid := encoding.DecodeUint64(([]byte)(id))
	data := seg.Data.WeakRefBlock(iid)
	if data == nil {
		return nil
	}
	blk := &Block{
		StrId: id,
		Id:    iid,
		Host:  seg,
	}
	return blk
}

func (seg *Segment) NewFilter() engine.Filter {
	return NewSegmentFilter(seg)
}

func (seg *Segment) NewSummarizer() engine.Summarizer {
	return NewSegmentSummarizer(seg)
}

func (seg *Segment) NewSparseFilter() engine.SparseFilter {
	return NewSegmentSparseFilter(seg)
}

func (seg *Segment) Rows() int64 {
	return int64(seg.Data.GetRowCount())
}

func (seg *Segment) Size(attr string) int64 {
	return int64(seg.Data.Size(attr))
}
