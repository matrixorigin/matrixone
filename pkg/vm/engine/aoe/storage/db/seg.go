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
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"sync/atomic"
)

// Segment is a high-level wrapper of the segment type in memory. It
// only provides some essential interfaces used by computation layer.
type Segment struct {
	Data iface.ISegment
	Ids  *atomic.Value
}

// ID returns the string representation of this segment's id.
func (seg *Segment) ID() string {
	id := seg.Data.GetMeta().Id
	return string(encoding.EncodeUint64(id))
}

// Blocks returns the list of block ids in string type.
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

// Block returns a block with the given block id.
func (seg *Segment) Block(id string) aoe.Block {
	iid := encoding.DecodeUint64(([]byte)(id))
	data := seg.Data.WeakRefBlock(iid)
	if data == nil {
		// TODO: returns error
		logutil.Warnf("specified block %s not found", id)
		return nil
	}
	blk := &Block{
		StrId: id,
		Id:    iid,
		Host:  seg,
	}
	return blk
}

// NewFilter generates a Filter for segment.
func (seg *Segment) NewFilter() engine.Filter {
	return NewSegmentFilter(seg)
}

// NewSummarizer generates a Summarizer for segment.
func (seg *Segment) NewSummarizer() engine.Summarizer {
	return NewSegmentSummarizer(seg)
}

// NewSparseFilter generates a SparseFilter for segment.
func (seg *Segment) NewSparseFilter() engine.SparseFilter {
	return NewSegmentSparseFilter(seg)
}

// Rows returns how many rows this segment contains currently.
func (seg *Segment) Rows() int64 {
	return int64(seg.Data.GetRowCount())
}

// Size returns the memory usage of the certain column in a segment.
func (seg *Segment) Size(attr string) int64 {
	return int64(seg.Data.Size(attr))
}
