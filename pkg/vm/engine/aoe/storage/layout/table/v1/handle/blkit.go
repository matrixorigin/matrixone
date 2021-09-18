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

package handle

import (
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
)

var (
	_ dbi.IBlockIt = (*BlockIt)(nil)
)

type BlockIt struct {
	Segment *Segment
	Ids     []uint64
	Pos     int
}

func NewBlockIt(segment *Segment, blkIds []uint64) dbi.IBlockIt {
	it := &BlockIt{
		Ids:     blkIds,
		Segment: segment,
	}
	return it
}

func (it *BlockIt) GetHandle() dbi.IBlock {
	h := &Block{
		// DataSource: it.Segment.DataSource.WeakRefBlock(it.Ids[it.Pos]),
		Id:   it.Ids[it.Pos],
		Host: it.Segment,
	}
	return h
}

func (it *BlockIt) Valid() bool {
	if it.Segment == nil {
		return false
	}
	if it.Pos >= len(it.Ids) {
		return false
	}
	return true
}

func (it *BlockIt) Next() {
	it.Pos++
}

func (it *BlockIt) Close() error {
	return nil
}
