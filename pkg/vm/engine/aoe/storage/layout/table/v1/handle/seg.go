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
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
)

var (
	_ dbi.ISegment = (*Segment)(nil)
)

type Segment struct {
	Data iface.ISegment
	Attr []int
}

func (seg *Segment) BlockIds() []uint64 {
	return seg.Data.BlockIds()
}

func (seg *Segment) GetID() uint64 {
	return seg.Data.GetMeta().ID
}

func (seg *Segment) GetTableID() uint64 {
	return seg.Data.GetMeta().Table.ID
}

func (seg *Segment) NewIt() dbi.IBlockIt {
	it := &BlockIt{
		Segment: seg,
		Ids:     seg.Data.BlockIds(),
	}
	return it
}

func (seg *Segment) GetBlock(id uint64) dbi.IBlock {
	data := seg.Data.WeakRefBlock(id)
	if data == nil {
		return nil
	}
	blk := &Block{
		Id: id,
		// DataSource: data,
		Host: seg,
	}
	return blk
}
