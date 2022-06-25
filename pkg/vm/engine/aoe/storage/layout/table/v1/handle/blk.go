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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
)

var (
	_ dbi.IBlock = (*Block)(nil)
)

type Block struct {
	Host *Segment
	Id   uint64
}

func (blk *Block) Prefetch() dbi.IBatchReader {
	realBlk := blk.Host.Data.StrongRefBlock(blk.Id)
	defer realBlk.Unref()
	return realBlk.GetBatch(blk.Host.Attr)
}

func (blk *Block) GetID() uint64 {
	return blk.Id
}

func (blk *Block) GetSegmentID() uint64 {
	realBlk := blk.Host.Data.StrongRefBlock(blk.Id)
	defer realBlk.Unref()
	return realBlk.GetMeta().Segment.Id
}

func (blk *Block) GetTableID() uint64 {
	realBlk := blk.Host.Data.StrongRefBlock(blk.Id)
	defer realBlk.Unref()
	return realBlk.GetMeta().Segment.Table.Id
}
