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
	"io"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type SegmentIt interface {
	Iterator
	GetSegment() Segment
}

type SegmentReader interface {
	io.Closer
	GetID() uint64
	IsUncommitted() bool
	MakeBlockIt() BlockIt
	// GetByFilter(filter Filter, offsetOnly bool) (map[uint64]*batch.Batch, error)
	String() string
	GetMeta() any

	GetBlock(id uint64) (Block, error)
	GetRelation() Relation

	BatchDedup(pks containers.Vector) error
}

type SegmentWriter interface {
	io.Closer
	String() string
	Update(blk uint64, row uint32, col uint16, v any) error
	RangeDelete(blk uint64, start, end uint32, dt DeleteType) error

	PushDeleteOp(filter Filter) error
	PushUpdateOp(filter Filter, attr string, val any) error

	CreateBlock(bool) (Block, error)
	CreateNonAppendableBlock() (Block, error)
	CreateNonAppendableBlockWithMeta(string, string) (Block, error)

	SoftDeleteBlock(id uint64) (err error)
}

type Segment interface {
	SegmentReader
	SegmentWriter
}
