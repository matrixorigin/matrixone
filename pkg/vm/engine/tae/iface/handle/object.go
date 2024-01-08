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
	"context"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type ObjectIt interface {
	Iterator
	GetObject() Object
}

type ObjectReader interface {
	io.Closer
	GetID() *types.Objectid
	IsUncommitted() bool
	IsAppendable() bool
	MakeBlockIt() BlockIt
	Fingerprint() *common.ID
	// GetByFilter(filter Filter, offsetOnly bool) (map[uint64]*batch.Batch, error)
	String() string
	GetMeta() any
	GetByFilter(ctx context.Context, filter *Filter, mp *mpool.MPool) (uint16, uint32, error)
	GetColumnDataByNames(ctx context.Context, blkID uint16, attrs []string, mp *mpool.MPool) (*containers.BlockView, error)
	GetColumnDataByIds(ctx context.Context, blkID uint16, colIdxes []int, mp *mpool.MPool) (*containers.BlockView, error)
	GetColumnDataByName(context.Context, uint16, string, *mpool.MPool) (*containers.ColumnView, error)
	GetColumnDataById(context.Context, uint16, int, *mpool.MPool) (*containers.ColumnView, error)

	GetRelation() Relation

	BatchDedup(pks containers.Vector) error
	Prefetch(idxes []int) error
}

type ObjectWriter interface {
	io.Closer
	String() string
	Update(blk uint64, row uint32, col uint16, v any) error
	RangeDelete(blk uint16, start, end uint32, dt DeleteType, mp *mpool.MPool) error

	PushDeleteOp(filter Filter) error
	PushUpdateOp(filter Filter, attr string, val any) error

	// create a appendable block, its id will be <objid>-<nextObjectid>-0
	CreateBlock(bool) (Block, error)
	// create a non-appendable block, instructed by CreateBlockOpt.
	// CreateBlockOpt can be nil, and the created block's id
	// will be <objid>-<nextObjectid>-0
	CreateNonAppendableBlock(*objectio.CreateBlockOpt) (Block, error)

	SoftDeleteBlock(id types.Blockid) (err error)
	UpdateStats(objectio.ObjectStats) error
}

type Object interface {
	ObjectReader
	ObjectWriter
}
