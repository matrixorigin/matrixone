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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

type BlockIt interface {
	Iterator
	GetBlock() Block
}

type FilterOp int16

type MetaUpdateNode interface{}

const (
	FilterEq FilterOp = iota
	FilterBatchEq
	FilterBtw
)

type Filter struct {
	Op  FilterOp
	Val any
}

func NewEQFilter(v any) *Filter {
	return &Filter{
		Op:  FilterEq,
		Val: v,
	}
}

type BlockReader interface {
	io.Closer
	ID() types.Blockid
	String() string
	IsUncommitted() bool
	GetByFilter(ctx context.Context, filter *Filter) (uint32, error)
	GetColumnDataByNames(ctx context.Context, attrs []string) (*model.BlockView, error)
	GetColumnDataByIds(ctx context.Context, colIdxes []int) (*model.BlockView, error)
	GetColumnDataByName(context.Context, string) (*model.ColumnView, error)
	GetColumnDataById(context.Context, int) (*model.ColumnView, error)
	GetMeta() any
	GetMetaLoc() objectio.Location
	GetDeltaLoc() objectio.Location
	Fingerprint() *common.ID
	Rows() int
	Prefetch(idxes []uint16) error
	// Why need rowmask?
	// We don't update the index until committing the transaction. Before that, even if we deleted a row
	// from a block, the index would not change. If then we insert a row with the same primary key as the
	// previously deleted row, there will be an deduplication error (unexpected!).
	// Here we use the rowmask to ingore any deduplication error on those deleted rows.
	// BatchDedup(col containers.Vector, invisibility *roaring.Bitmap) error

	IsAppendableBlock() bool

	GetSegment() Segment

	GetTotalChanges() int
}

type BlockWriter interface {
	io.Closer
	Append(data *containers.Batch, offset uint32) (uint32, error)
	Update(row uint32, col uint16, v any) error
	RangeDelete(start, end uint32, dt DeleteType) error
	UpdateMetaLoc(metaLoc objectio.Location) error
	UpdateDeltaLoc(deltaLoc objectio.Location) error

	PushDeleteOp(filter Filter) error
	PushUpdateOp(filter Filter, attr string, val any) error
}

type Block interface {
	BlockReader
	BlockWriter
}

type DeleteType int8

const (
	DT_Normal DeleteType = iota
	DT_MergeCompact
)
