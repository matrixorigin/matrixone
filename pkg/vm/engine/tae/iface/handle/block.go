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
	"bytes"
	"io"

	"github.com/RoaringBitmap/roaring"
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
	ID() uint64
	String() string
	IsUncommitted() bool
	GetByFilter(filter *Filter) (uint32, error)
	GetColumnDataByNames(attrs []string, buffers []*bytes.Buffer) (*model.BlockView, error)
	GetColumnDataByIds(colIdxes []int, buffers []*bytes.Buffer) (*model.BlockView, error)
	GetColumnDataByName(string, *bytes.Buffer) (*model.ColumnView, error)
	GetColumnDataById(int, *bytes.Buffer) (*model.ColumnView, error)
	GetMeta() any
	GetMetaLoc() string
	GetDeltaLoc() string
	Fingerprint() *common.ID
	Rows() int

	// Why need rowmask?
	// We don't update the index until committing the transaction. Before that, even if we deleted a row
	// from a block, the index would not change. If then we insert a row with the same primary key as the
	// previously deleted row, there will be an deduplication error (unexpected!).
	// Here we use the rowmask to ingore any deduplication error on those deleted rows.
	BatchDedup(col containers.Vector, invisibility *roaring.Bitmap) error

	IsAppendableBlock() bool

	GetSegment() Segment

	GetTotalChanges() int
}

type BlockWriter interface {
	io.Closer
	Append(data *containers.Batch, offset uint32) (uint32, error)
	Update(row uint32, col uint16, v any) error
	RangeDelete(start, end uint32, dt DeleteType) error
	UpdateMetaLoc(metaLoc string) error
	UpdateDeltaLoc(deltaLoc string) error

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
