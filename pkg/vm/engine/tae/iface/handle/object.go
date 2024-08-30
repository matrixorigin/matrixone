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

type FilterOp int16

const (
	FilterEq FilterOp = iota
	FilterBatchEq
	FilterBtw
)

type Filter struct {
	Op  FilterOp
	Val any
}

type DeleteType int8

const (
	DT_Normal DeleteType = iota
	DT_MergeCompact
)

func NewEQFilter(v any) *Filter {
	return &Filter{
		Op:  FilterEq,
		Val: v,
	}
}

type ObjectIt interface {
	BtreeIterator
	GetObject() Object
}

type ObjectReader interface {
	io.Closer
	GetID() *types.Objectid
	IsUncommitted() bool
	IsAppendable() bool
	Fingerprint() *common.ID
	// GetByFilter(filter Filter, offsetOnly bool) (map[uint64]*batch.Batch, error)
	String() string
	GetMeta() any
	Scan(
		ctx context.Context,
		bat **containers.Batch,
		blkID uint16,
		colIdxes []int,
		mp *mpool.MPool,
	) (err error)
	HybridScan(
		ctx context.Context,
		bat **containers.Batch,
		blkOffset uint16,
		colIdxs []int,
		mp *mpool.MPool,
	) error
	GetRelation() Relation

	BatchDedup(pks containers.Vector) error
	Prefetch(idxes []int) error
	BlkCnt() int
}

type ObjectWriter interface {
	io.Closer
	String() string

	PushDeleteOp(filter Filter) error
	PushUpdateOp(filter Filter, attr string, val any) error

	UpdateStats(objectio.ObjectStats) error
}

type Object interface {
	ObjectReader
	ObjectWriter
}
