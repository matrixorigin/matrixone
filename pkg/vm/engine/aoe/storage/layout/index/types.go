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

package index

import (
	"github.com/RoaringBitmap/roaring/roaring64"
	buf "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"

	"github.com/RoaringBitmap/roaring"
)

type OpType uint8

const (
	OpInv OpType = iota
	OpEq
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
	OpIn
	OpOut
)

type FilterCtx struct {
	Op  OpType
	Val interface{}

	// Used for IN | NOT IN
	ValSet map[interface{}]bool

	ValMin interface{}
	ValMax interface{}

	BoolRes bool
	BMRes   *roaring.Bitmap
	Err     error

	BsiRequired bool
}

func NewFilterCtx(t OpType) *FilterCtx {
	ctx := &FilterCtx{
		Op:     t,
		ValSet: make(map[interface{}]bool),
	}
	return ctx
}

func (ctx *FilterCtx) Reset() {
	ctx.Op = OpInv
	ctx.Val = nil
	for k := range ctx.ValSet {
		delete(ctx.ValSet, k)
	}
	ctx.ValMin = nil
	ctx.ValMax = nil
	ctx.BoolRes = false
	ctx.BMRes = nil
	ctx.Err = nil
}

func (ctx *FilterCtx) Eval(i Index) error {
	return i.Eval(ctx)
}

type Index interface {
	buf.IMemoryNode
	Type() base.IndexType
	GetCol() int16
	Eval(ctx *FilterCtx) error
	IndexFile() common.IVFile
}

type SegmentIndexHolder interface {
	Init(base.ISegmentFile)
	EvalFilter(int, *FilterCtx) error
	CollectMinMax(int) ([]interface{}, []interface{}, error)
	Count(int, *roaring64.Bitmap) (uint64, error)
	NullCount(int, *roaring64.Bitmap) (uint64, error)
	Min(int, *roaring64.Bitmap) (interface{}, error)
	Max(int, *roaring64.Bitmap) (interface{}, error)
	Sum(int, *roaring64.Bitmap) (int64, uint64, error)

	StrongRefBlock(uint64) *BlockHolder
	RegisterBlock(common.ID, base.BlockType, PostCloseCB) *BlockHolder
	DropBlock(uint64) *BlockHolder
	GetBlockCount() int32
	UpgradeBlock(uint64, base.BlockType) *BlockHolder
	stringNoLock() string

	AllocateVersion(int) uint64
    IndicesCount() int
	DropIndex(filename string)
	LoadIndex(base.ISegmentFile, string)
	StringIndicesRefsNoLock() string
	close()
}
