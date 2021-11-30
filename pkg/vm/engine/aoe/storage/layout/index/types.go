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
