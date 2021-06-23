package index

import (
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"

	"github.com/pilosa/pilosa/roaring"
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
}
