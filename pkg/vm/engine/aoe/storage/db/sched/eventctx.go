package db

import (
	roaring "github.com/RoaringBitmap/roaring/roaring64"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/ops"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
)

type scopeType int

const (
	Scope_All scopeType = iota
	Scope_Meta
	Scope_Data
)

type Scope struct {
	mask *roaring.Bitmap
}

type Context struct {
	Opts     *e.Options
	DoneCB   ops.OpDoneCB
	Waitable bool
	Scope    *Scope
}

type BaseEvent struct {
	sched.BaseEvent
	Ctx *Context
}

func (ctx *Context) addScope(t scopeType) {
	if ctx.Scope == nil {
		ctx.Scope = &Scope{mask: roaring.NewBitmap()}
	}
	ctx.Scope.mask.Add(uint64(t))
}

func (ctx *Context) AddMetaScope() {
	ctx.addScope(Scope_Meta)
}

func (ctx *Context) AddDataScope() {
	ctx.addScope(Scope_Data)
}

func (ctx *Context) RemoveDataScope() {
	if ctx.Scope == nil {
		ctx.Scope = &Scope{mask: roaring.NewBitmap()}
		return
	}
	ctx.Scope.mask.Remove(uint64(Scope_Data))
}

func (ctx *Context) HasMetaScope() bool {
	if ctx.Scope == nil {
		return true
	}
	return ctx.Scope.mask.Contains(uint64(Scope_Meta))
}

func (ctx *Context) HasDataScope() bool {
	if ctx.Scope == nil {
		return true
	}
	return ctx.Scope.mask.Contains(uint64(Scope_Data))
}

func (ctx *Context) HasAllScope() bool {
	if ctx.Scope == nil {
		return true
	}
	return ctx.Scope.mask.Contains(uint64(Scope_Meta)) && ctx.Scope.mask.Contains(uint64(Scope_Data))
}
