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

package sched

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

// Scope is the scope of influenced space.
type Scope struct {
	mask *roaring.Bitmap
}

// Context is the general context of event.
type Context struct {
	// Options of DB
	Opts     *e.Options
	// Callback on event done
	DoneCB   ops.OpDoneCB
	// If the event is waitable
	Waitable bool
	// Influence scope
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
