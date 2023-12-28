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

package catalog

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type IDAlloctor struct {
	dbAlloc  *common.IdAlloctor
	tblAlloc *common.IdAlloctor
	objAlloc *common.IdAlloctor
	blkAlloc *common.IdAlloctor
}

func NewIDAllocator() *IDAlloctor {
	return &IDAlloctor{
		dbAlloc:  common.NewIdAlloctor(1000),
		tblAlloc: common.NewIdAlloctor(1000),
		objAlloc: common.NewIdAlloctor(1000),
		blkAlloc: common.NewIdAlloctor(1000),
	}
}

func (alloc *IDAlloctor) Init(prevDb, prevTbl, prevObj, prevBlk uint64) {
	alloc.dbAlloc.SetStart(prevDb)
	alloc.tblAlloc.SetStart(prevTbl)
	alloc.objAlloc.SetStart(prevObj)
	alloc.blkAlloc.SetStart(prevBlk)
}

func (alloc *IDAlloctor) NextDB() uint64     { return alloc.dbAlloc.Alloc() }
func (alloc *IDAlloctor) NextTable() uint64  { return alloc.tblAlloc.Alloc() }
func (alloc *IDAlloctor) NextObject() uint64 { return alloc.objAlloc.Alloc() }
func (alloc *IDAlloctor) NextBlock() uint64  { return alloc.blkAlloc.Alloc() }

func (alloc *IDAlloctor) CurrDB() uint64     { return alloc.dbAlloc.Get() }
func (alloc *IDAlloctor) CurrTable() uint64  { return alloc.tblAlloc.Get() }
func (alloc *IDAlloctor) CurrObject() uint64 { return alloc.objAlloc.Get() }
func (alloc *IDAlloctor) CurrBlock() uint64  { return alloc.blkAlloc.Get() }

func (alloc *IDAlloctor) OnReplayBlockID(id uint64) {
	if alloc.CurrBlock() < id {
		alloc.blkAlloc.SetStart(id)
	}
}

func (alloc *IDAlloctor) OnReplayObjectID(id uint64) {
	if alloc.CurrObject() < id {
		alloc.objAlloc.SetStart(id)
	}
}
func (alloc *IDAlloctor) OnReplayTableID(id uint64) {
	if alloc.CurrTable() < id {
		alloc.tblAlloc.SetStart(id)
	}
}
func (alloc *IDAlloctor) OnReplayDBID(id uint64) {
	if alloc.CurrDB() < id {
		alloc.dbAlloc.SetStart(id)
	}
}

func (alloc *IDAlloctor) IDStates() string {
	return fmt.Sprintf("Current DBID=%d,TID=%d,SID=%d,BID=%d",
		alloc.CurrDB(), alloc.CurrTable(), alloc.CurrObject(), alloc.CurrBlock())
}
