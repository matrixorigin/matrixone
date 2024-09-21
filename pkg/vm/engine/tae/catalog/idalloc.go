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

type IDAllocator struct {
	dbAlloc  *common.IdAllocator
	tblAlloc *common.IdAllocator
	objAlloc *common.IdAllocator
	blkAlloc *common.IdAllocator
}

func NewIDAllocator() *IDAllocator {
	return &IDAllocator{
		dbAlloc:  common.NewIdAllocator(1000),
		tblAlloc: common.NewIdAllocator(1000),
		objAlloc: common.NewIdAllocator(1000),
		blkAlloc: common.NewIdAllocator(1000),
	}
}

func (alloc *IDAllocator) Init(prevDb, prevTbl, prevObj, prevBlk uint64) {
	alloc.dbAlloc.SetStart(prevDb)
	alloc.tblAlloc.SetStart(prevTbl)
	alloc.objAlloc.SetStart(prevObj)
	alloc.blkAlloc.SetStart(prevBlk)
}

func (alloc *IDAllocator) NextDB() uint64     { return alloc.dbAlloc.Alloc() }
func (alloc *IDAllocator) NextTable() uint64  { return alloc.tblAlloc.Alloc() }
func (alloc *IDAllocator) NextObject() uint64 { return alloc.objAlloc.Alloc() }
func (alloc *IDAllocator) NextBlock() uint64  { return alloc.blkAlloc.Alloc() }

func (alloc *IDAllocator) CurrDB() uint64     { return alloc.dbAlloc.Get() }
func (alloc *IDAllocator) CurrTable() uint64  { return alloc.tblAlloc.Get() }
func (alloc *IDAllocator) CurrObject() uint64 { return alloc.objAlloc.Get() }
func (alloc *IDAllocator) CurrBlock() uint64  { return alloc.blkAlloc.Get() }

func (alloc *IDAllocator) OnReplayBlockID(id uint64) {
	if alloc.CurrBlock() < id {
		alloc.blkAlloc.SetStart(id)
	}
}

func (alloc *IDAllocator) OnReplayObjectID(id uint64) {
	if alloc.CurrObject() < id {
		alloc.objAlloc.SetStart(id)
	}
}
func (alloc *IDAllocator) OnReplayTableID(id uint64) {
	if alloc.CurrTable() < id {
		alloc.tblAlloc.SetStart(id)
	}
}
func (alloc *IDAllocator) OnReplayDBID(id uint64) {
	if alloc.CurrDB() < id {
		alloc.dbAlloc.SetStart(id)
	}
}

func (alloc *IDAllocator) IDStates() string {
	return fmt.Sprintf("Current DBID=%d,TID=%d,SID=%d,BID=%d",
		alloc.CurrDB(), alloc.CurrTable(), alloc.CurrObject(), alloc.CurrBlock())
}
