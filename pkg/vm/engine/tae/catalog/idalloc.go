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

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"

type IDAlloctor struct {
	dbAlloc  *common.IdAlloctor
	tblAlloc *common.IdAlloctor
	segAlloc *common.IdAlloctor
	blkAlloc *common.IdAlloctor
}

func NewIDAllocator() *IDAlloctor {
	return &IDAlloctor{
		dbAlloc:  common.NewIdAlloctor(1000),
		tblAlloc: common.NewIdAlloctor(1000),
		segAlloc: common.NewIdAlloctor(1000),
		blkAlloc: common.NewIdAlloctor(1000),
	}
}

func (alloc *IDAlloctor) Init(prevDb, prevTbl, prevSeg, prevBlk uint64) {
	alloc.dbAlloc.SetStart(prevDb)
	alloc.tblAlloc.SetStart(prevTbl)
	alloc.segAlloc.SetStart(prevSeg)
	alloc.blkAlloc.SetStart(prevBlk)
}

func (alloc *IDAlloctor) NextDB() uint64      { return alloc.dbAlloc.Alloc() }
func (alloc *IDAlloctor) NextTable() uint64   { return alloc.tblAlloc.Alloc() }
func (alloc *IDAlloctor) NextSegment() uint64 { return alloc.segAlloc.Alloc() }
func (alloc *IDAlloctor) NextBlock() uint64   { return alloc.blkAlloc.Alloc() }

func (alloc *IDAlloctor) CurrDB() uint64      { return alloc.dbAlloc.Get() }
func (alloc *IDAlloctor) CurrTable() uint64   { return alloc.tblAlloc.Get() }
func (alloc *IDAlloctor) CurrSegment() uint64 { return alloc.segAlloc.Get() }
func (alloc *IDAlloctor) CurrBlock() uint64   { return alloc.blkAlloc.Get() }

func (alloc *IDAlloctor) OnReplayBlockID(id uint64) {
	if alloc.CurrBlock() < id {
		alloc.blkAlloc.SetStart(id)
	}
}

func (alloc *IDAlloctor) OnReplaySegmentID(id uint64) {
	if alloc.CurrSegment() < id {
		alloc.segAlloc.SetStart(id)
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
