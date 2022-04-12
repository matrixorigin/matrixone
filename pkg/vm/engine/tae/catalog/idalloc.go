package catalog

import "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"

type IDAlloctor struct {
	dbAlloc  *common.IdAlloctor
	tblAlloc *common.IdAlloctor
	segAlloc *common.IdAlloctor
	blkAlloc *common.IdAlloctor
}

func NewIDAllocator() *IDAlloctor {
	return &IDAlloctor{
		dbAlloc:  common.NewIdAlloctor(1),
		tblAlloc: common.NewIdAlloctor(1),
		segAlloc: common.NewIdAlloctor(1),
		blkAlloc: common.NewIdAlloctor(1),
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
