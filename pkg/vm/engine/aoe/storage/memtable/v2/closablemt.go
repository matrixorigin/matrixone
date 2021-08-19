package memtable

import (
	engine "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	// "matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"sync"
)

type mtHandle struct {
	common.RefHelper
	mu     sync.RWMutex
	loaded bool
}

type closableMemtable struct {
	sync.RWMutex
	common.RefHelper
	Opts      *engine.Options
	TableData iface.ITableData
	Meta      *md.Block
	Block     iface.IBlock
	Handle    *mtHandle
	File      *dataio.TransientBlockFile
}

func NewClosableMemtable(opts *engine.Options, tableData iface.ITableData, data iface.IBlock) *closableMemtable {
	mt := &closableMemtable{
		Opts:      opts,
		TableData: tableData,
		Block:     data,
		Meta:      data.GetMeta(),
		Handle:    &mtHandle{},
		// File: data.GetFileName
	}
	mt.OnZeroCB = mt.close
	mt.Ref()
	return mt
}

func (mt *closableMemtable) GetID() common.ID {
	return mt.Meta.AsCommonID().AsBlockID()
}

func (mt *closableMemtable) close() {
	// TODO
}

func (mt *closableMemtable) String() string {
	// TODO
	return ""
}

func (mt *closableMemtable) Flush() error {
	// TODO
	return nil
}

func (mt *closableMemtable) GetMeta() *md.Block {
	return mt.Meta
}

func (mt *closableMemtable) Unpin() {
	// TODO
}

func (mt *closableMemtable) pin() {
	// TODO
}

func (mt *closableMemtable) IsFull() bool {
	return mt.Meta.IsFull()
}
