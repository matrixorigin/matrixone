package memtable

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type collection struct {
	common.RefHelper
	mgr  *manager
	data iface.ITableData
}

func newCollection(mgr *manager, data iface.ITableData) *collection {
	c := &collection{
		mgr:  mgr,
		data: data,
	}
	c.Ref()
	c.OnZeroCB = c.close
	return c
}

func (c *collection) close() {
	// TODO
}

func (c *collection) String() string {
	// TODO
	return ""
}

func (c *collection) Append(bat *batch.Batch, index *metadata.LogIndex) (err error) {
	// TODO
	return err
}
