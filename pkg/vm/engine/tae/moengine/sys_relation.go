package moengine

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
)

var (
	_ engine.Relation = (*sysRelation)(nil)
)

func newSysRelation(h handle.Relation) *sysRelation {
	r := &sysRelation{}
	r.handle = h
	r.nodes = append(r.nodes, engine.Node{
		Addr: ADDR,
	})
	return r
}

func isSysRelation(name string) bool {
	if name == catalog.SystemTable_DB_Name ||
		name == catalog.SystemTable_Table_Name ||
		name == catalog.SystemTable_Columns_Name {
		return true
	}
	return false
}

func (s sysRelation) Write(_ uint64, _ *batch.Batch, _ engine.Snapshot) error {
	return ErrReadOnly
}

func (s sysRelation) Update(_ uint64, _ *batch.Batch, _ engine.Snapshot) error {
	return ErrReadOnly
}

func (s sysRelation) Delete(_ uint64, _ *vector.Vector, _ string, _ engine.Snapshot) error {
	return ErrReadOnly
}
