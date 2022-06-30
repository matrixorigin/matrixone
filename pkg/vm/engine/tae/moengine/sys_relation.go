package moengine

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
)

var (
	_ engine.Relation = (*sysRelation)(nil)
)

func newSysRelation(h handle.Relation) *sysRelation {
	r := &sysRelation{
		handle: h,
	}
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

func (s sysRelation) Rows() int64 {
	return s.handle.Rows()
}

func (s sysRelation) Size(s2 string) int64 {
	return 0
}

func (s sysRelation) Close(_ engine.Snapshot) {}

func (s sysRelation) ID(_ engine.Snapshot) string {
	return s.handle.GetMeta().(*catalog.TableEntry).GetSchema().Name
}

func (s sysRelation) Nodes(_ engine.Snapshot) engine.Nodes {
	return s.nodes
}

func (s sysRelation) TableDefs(_ engine.Snapshot) []engine.TableDef {
	schema := s.handle.GetMeta().(*catalog.TableEntry).GetSchema()
	defs, _ := SchemaToDefs(schema)
	return defs
}

func (s sysRelation) GetPrimaryKeys(_ engine.Snapshot) (attrs []*engine.Attribute) {
	schema := s.handle.GetMeta().(*catalog.TableEntry).GetSchema()
	if !schema.HasPK() {
		return
	}
	for _, def := range schema.SortKey.Defs {
		attr := new(engine.Attribute)
		attr.Name = def.Name
		attr.Type = def.Type
		attrs = append(attrs, attr)
	}
	logutil.Debugf("GetPrimaryKeys: %v", attrs[0])
	return
}

func (s sysRelation) GetHideKey(_ engine.Snapshot) *engine.Attribute {
	schema := s.handle.GetMeta().(*catalog.TableEntry).GetSchema()
	key := new(engine.Attribute)
	key.Name = schema.HiddenKey.Name
	key.Type = schema.HiddenKey.Type
	logutil.Debugf("GetHideKey: %v", key)
	return key
}

func (s sysRelation) GetPriKeyOrHideKey(_ engine.Snapshot) ([]engine.Attribute, bool) {
	schema := s.handle.GetMeta().(*catalog.TableEntry).GetSchema()
	attrs := make([]engine.Attribute, 1)
	attrs[0].Name = schema.HiddenKey.Name
	attrs[0].Type = schema.HiddenKey.Type
	return attrs, false
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

func (s sysRelation) AddTableDef(_ uint64, _ engine.TableDef, _ engine.Snapshot) error {
	panic(any("implement me"))
}

func (s sysRelation) DelTableDef(_ uint64, _ engine.TableDef, _ engine.Snapshot) error {
	panic(any("implement me"))
}

func (s sysRelation) NewReader(num int, _ extend.Extend, _ []byte, _ engine.Snapshot) (rds []engine.Reader) {
	it := s.handle.MakeBlockIt()
	for i := 0; i < num; i++ {
		reader := newReader(s.handle, it)
		rds = append(rds, reader)
	}
	return
}
