package moengine

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
)

var (
	_ engine.Relation = (*txnRelation)(nil)
)

func newRelation(h handle.Relation) *txnRelation {
	return &txnRelation{
		handle: h,
	}
}

func (rel *txnRelation) ID() string {
	return rel.handle.GetMeta().(*catalog.TableEntry).GetSchema().Name
}

func (rel *txnRelation) Close() {}

func (rel *txnRelation) Nodes() (nodes engine.Nodes) {
	return
}

func (_ *txnRelation) Size(_ string) int64 {
	return 0
}

func (_ *txnRelation) CardinalNumber(_ string) int64 {
	return 0
}

func (_ *txnRelation) CreateIndex(_ uint64, _ []engine.TableDef) error {
	panic("implement me")
}

func (_ *txnRelation) DropIndex(_ uint64, _ string) error {
	panic("implement me")
}

func (_ *txnRelation) AddTableDef(u uint64, def engine.TableDef) error {
	panic("implement me")
}

func (_ *txnRelation) DelTableDef(u uint64, def engine.TableDef) error {
	panic("implement me")
}

func (_ *txnRelation) TableDefs() []engine.TableDef {
	panic("implement me")
}

func (rel *txnRelation) Rows() int64 {
	return rel.handle.Rows()
}

func (_ *txnRelation) Index() []*engine.IndexTableDef {
	panic("implement me")
}

func (rel *txnRelation) Attribute() []engine.Attribute {
	meta := rel.handle.GetMeta().(*catalog.TableEntry)
	attrs := make([]engine.Attribute, len(meta.GetSchema().ColDefs))
	for idx, attr := range attrs {
		attr.Name = meta.GetSchema().ColDefs[idx].Name
		attr.Type = meta.GetSchema().ColDefs[idx].Type
		attrs[idx] = attr
	}
	return attrs
}

func (rel *txnRelation) Write(_ uint64, bat *batch.Batch) error {
	return rel.handle.Append(bat)
}

func (rel *txnRelation) NewReader(num int) (rds []engine.Reader) {
	// it := newReaderIt(rel.handle)
	it := rel.handle.MakeBlockIt()
	for i := 0; i < num; i++ {
		reader := newReader(rel.handle, it)
		rds = append(rds, reader)
	}
	return
}
