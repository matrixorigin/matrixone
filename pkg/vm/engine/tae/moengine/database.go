package moengine

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/helper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
)

var (
	_ engine.Database = (*txnDatabase)(nil)
)

func newDatabase(h handle.Database) *txnDatabase {
	return &txnDatabase{
		handle: h,
	}
}

func (db *txnDatabase) Relations() (names []string) {
	it := db.handle.MakeRelationIt()
	for it.Valid() {
		names = append(names, it.GetRelation().GetMeta().(*catalog.TableEntry).GetSchema().Name)
		it.Next()
	}
	return
}

func (db *txnDatabase) Relation(name string) (rel engine.Relation, err error) {
	h, err := db.handle.GetRelationByName(name)
	if err != nil {
		return
	}
	rel = newRelation(h)
	return
}

func (db *txnDatabase) Create(_ uint64, name string, defs []engine.TableDef) error {
	info, err := helper.Transfer(db.handle.GetID(), 0, 0, name, defs)
	if err != nil {
		return err
	}
	schema := TableInfoToSchema(&info)
	schema.BlockMaxRows = 40000
	schema.SegmentMaxBlocks = 20
	_, err = db.handle.CreateRelation(schema)
	return err
}

func (db *txnDatabase) Delete(_ uint64, name string) error {
	_, err := db.handle.DropRelationByName(name)
	return err
}
