package local

import (
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/storage/db"
)

type localRoDatabase struct {
	dbimpl *db.DB
}

func NewLocalRoDatabase(dbimpl *db.DB) *localRoDatabase {
	return &localRoDatabase{
		dbimpl: dbimpl,
	}
}

func (d *localRoDatabase) Type() int {
	panic("not supported")
}

func (d *localRoDatabase) Relations() []string {
	return d.dbimpl.TableNames()
}

func (d *localRoDatabase) Relation(name string) (engine.Relation, error) {
	impl, err := d.dbimpl.Relation(name)
	if err != nil {
		return nil, err
	}
	return NewLocalRoRelation(impl), nil
}

func (d *localRoDatabase) Delete(string) error {
	panic("not supported")
}

func (d *localRoDatabase) Create(string, []engine.TableDef, *engine.PartitionBy, *engine.DistributionBy, string) error {
	panic("not supported")
}
