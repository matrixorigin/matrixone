package engine

import (
	"matrixone/pkg/vm/engine"
)

func (db *database) Delete(name string) error {
	_, err :=  db.catalog.DropTable(db.id, name)
	return err
}

func (db *database) Create(name string, defs []engine.TableDef, pdef *engine.PartitionBy, _ *engine.DistributionBy) error {
	_, err := db.catalog.CreateTable(db.id, 0, name, "", defs, pdef)
	return err
}

func (db *database) Relations() []string {
	var rs []string
	tbs, err := db.catalog.GetTables(db.id)
	if err != nil {
		return rs
	}
	for _, tb := range tbs {
		rs = append(rs, tb.Name)
	}
	return nil
}

func (db *database) Relation(name string) (engine.Relation, error) {
	tb, err := db.catalog.GetTable(db.id, name)
	if err != nil {
		return nil, nil
	}
	return &relation{
		pid: db.id,
		id: tb.Id,
		catalog: db.catalog,
	}, nil
}

