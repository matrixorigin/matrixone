package engine

import (
	"matrixone/pkg/vm/engine"
)

func (db *database) Delete(name string) error {
	_, err :=  db.catalog.DropTable(db.name, name)
	return err
}

func (db *database) Create(name string, defs []engine.TableDef, pdef *engine.PartitionBy, _ *engine.DistributionBy) error {
	_, err := db.catalog.CreateTable(db.name, name, "",0, defs, pdef)
	return err
}

func (db *database) Relations() []string {
	var rs []string
	tbs, err := db.catalog.GetTables(db.name)
	if err != nil {
		return rs
	}
	for _, tb := range tbs {
		rs = append(rs, tb.Name)
	}
	return nil
}

func (db *database) Relation(name string) (engine.Relation, error) {
	tb, err := db.catalog.GetTable(db.name, name)
	if err != nil {
		return nil, nil
	}
	return &relation{
		dbName: db.name,
		name: tb.Name,
		catalog: db.catalog,
	}, nil
}

