package engine

import (
	log "github.com/sirupsen/logrus"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/catalog"
)



func (db *database) Type() int {
	return db.typ
}

func (db *database) Delete(name string) error {
	_, err :=  db.catalog.DropTable(db.id, name)
	return err
}

func (db *database) Create(name string, defs []engine.TableDef, pdef *engine.PartitionBy, _ *engine.DistributionBy, comment string) error {
	_, err := db.catalog.CreateTable(db.id, 0, name, comment, defs, pdef)
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
	tablets, err := db.catalog.GetTablets(db.id, name)
	if err != nil {
		return nil, err
	}
	if tablets == nil || len(tablets) == 0 {
		return nil, catalog.ErrTableNotExists
	}

	r := &relation{
		pid:     db.id,
		tbl:     &tablets[0].Table,
		catalog: db.catalog,
	}
	for _, tbl := range tablets {
		if tR, err := db.catalog.Store.Relation(tbl.Name); err != nil {
			log.Errorf("Generate relation for tablet %s failed, %s", tbl.Name, err.Error())
		}else {
			r.tablets = append(r.tablets, tR)
		}
	}
	return r, nil
}

