package engine

import (
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/catalog"
)

func New() *aoeEngine {
	//1. Parse config
	//2. New Storage
	//3. New Catalog
	return &aoeEngine{}
}

func Mock(c *catalog.Catalog) *aoeEngine {
	//1. Parse config
	//2. New Storage
	//3. New Catalog
	return &aoeEngine{
		catalog: c,
	}
}

func (e *aoeEngine) Node(_ string) *engine.NodeInfo {
	return nil
}

func (e *aoeEngine) Delete(name string) error {
	_, err := e.catalog.DelDatabase(name)
	return err
}

func (e *aoeEngine) Create(name string, typ int) error {
	_, err := e.catalog.CreateDatabase(name, typ)
	return err
}

func (e *aoeEngine) Databases() []string {
	var ds []string
	if dbs, err := e.catalog.GetDBs(); err == nil {
		for _, db := range dbs {
			ds = append(ds, db.Name)
		}
	}
	return ds
}

func (e *aoeEngine) Database(name string) (engine.Database, error) {
	db, err := e.catalog.GetDB(name)
	if err != nil {
		return nil, err
	}
	return &database{
		id:      db.Id,
		typ:     db.Type,
		catalog: e.catalog,
	}, nil
}
