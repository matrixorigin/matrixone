package local

import (
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/storage/db"
)

type localRoEngine struct {
	dbimpl *db.DB
}

func NewLocalRoEngine(dbimpl *db.DB) *localRoEngine {
	return &localRoEngine{
		dbimpl: dbimpl,
	}
}

func (e *localRoEngine) Node(_ string) *engine.NodeInfo {
	panic("not supported")
}

func (e *localRoEngine) Delete(name string) error {
	panic("not supported")
}

func (e *localRoEngine) Create(_ string, _ int) error {
	panic("not supported")
}

func (e *localRoEngine) Databases() []string {
	panic("not supported")
}

func (e *localRoEngine) Database(_ string) (engine.Database, error) {
	return NewLocalRoDatabase(e.dbimpl), nil
}
