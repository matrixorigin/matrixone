package laoe

import (
	"fmt"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/storage/db"
)

func New(db *db.DB, mp map[string]map[string][]engine.SegmentInfo) *aoeEngine {
	return &aoeEngine{db: db, mp: mp}
}

func (e *aoeEngine) Node(_ string) *engine.NodeInfo {
	return nil
}

func (e *aoeEngine) Delete(_ uint64, _ string) error {
	return nil
}

func (e *aoeEngine) Create(_ uint64, _ string, _ int) error {
	return nil
}

func (e *aoeEngine) Databases() []string {
	return nil
}

func (e *aoeEngine) Database(name string) (engine.Database, error) {
	if _, ok := e.mp[name]; !ok {
		return nil, fmt.Errorf("database '%s' not exist", name)
	}
	return &database{
		id: name,
		db: e.db,
		mp: e.mp[name],
	}, nil
}
