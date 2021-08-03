package laoe

import (
	"fmt"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/storage/db"
)

func (e *database) Type() int {
	return 0
}

func (e *database) Relations() []string {
	return nil
}

func (e *database) Relation(name string) (engine.Relation, error) {
	mp := make(map[string]*db.Relation)
	ss, ok := e.mp[name]
	if !ok {
		return nil, fmt.Errorf("relation '%s' not exist", name)
	}
	for _, s := range ss {
		if _, ok := mp[s.TabletId]; ok {
			continue
		}
		r, err := e.db.Relation(s.TabletId)
		if err != nil {
			for _, v := range mp {
				v.Close()
			}
			return nil, err
		}
		mp[s.TabletId] = r
	}
	return &relation{
		mp: mp,
		id: name,
		db: e.id,
	}, nil
}

func (e *database) Delete(_ string) error {
	return nil
}

func (e *database) Create(_ string, _ []engine.TableDef, _ *engine.PartitionBy, _ *engine.DistributionBy, _ string) error {
	return nil
}
