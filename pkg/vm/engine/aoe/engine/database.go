package engine

import (
	"matrixone/pkg/vm/engine"
)

func (e *database) Delete(name string) error {
	return nil
}

func (e *database) Create(name string, defs []engine.TableDef, _ *engine.PartitionBy, _ *engine.DistributionBy) error {
	return nil
}

func (e *database) Relations() []string {
	return nil
}

func (e *database) Relation(name string) (engine.Relation, error) {
	return nil, nil
}

