package engine

import (
	"fmt"
	"matrixone/pkg/vm/engine"
)

func New() *aoeEngine {
	//1. Parse config
	//2. New Storage
	//3. New Catalog
	return &aoeEngine{

	}
}

func (e *aoeEngine) Node(_ string) *engine.NodeInfo {
	return nil
}

func (e *aoeEngine) Delete(name string) error {

	return nil
}

func (e *aoeEngine) Create(name string) error {

	return nil
}

func (e *aoeEngine) Databases() []string {
	return nil
}

func (e *aoeEngine) Database(name string) (engine.Database, error) {
	return nil, fmt.Errorf("database '%s' not exist", name)
}