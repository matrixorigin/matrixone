package memEngine

import (
	"fmt"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/memEngine/kv"
	"matrixone/pkg/vm/metadata"
	"runtime"
)

func New(db *kv.KV, n metadata.Node) *memEngine {
	return &memEngine{
		n:  n,
		db: db,
	}
}

func (e *memEngine) Delete(_ uint64, _ string) error {
	return nil
}

func (e *memEngine) Create(_ uint64, _ string, _ int) error {
	return nil
}

func (e *memEngine) Databases() []string {
	return []string{"test"}
}

func (e *memEngine) Database(name string) (engine.Database, error) {
	if name != "test" {
		return nil, fmt.Errorf("database '%s' not exist", name)
	}
	return &database{db: e.db, n: e.n}, nil
}

func (e *memEngine) Node(_ string) *engine.NodeInfo {
	return &engine.NodeInfo{Mcpu: runtime.NumCPU()}
}
