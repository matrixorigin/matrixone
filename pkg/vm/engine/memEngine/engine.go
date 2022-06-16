package memEngine

import (
	"fmt"
	"runtime"

	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memEngine/kv"
)

func New(db *kv.KV, n engine.Node) *memEngine {
	return &memEngine{
		n:  n,
		db: db,
	}
}

func (e *memEngine) Delete(_ uint64, _ string, _ engine.Snapshot) error {
	return nil
}

func (e *memEngine) Create(_ uint64, _ string, _ int, _ engine.Snapshot) error {
	return nil
}

func (e *memEngine) Databases(_ engine.Snapshot) []string {
	return []string{"test"}
}

func (e *memEngine) Database(name string, _ engine.Snapshot) (engine.Database, error) {
	if name != "test" {
		return nil, fmt.Errorf("database '%s' not exist", name)
	}
	return &database{db: e.db, n: e.n}, nil
}

func (e *memEngine) Node(_ string, _ engine.Snapshot) *engine.NodeInfo {
	return &engine.NodeInfo{Mcpu: runtime.NumCPU()}
}
