package memEngine

import (
	"bytes"
	"errors"
	"fmt"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/common/codec"
	"matrixone/pkg/vm/engine/memEngine/kv"
	"matrixone/pkg/vm/metadata"
	"runtime"
	"strings"
)

func New(db *kv.KV, n metadata.Node) *memEngine {
	return &memEngine{
		n:  n,
		db: db,
	}
}

func (e *memEngine) Delete(_ uint64, name string) error {
	_, err := e.db.Get(name, bytes.NewBuffer([]byte{}))
	if err != nil {
		return err
	}
	return e.db.Del(name)
}

func (e *memEngine) Create(_ uint64, name string, _ int) error {
	_, err := e.db.Get(name, bytes.NewBuffer([]byte{}))
	if err == nil {
		return errors.New("already existed")
	}
	return e.db.Set(name, codec.Uint642Bytes(uint64(e.db.Size()+1)))
}

func (e *memEngine) Databases() []string {
	var dbs []string
	for _, key := range e.db.Keys() {
		if strings.Contains(key, ".") {
			continue
		}
		dbs = append(dbs, key)
	}
	return dbs
}

func (e *memEngine) Database(name string) (engine.Database, error) {
	_, err := e.db.Get(name, bytes.NewBuffer([]byte{}))
	if err != nil {
		return nil, fmt.Errorf("database '%s' not exist", name)
	}
	return &database{id: name, db: e.db, n: e.n}, nil
}

func (e *memEngine) Node(_ string) *engine.NodeInfo {
	return &engine.NodeInfo{Mcpu: runtime.NumCPU()}
}
