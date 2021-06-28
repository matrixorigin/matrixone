package memEngine

import (
	"fmt"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/memEngine/kv"
	"matrixone/pkg/vm/engine/memEngine/meta"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/mmu/host"
	"matrixone/pkg/vm/process"
	"runtime"
)

func New() *memEngine {
	return &memEngine{
		mp:   make(map[string]*database),
		proc: process.New(guest.New(1<<20, host.New(1<<20)), mempool.New(1<<32, 16)),
	}
}

func (e *memEngine) Node(_ string) *engine.NodeInfo {
	return &engine.NodeInfo{
		Mcpu: runtime.NumCPU(),
	}
}

func (e *memEngine) Delete(name string) error {
	delete(e.mp, name)
	return nil
}

func (e *memEngine) Create(name string) error {
	e.mp[name] = &database{
		proc: e.proc,
		db:   kv.New(),
		mp:   make(map[string]uint8),
	}
	return nil
}

func (e *memEngine) Databases() []string {
	var rs []string

	for k, _ := range e.mp {
		rs = append(rs, k)
	}
	return rs
}

func (e *memEngine) Database(name string) (engine.Database, error) {
	if db, ok := e.mp[name]; ok {
		return db, nil
	}
	return nil, fmt.Errorf("database '%s' not exist", name)
}

func (e *database) Delete(name string) error {
	delete(e.mp, name)
	return e.db.Del(name)
}

func (e *database) Create(name string, defs []engine.TableDef, _ *engine.PartitionBy, _ *engine.DistributionBy) error {
	var md meta.Metadata
	var attrs []metadata.Attribute

	{
		for _, def := range defs {
			v, ok := def.(*engine.AttributeDef)
			if ok {
				attrs = append(attrs, v.Attr)
			}
		}
	}
	md.Name = name
	md.Attrs = attrs
	data, err := encoding.Encode(md)
	if err != nil {
		return err
	}
	e.mp[name] = 0
	return e.db.Set(name, data)
}

func (e *database) Relations() []string {
	var rs []string

	for k, _ := range e.mp {
		rs = append(rs, k)
	}
	return rs
}

func (e *database) Relation(name string) (engine.Relation, error) {
	var md meta.Metadata

	data, err := e.db.Get(name, e.proc)
	if err != nil {
		return nil, err
	}
	defer e.proc.Free(data)
	if err := encoding.Decode(data[mempool.CountSize:], &md); err != nil {
		return nil, err
	}
	return &relation{name, e.db, md}, nil
}
