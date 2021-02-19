package memEngine

import (
	"matrixbase/pkg/encoding"
	"matrixbase/pkg/mempool"
	"matrixbase/pkg/vm/engine"
	"matrixbase/pkg/vm/engine/memEngine/kv"
	"matrixbase/pkg/vm/engine/memEngine/meta"
	"matrixbase/pkg/vm/metadata"
)

func New(db *kv.KV) *memEngine {
	return &memEngine{db, mempool.New(4<<20, 16)}
}

func (e *memEngine) Delete(name string) error {
	return e.db.Del(name)
}

func (e *memEngine) Create(name string, attrs []metadata.Attribute) error {
	var md meta.Metadata

	md.Name = name
	md.Attrs = attrs
	data, err := encoding.Encode(md)
	if err != nil {
		return err
	}
	return e.db.Set(name, data)
}

func (e *memEngine) Relations() []engine.Relation {
	return nil
}

func (e *memEngine) Relation(name string) (engine.Relation, error) {
	var md meta.Metadata

	data, err := e.db.Get(name, e.mp)
	if err != nil {
		return nil, err
	}
	defer e.mp.Free(data)
	if err := encoding.Decode(data[mempool.CountSize:], &md); err != nil {
		return nil, err
	}
	return &relation{name, e.db, md}, nil
}
