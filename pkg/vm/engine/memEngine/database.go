package memEngine

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memEngine/meta"
)

func (d *database) Relations(_ engine.Snapshot) []string {
	names, _ := d.db.Range()
	return names
}

func (d *database) Relation(name string, _ engine.Snapshot) (engine.Relation, error) {
	var md meta.Metadata
	var buf bytes.Buffer

	data, err := d.db.Get(name, &buf)
	if err != nil {
		return nil, err
	}
	if err := encoding.Decode(data, &md); err != nil {
		return nil, err
	}
	return &relation{id: name, db: d.db, n: d.n, md: md}, nil
}

func (d *database) Delete(_ uint64, _ string, _ engine.Snapshot) error {
	return nil
}

func (d *database) Create(_ uint64, name string, defs []engine.TableDef, _ engine.Snapshot) error {
	var md meta.Metadata

	for _, def := range defs {
		switch d := def.(type) {
		case *engine.AttributeDef:
			md.Attrs = append(md.Attrs, d.Attr)
		case *engine.IndexTableDef:
			md.Index = append(md.Index, engine.IndexTableDef{Typ: d.Typ, ColNames: d.ColNames, Name: d.Name})
		}
	}
	data, err := encoding.Encode(md)
	if err != nil {
		return err
	}
	return d.db.Set(name, data)
}
