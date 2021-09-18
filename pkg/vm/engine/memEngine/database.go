package memEngine

import (
	"bytes"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/memEngine/meta"
)

func (d *database) Type() int {
	return 0
}

func (d *database) Relations() []string {
	return nil
}

func (d *database) Relation(name string) (engine.Relation, error) {
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

func (d *database) Delete(_ uint64, _ string) error {
	return nil
}

func (d *database) Create(_ uint64, name string, defs []engine.TableDef, _ *engine.PartitionBy, _ *engine.DistributionBy, _ string) error {
	var md meta.Metadata

	for _, def := range defs {
		md.Attrs = append(md.Attrs, def.(*engine.AttributeDef).Attr)
	}
	data, err := encoding.Encode(md)
	if err != nil {
		return err
	}
	return d.db.Set(name, data)
}
