package memEngine

import (
	"bytes"
	"fmt"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/memEngine/meta"
	"strings"
)

func (d *database) Type() int {
	return 0
}

func (d *database) Relations() []string {
	var relations []string
	for _, key := range d.db.Keys() {
		if strings.HasPrefix(key, fmt.Sprintf("%s.", d.id)) {
			relations = append(relations, key)
		}
	}
	return relations
}

func (d *database) Relation(name string) (engine.Relation, error) {
	var md meta.Metadata
	var buf bytes.Buffer

	data, err := d.db.Get(fmt.Sprintf("%s.%s", d.id, name), &buf)
	if err != nil {
		return nil, err
	}
	if err := encoding.Decode(data, &md); err != nil {
		return nil, err
	}
	return &relation{rid: d.id, id: name, db: d.db, n: d.n, md: md}, nil

}

func (d *database) Delete(_ uint64, name string) error {
	handle := fmt.Sprintf("%s.%s", d.id, name)
	var remove []string
	for _, k := range d.db.Keys() {
		if strings.HasPrefix(k,handle) {
			remove = append(remove,k)
		}
	}
	for _, r := range remove {
		d.db.Del(r)
	}
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
	return d.db.Set(fmt.Sprintf("%s.%s", d.id, name), data)
}
