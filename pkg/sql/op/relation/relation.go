package relation

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/engine"
)

func New(name string, r engine.Relation) *Relation {
	var cols []string

	attrs := make(map[string]types.Type)
	{
		attrDefs := r.Attribute()
		for _, attr := range attrDefs {
			attrs[attr.Name] = attr.Type
			cols = append(cols, attr.Name)
		}
	}
	return &Relation{
		R:     r,
		Cols:  cols,
		Rid:   name,
		ID:    name,
		Attrs: attrs,
		Segs:  r.Segments(),
	}
}

func (n *Relation) Name() string {
	return n.ID
}

func (n *Relation) Rename(name string) {
	n.ID = name
}

func (n *Relation) String() string {
	return n.ID
}

func (n *Relation) Columns() []string {
	return n.Cols
}

func (n *Relation) Attribute() map[string]types.Type {
	return n.Attrs
}
