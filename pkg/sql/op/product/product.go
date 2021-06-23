package product

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/op"
)

func New(r, s op.OP) *Product {
	attrs := make(map[string]types.Type)
	{
		rname := r.Name()
		rattrs := r.Attribute()
		if len(rname) > 0 {
			for k, v := range rattrs {
				attrs[rname+"."+k] = v
			}
		} else {
			for k, v := range rattrs {
				attrs[k] = v
			}
		}
	}
	{
		sname := s.Name()
		sattrs := s.Attribute()
		if len(sname) > 0 {
			for k, v := range sattrs {
				attrs[sname+"."+k] = v
			}
		} else {
			for k, v := range sattrs {
				attrs[k] = v
			}
		}
	}
	return &Product{
		R:     r,
		S:     s,
		Attrs: attrs,
	}
}

func (n *Product) Name() string {
	return n.ID
}

func (n *Product) String() string {
	return fmt.Sprintf("(%s) ⨯  (%s)", n.R, n.S)
}

func (n *Product) Rename(name string) {
	n.ID = name
}

func (n *Product) Columns() []string {
	return nil
}

func (n *Product) Attribute() map[string]types.Type {
	return n.Attrs
}
