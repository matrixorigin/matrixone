package naturalJoin

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/op"
)

func New(r, s op.OP) *Join {
	pub := make([]string, 0, 1)
	attrs := make(map[string]types.Type)
	{
		rattrs := r.Attribute()
		for k, v := range rattrs {
			attrs[k] = v
		}
	}
	{
		sattrs := s.Attribute()
		for k, v := range sattrs {
			if _, ok := attrs[k]; !ok {
				attrs[k] = v
			} else {
				pub = append(pub, k)
			}
		}
	}
	return &Join{
		R:     r,
		S:     s,
		Pub:   pub,
		Attrs: attrs,
	}
}

func (n *Join) String() string {
	return fmt.Sprintf("(%s) ⨝ (%s)", n.R, n.S)
}

func (n *Join) Name() string {
	return n.ID
}

func (n *Join) Rename(name string) {
	n.ID = name
}

func (n *Join) Attribute() map[string]types.Type {
	return n.Attrs
}
