package projection

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/op"
)

func New(prev op.OP, es []*Extend) *Projection {
	attrs := make(map[string]types.Type)
	for _, e := range es {
		if len(e.Alias) == 0 {
			e.Alias = e.E.String()
		}
		switch typ := e.E.ReturnType(); typ {
		case types.T_int8:
			attrs[e.Alias] = types.Type{Oid: typ, Size: 1}
		case types.T_int16:
			attrs[e.Alias] = types.Type{Oid: typ, Size: 2}
		case types.T_int32:
			attrs[e.Alias] = types.Type{Oid: typ, Size: 4}
		case types.T_int64:
			attrs[e.Alias] = types.Type{Oid: typ, Size: 8}
		case types.T_uint8:
			attrs[e.Alias] = types.Type{Oid: typ, Size: 1}
		case types.T_uint16:
			attrs[e.Alias] = types.Type{Oid: typ, Size: 2}
		case types.T_uint32:
			attrs[e.Alias] = types.Type{Oid: typ, Size: 4}
		case types.T_uint64:
			attrs[e.Alias] = types.Type{Oid: typ, Size: 8}
		case types.T_float32:
			attrs[e.Alias] = types.Type{Oid: typ, Size: 4}
		case types.T_float64:
			attrs[e.Alias] = types.Type{Oid: typ, Size: 8}
		case types.T_char:
			attrs[e.Alias] = types.Type{Oid: typ, Size: 24}
		case types.T_varchar:
			attrs[e.Alias] = types.Type{Oid: typ, Size: 24}
		case types.T_sel:
			attrs[e.Alias] = types.Type{Oid: typ, Size: 8}
		}
	}
	return &Projection{
		Es:    es,
		Prev:  prev,
		Attrs: attrs,
	}
}

func (n *Projection) String() string {
	r := fmt.Sprintf("%s -> π([", n.Prev)
	for i, e := range n.Es {
		switch i {
		case 0:
			if len(e.Alias) == 0 {
				r += fmt.Sprintf("%s", e.E)
			} else {
				r += fmt.Sprintf("%s -> %s", e.E, e.Alias)
			}
		default:
			if len(e.Alias) == 0 {
				r += fmt.Sprintf(", %s", e.E)
			} else {
				r += fmt.Sprintf(", %s -> %s", e.E, e.Alias)
			}
		}
	}
	r += fmt.Sprintf("]")
	return r
}

func (n *Projection) Name() string {
	return n.ID
}

func (n *Projection) Rename(name string) {
	n.ID = name
}

func (n *Projection) Attribute() map[string]types.Type {
	return n.Attrs
}
