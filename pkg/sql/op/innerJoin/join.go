package innerJoin

import (
	"bytes"
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/op"
)

func New(r, s op.OP, rattrs, sattrs []string) *Join {
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
	return &Join{
		R:      r,
		S:      s,
		Attrs:  attrs,
		Rattrs: rattrs,
		Sattrs: sattrs,
	}
}

func (n *Join) String() string {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("(%s) ⨝ (%s) on(", n.R, n.S))
	for i, rattr := range n.Rattrs {
		if i > 0 {
			buf.WriteString(" AND ")
		}
		buf.WriteString(fmt.Sprintf("%s = %s", rattr, n.Sattrs[i]))
	}
	buf.WriteString(")")
	return buf.String()
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
