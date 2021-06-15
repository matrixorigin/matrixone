package build

import (
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/projection"
	"matrixone/pkg/sql/tree"
)

func (b *build) buildGroupBy(o op.OP, ns tree.GroupBy) (op.OP, []*extend.Attribute, error) {
	var es []extend.Extend
	var attrs []*extend.Attribute

	for _, n := range ns {
		e, err := b.buildExtend(o, n)
		if err != nil {
			return nil, nil, err
		}
		if attr, ok := e.(*extend.Attribute); !ok {
			es = append(es, e)
		} else {
			attrs = append(attrs, attr)
		}
	}
	if len(es) == 0 {
		return o, attrs, nil
	}
	rs := make([]*extend.Attribute, 0, len(es)+len(attrs))
	pes := make([]*projection.Extend, 0, len(es)+len(attrs))
	{
		for _, e := range es {
			pes = append(pes, &projection.Extend{E: e})
			rs = append(rs, &extend.Attribute{
				Name: e.String(),
				Type: e.ReturnType(),
			})
		}
		for _, attr := range attrs {
			pes = append(pes, &projection.Extend{E: attr})
			rs = append(rs, attr)
		}
	}
	o, err := projection.New(o, pes)
	if err != nil {
		return nil, nil, err
	}
	return o, rs, nil
}

func (b *build) stripGroup(o op.OP, ns tree.SelectExprs, gs []*extend.Attribute) (op.OP, error) {
	var es []*projection.Extend

	for _, n := range ns {
		if _, ok := n.Expr.(*tree.FuncExpr).Exprs[0].(*tree.NumVal); !ok {
			e, err := b.buildExtend(o, n.Expr.(*tree.FuncExpr).Exprs[0])
			if err != nil {
				return nil, err
			}
			es = append(es, &projection.Extend{E: e})
			n.Expr.(*tree.FuncExpr).Exprs[0] = &tree.UnresolvedName{
				Parts: [4]string{e.String()},
			}
		}
	}
	if len(es) == 0 {
		return o, nil
	}
	for _, g := range gs {
		es = append(es, &projection.Extend{E: g})
	}
	return projection.New(o, es)
}
