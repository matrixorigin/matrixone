package build

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/group"
	"matrixone/pkg/sql/op/projection"
	"matrixone/pkg/sql/tree"
)

func (b *build) buildProjection(o op.OP, ns tree.SelectExprs) (op.OP, error) {
	var es []*projection.Extend

	for _, n := range ns {
		if _, ok := n.Expr.(tree.UnqualifiedStar); ok {
			attrs := o.Attribute()
			for name, typ := range attrs {
				es = append(es, &projection.Extend{
					E: &extend.Attribute{
						Name: name,
						Type: typ.Oid,
					},
				})
			}
		} else {
			e, err := b.buildExtend(o, n.Expr)
			if err != nil {
				return nil, err
			}
			es = append(es, &projection.Extend{
				E:     e,
				Alias: string(n.As),
			})
		}
	}
	return projection.New(o, es)
}

func (b *build) buildProjectionWithGroup(o op.OP, ns tree.SelectExprs, gs []*extend.Attribute) (op.OP, error) {
	var es []aggregation.Extend

	xs, ys, err := classify(ns)
	if err != nil {
		return nil, err
	}
	if o, err = b.stripGroup(o, xs, gs); err != nil {
		return nil, err
	}
	for _, n := range xs {
		expr := n.Expr.(*tree.FuncExpr)
		name, ok := expr.Func.FunctionReference.(*tree.UnresolvedName)
		if !ok {
			return nil, fmt.Errorf("illegal expression '%s'", n)
		}
		op, ok := AggFuncs[name.Parts[0]]
		if !ok {
			return nil, fmt.Errorf("unimplemented aggregated functions '%s'", name.Parts[0])
		}
		alias := string(n.As)
		switch e := expr.Exprs[0].(type) {
		case *tree.NumVal:
			if len(alias) == 0 {
				alias = "count(*)"
			}
			agg, err := newAggregate(op, types.Type{Oid: types.T_int64, Size: 8})
			if err != nil {
				return nil, err
			}
			es = append(es, aggregation.Extend{
				Agg:   agg,
				Alias: alias,
				Op:    aggregation.StarCount,
			})
			ys = append(ys, tree.SelectExpr{
				As:   tree.UnrestrictedIdentifier(alias),
				Expr: &tree.UnresolvedName{Parts: [4]string{alias}},
			})
		case *tree.UnresolvedName:
			if len(alias) == 0 {
				alias = fmt.Sprintf("%s(%s)", name.Parts[0], e.Parts[0])
			}
			typ, ok := o.Attribute()[e.Parts[0]]
			if !ok {
				return nil, fmt.Errorf("unknown column '%s' in aggregation", e.Parts[0])
			}
			agg, err := newAggregate(op, typ)
			if err != nil {
				return nil, err
			}
			es = append(es, aggregation.Extend{
				Op:    op,
				Agg:   agg,
				Alias: alias,
				Name:  e.Parts[0],
			})
			ys = append(ys, tree.SelectExpr{
				As:   tree.UnrestrictedIdentifier(alias),
				Expr: &tree.UnresolvedName{Parts: [4]string{alias}},
			})
		}
	}
	if o, err = group.New(o, gs, es); err != nil {
		return nil, err
	}
	return b.buildProjection(o, ys)
}

func classify(ns tree.SelectExprs) (tree.SelectExprs, tree.SelectExprs, error) {
	var xs, ys tree.SelectExprs

	for _, n := range ns {
		if _, ok := n.Expr.(tree.UnqualifiedStar); ok {
			return nil, nil, fmt.Errorf("noaggregated column '*'")
		}
		if _, ok := n.Expr.(*tree.FuncExpr); ok {
			xs = append(xs, n)
		} else {
			ys = append(ys, n)
		}

	}
	return xs, ys, nil
}
