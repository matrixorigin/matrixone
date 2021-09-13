package build

import (
	"fmt"
	"matrixone/pkg/errno"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/projection"
	"matrixone/pkg/sql/tree"
	"matrixone/pkg/sqlerror"
	"strings"
)

func (b *build) checkProjection(ns tree.SelectExprs) error {
	for _, n := range ns {
		if attrs := b.checkProjectionExpr(n.Expr, []string{}); len(attrs) == 0 {
			return sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("invalid projection '%v'", n))
		}
	}
	return nil
}

func (b *build) checkProjectionExpr(n tree.Expr, attrs []string) []string {
	switch e := n.(type) {
	case *tree.NumVal:
		return attrs
	case tree.UnqualifiedStar:
		return append(attrs, "*")
	case *tree.ParenExpr:
		return b.checkProjectionExpr(e.Expr, attrs)
	case *tree.OrExpr:
		attrs = b.checkProjectionExpr(e.Left, attrs)
		return b.checkProjectionExpr(e.Right, attrs)
	case *tree.NotExpr:
		return b.checkProjectionExpr(e.Expr, attrs)
	case *tree.AndExpr:
		attrs = b.checkProjectionExpr(e.Left, attrs)
		return b.checkProjectionExpr(e.Right, attrs)
	case *tree.UnaryExpr:
		return b.checkProjectionExpr(e.Expr, attrs)
	case *tree.BinaryExpr:
		attrs = b.checkProjectionExpr(e.Left, attrs)
		return b.checkProjectionExpr(e.Right, attrs)
	case *tree.ComparisonExpr:
		attrs = b.checkProjectionExpr(e.Left, attrs)
		return b.checkProjectionExpr(e.Right, attrs)
	case *tree.Tuple:
		return attrs
	case *tree.FuncExpr:
		if name, ok := e.Func.FunctionReference.(*tree.UnresolvedName); ok {
			name.Parts[0] = strings.ToLower(name.Parts[0])
			if op, ok := AggFuncs[name.Parts[0]]; ok {
				if op == aggregation.StarCount {
					attrs = append(attrs, "*")
				} else {
					attrs = b.checkProjectionExpr(e.Exprs[0], attrs)
				}
			}
		}
		return attrs
	case *tree.CastExpr:
		return b.checkProjectionExpr(e.Expr, attrs)
	case *tree.UnresolvedName:
		if e.NumParts == 1 {
			attrs = append(attrs, e.Parts[0])
		} else {
			attrs = append(attrs, e.Parts[1]+"."+e.Parts[0])
		}
		return attrs
	}
	return attrs
}

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

func (b *build) buildProjectionWithOrder(o op.OP, ns tree.SelectExprs, es []*projection.Extend, mp map[string]uint8) (op.OP, []*projection.Extend, error) {
	var pes []*projection.Extend

	for _, n := range ns {
		if _, ok := n.Expr.(tree.UnqualifiedStar); ok {
			attrs := o.Attribute()
			for name, typ := range attrs {
				if _, ok := mp[name]; !ok {
					mp[name] = 0
					es = append(es, &projection.Extend{
						E: &extend.Attribute{
							Name: name,
							Type: typ.Oid,
						},
					})
				}
				pes = append(pes, &projection.Extend{
					E: &extend.Attribute{
						Name: name,
						Type: typ.Oid,
					},
				})
			}
		} else {
			e, err := b.buildExtend(o, n.Expr)
			if err != nil {
				return nil, nil, err
			}
			alias := string(n.As)
			if len(alias) == 0 {
				alias = e.String()
			}
			if _, ok := mp[e.String()]; !ok {
				mp[e.String()] = 0
				es = append(es, &projection.Extend{
					E:     e,
					Alias: alias,
				})
			}
			pes = append(pes, &projection.Extend{
				E: &extend.Attribute{
					Name: alias,
					Type: e.ReturnType(),
				},
			})
		}
	}
	o, err := projection.New(o, es)
	if err != nil {
		return nil, nil, err
	}
	return o, pes, nil
}

func (b *build) extractExtend(o op.OP, n tree.Expr, es *[]*projection.Extend, mp map[string]uint8) error {
	switch e := n.(type) {
	case *tree.ParenExpr:
		return b.extractExtend(o, e.Expr, es, mp)
	case *tree.OrExpr:
		if err := b.extractExtend(o, e.Left, es, mp); err != nil {
			return err
		}
		return b.extractExtend(o, e.Right, es, mp)
	case *tree.NotExpr:
		return b.extractExtend(o, e.Expr, es, mp)
	case *tree.AndExpr:
		if err := b.extractExtend(o, e.Left, es, mp); err != nil {
			return err
		}
		return b.extractExtend(o, e.Right, es, mp)
	case *tree.UnaryExpr:
		return b.extractExtend(o, e.Expr, es, mp)
	case *tree.BinaryExpr:
		if err := b.extractExtend(o, e.Left, es, mp); err != nil {
			return err
		}
		return b.extractExtend(o, e.Right, es, mp)
	case *tree.ComparisonExpr:
		if err := b.extractExtend(o, e.Left, es, mp); err != nil {
			return err
		}
		return b.extractExtend(o, e.Right, es, mp)
	case *tree.RangeCond:
		if err := b.extractExtend(o, e.To, es, mp); err != nil {
			return err
		}
		if err := b.extractExtend(o, e.From, es, mp); err != nil {
			return err
		}
		return b.extractExtend(o, e.Left, es, mp)
	case *tree.UnresolvedName:
		ext, err := b.buildAttribute(o, e)
		if err != nil {
			return err
		}
		if _, ok := mp[ext.String()]; !ok {
			mp[ext.String()] = 0
			(*es) = append((*es), &projection.Extend{E: ext})
		}
		return nil
	}
	return nil
}
