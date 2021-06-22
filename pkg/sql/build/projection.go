package build

import (
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/op"
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
			if _, ok := mp[e.String()]; !ok {
				mp[e.String()] = 0
				es = append(es, &projection.Extend{
					E:     e,
					Alias: string(n.As),
				})
			}
			pes = append(pes, &projection.Extend{
				E:     e,
				Alias: string(n.As),
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
