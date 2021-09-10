package compile

import (
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/dedup"
	"matrixone/pkg/sql/op/group"
	"matrixone/pkg/sql/op/innerJoin"
	"matrixone/pkg/sql/op/limit"
	"matrixone/pkg/sql/op/naturalJoin"
	"matrixone/pkg/sql/op/order"
	"matrixone/pkg/sql/op/product"
	"matrixone/pkg/sql/op/projection"
	"matrixone/pkg/sql/op/relation"
	"matrixone/pkg/sql/op/restrict"
	"matrixone/pkg/sql/op/summarize"
	"matrixone/pkg/sql/op/top"
)

func prune(o op.OP) op.OP {
	switch n := o.(type) {
	case *top.Top:
		n.Prev = prune(n.Prev)
		return n
	case *dedup.Dedup:
		n.Prev = prune(n.Prev)
		return n
	case *group.Group:
		n.Prev = prune(n.Prev)
		return n
	case *limit.Limit:
		n.Prev = prune(n.Prev)
		return n
	case *order.Order:
		n.Prev = prune(n.Prev)
		return n
	case *product.Product:
		n.R = prune(n.R)
		n.S = prune(n.S)
		return n
	case *innerJoin.Join:
		n.R = prune(n.R)
		n.S = prune(n.S)
		return n
	case *naturalJoin.Join:
		n.R = prune(n.R)
		n.S = prune(n.S)
		return n
	case *relation.Relation:
		return n
	case *restrict.Restrict:
		n.Prev = prune(n.Prev)
		return n
	case *summarize.Summarize:
		n.Prev = prune(n.Prev)
		return n
	case *projection.Projection:
		o = pruneProjection(n)
		n, _ = o.(*projection.Projection)
		n.Prev = prune(n.Prev)
		return n
	}
	return o
}

func pruneProjection(n *projection.Projection) op.OP {
	es := n.Es
	for prev, ok := n.Prev.(*projection.Projection); ok; prev, ok = n.Prev.(*projection.Projection) {
		if projectionExtendEq(es, prev.Es) {
			n.Prev = prev.Prev
			continue
		}
		break
	}
	return n
}

func projectionExtendEq(xs, ys []*projection.Extend) bool {
	if len(xs) != len(ys) {
		return false
	}
	for i, x := range xs {
		if x.Alias != ys[i].Alias || !x.E.Eq(ys[i].E) {
			return false
		}
	}
	return true
}
