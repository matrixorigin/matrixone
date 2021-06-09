package build

import (
	"fmt"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/innerJoin"
	"matrixone/pkg/sql/op/naturalJoin"
	"matrixone/pkg/sql/op/product"
	"matrixone/pkg/sql/tree"
)

func (b *build) buildJoin(stmt *tree.JoinTableExpr) (op.OP, error) {
	r, err := b.buildFromTable(stmt.Left)
	if err != nil {
		return nil, err
	}
	s, err := b.buildFromTable(stmt.Right)
	if err != nil {
		return nil, err
	}
	{
		switch stmt.JoinType {
		case tree.JOIN_TYPE_FULL:
			return nil, fmt.Errorf("unsupport join type %v", stmt.JoinType)
		case tree.JOIN_TYPE_LEFT:
			return nil, fmt.Errorf("unsupport join type %v", stmt.JoinType)
		case tree.JOIN_TYPE_RIGHT:
			return nil, fmt.Errorf("unsupport join type %v", stmt.JoinType)
		}
	}
	if stmt.Cond == nil {
		if err := b.checkProduct(r, s); err != nil {
			return nil, err
		}
		return product.New(r, s), nil
	}
	switch cond := stmt.Cond.(type) {
	case *tree.NaturalJoinCond:
		if err := b.checkNaturalJoin(r, s); err != nil {
			return nil, err
		}
		return naturalJoin.New(r, s), nil
	case *tree.OnJoinCond:
		rattrs, sattrs, err := b.checkInnerJoin(r, s, nil, nil, cond.Expr)
		if err != nil {
			return nil, err
		}
		return innerJoin.New(r, s, rattrs, sattrs), nil
	default:
		return nil, fmt.Errorf("unsupport join condition %#v", cond)
	}
	return nil, nil
}
