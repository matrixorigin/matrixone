package build

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/restrict"
	"matrixone/pkg/sql/tree"
)

func (b *build) buildWhere(o op.OP, stmt *tree.Where) (op.OP, error) {
	e, err := b.buildExtend(o, stmt.Expr)
	if err != nil {
		return nil, err
	}
	if v, ok := e.(*extend.ValueExtend); ok {
		switch v.V.Typ.Oid {
		case types.T_int64:
			if v.V.Col.([]int64)[0] != 0 {
				return o, nil
			} else {
				return nil, nil
			}
		case types.T_float64:
			if v.V.Col.([]float64)[0] != 0 {
				return o, nil
			} else {
				return nil, nil
			}
		default:
			return nil, nil
		}
	}
	return restrict.New(o, e), nil
}
