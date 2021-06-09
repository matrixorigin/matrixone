package build

import (
	"fmt"
	"go/constant"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/server"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/colexec/extend/overload"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/tree"
)

func (b *build) buildExtend(o op.OP, n tree.Expr) (extend.Extend, error) {
	e, err := b.buildExpr(o, n)
	if err != nil {
		return nil, err
	}
	e = RewriteExtend(e)
	return b.pruneExtend(e)
}

func (b *build) buildExpr(o op.OP, n tree.Expr) (extend.Extend, error) {
	switch e := n.(type) {
	case *tree.NumVal:
		return buildValue(e.Value)
	case *tree.ParenExpr:
		ext, err := b.buildExpr(o, e.Expr)
		if err != nil {
			return nil, err
		}
		return &extend.ParenExtend{ext}, nil
	case *tree.OrExpr:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{overload.Or, left, right}, nil
	case *tree.NotExpr:
		ext, err := b.buildExpr(o, e.Expr)
		if err != nil {
			return nil, err
		}
		return &extend.UnaryExtend{overload.Not, ext}, nil
	case *tree.AndExpr:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{overload.And, left, right}, nil
	case *tree.XorExpr:
		return nil, fmt.Errorf("Xor is not support now")
	case *tree.UnaryExpr:
		return b.buildUnary(o, e)
	case *tree.BinaryExpr:
		return b.buildBinary(o, e)
	case *tree.ComparisonExpr:
		return b.buildComparison(o, e)
	case *tree.Tuple:
		return nil, fmt.Errorf("'%v' is not support now", n)
	case *tree.FuncExpr:
		return nil, fmt.Errorf("'%v' is not support now", n)
	case *tree.IsNullExpr:
		return nil, fmt.Errorf("'%v' is not support now", n)
	case *tree.IsNotNullExpr:
		return nil, fmt.Errorf("'%v' is not support now", n)
	case *tree.Subquery:
		return nil, fmt.Errorf("'%v' is not support now", n)
	case *tree.CastExpr:
		left, err := b.buildExpr(o, e.Expr)
		if err != nil {
			return nil, err
		}
		typ := types.Type{}
		switch uint8(e.Type.(*tree.T).InternalType) {
		case server.MYSQL_TYPE_TINY:
			typ.Size = 1
			typ.Oid = types.T_int8
		case server.MYSQL_TYPE_SHORT:
			typ.Size = 2
			typ.Oid = types.T_int16
		case server.MYSQL_TYPE_LONG:
			typ.Size = 4
			typ.Oid = types.T_int32
		case server.MYSQL_TYPE_LONGLONG:
			typ.Size = 8
			typ.Oid = types.T_int64
		case server.MYSQL_TYPE_FLOAT:
			typ.Size = 4
			typ.Oid = types.T_float32
		case server.MYSQL_TYPE_DOUBLE:
			typ.Size = 8
			typ.Oid = types.T_float64
		case server.MYSQL_TYPE_VARCHAR:
			typ.Size = 24
			typ.Oid = types.T_varchar
		default:
			return nil, fmt.Errorf("'%v' is not support now", n)
		}
		return &extend.BinaryExtend{
			Op:    overload.Typecast,
			Left:  left,
			Right: &extend.ValueExtend{vector.New(typ)},
		}, nil
	case *tree.RangeCond:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		from, err := b.buildExpr(o, e.From)
		if err != nil {
			return nil, err
		}
		to, err := b.buildExpr(o, e.To)
		if err != nil {
			return nil, err
		}
		if e.Not {
			return &extend.BinaryExtend{
				Op: overload.And,
				Left: &extend.BinaryExtend{
					Op:    overload.LT,
					Left:  left,
					Right: from,
				},
				Right: &extend.BinaryExtend{
					Op:    overload.GT,
					Left:  left,
					Right: to,
				},
			}, nil
		}
		return &extend.BinaryExtend{
			Op: overload.And,
			Left: &extend.BinaryExtend{
				Op:    overload.GE,
				Left:  left,
				Right: from,
			},
			Right: &extend.BinaryExtend{
				Op:    overload.LE,
				Left:  left,
				Right: to,
			},
		}, nil
	case *tree.UnresolvedName:
		return b.buildAttribute(o, e)
	}
	return nil, fmt.Errorf("'%v' is not support now", n)
}

func (b *build) buildUnary(o op.OP, e *tree.UnaryExpr) (extend.Extend, error) {
	switch e.Op {
	case tree.UNARY_MINUS:
		ext, err := b.buildExpr(o, e.Expr)
		if err != nil {
			return nil, err
		}
		return &extend.UnaryExtend{overload.UnaryMinus, ext}, nil
	case tree.UNARY_PLUS:
		return b.buildExpr(o, e.Expr)
	}
	return nil, fmt.Errorf("'%v' is not support now", e)
}

func (b *build) buildBinary(o op.OP, e *tree.BinaryExpr) (extend.Extend, error) {
	switch e.Op {
	case tree.PLUS:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{overload.Plus, left, right}, nil
	case tree.MINUS:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{overload.Minus, left, right}, nil
	case tree.MULTI:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{overload.Mult, left, right}, nil
	case tree.MOD:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{overload.Mod, left, right}, nil
	case tree.DIV, tree.INTEGER_DIV:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{overload.Div, left, right}, nil
	}
	return nil, fmt.Errorf("'%v' is not support now", e)
}

func (b *build) buildComparison(o op.OP, e *tree.ComparisonExpr) (extend.Extend, error) {
	switch e.Op {
	case tree.EQUAL:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{overload.EQ, left, right}, nil
	case tree.LESS_THAN:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{overload.LT, left, right}, nil
	case tree.LESS_THAN_EQUAL:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{overload.LE, left, right}, nil
	case tree.GREAT_THAN:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{overload.GT, left, right}, nil
	case tree.GREAT_THAN_EQUAL:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{overload.GE, left, right}, nil
	case tree.NOT_EQUAL:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{overload.NE, left, right}, nil
	}
	return nil, fmt.Errorf("'%v' is not support now", e)
}

func (b *build) buildAttribute(o op.OP, e *tree.UnresolvedName) (extend.Extend, error) {
	if e.Star {
		return nil, fmt.Errorf("'%v' is not support now", e)
	}
	attrs := o.Attribute()
	if e.NumParts == 1 {
		if typ, ok := attrs[e.Parts[0]]; ok {
			return &extend.Attribute{Name: e.Parts[0], Type: typ.Oid}, nil
		}
		return nil, fmt.Errorf("unknown column '%s' in expr", e.Parts[0])
	}
	if typ, ok := attrs[e.Parts[0]]; ok {
		return &extend.Attribute{Name: e.Parts[0], Type: typ.Oid}, nil
	}
	name := e.Parts[1] + "." + e.Parts[0]
	if typ, ok := attrs[name]; ok {
		return &extend.Attribute{Name: name, Type: typ.Oid}, nil
	}
	return nil, fmt.Errorf("unknown column '%s' in expr", e.Parts[0])
}

func stripParens(expr tree.Expr) tree.Expr {
	if p, ok := expr.(*tree.ParenExpr); ok {
		return stripParens(p.Expr)
	}
	return expr
}

func buildValue(val constant.Value) (extend.Extend, error) {
	switch val.Kind() {
	case constant.Int:
		vec := vector.New(types.Type{Oid: types.T_int64, Size: 8})
		v, _ := constant.Int64Val(val)
		vec.Col = []int64{v}
		return &extend.ValueExtend{vec}, nil
	case constant.Float:
		vec := vector.New(types.Type{Oid: types.T_float64, Size: 8})
		v, _ := constant.Float64Val(val)
		vec.Col = []float64{v}
		return &extend.ValueExtend{vec}, nil
	case constant.String:
		vec := vector.New(types.Type{Oid: types.T_varchar, Size: 24})
		v := constant.StringVal(val)
		vec.Col = &types.Bytes{
			Data:    []byte(v),
			Offsets: []uint32{0},
			Lengths: []uint32{uint32(len(v))},
		}
		return &extend.ValueExtend{vec}, nil
	default:
		return nil, fmt.Errorf("unsupport value: %v", val)
	}
}
