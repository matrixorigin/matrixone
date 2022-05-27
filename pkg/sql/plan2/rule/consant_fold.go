package rule

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	colexec "github.com/matrixorigin/matrixone/pkg/sql/colexec2"
)

type ConstantFold struct {
	bat *batch.Batch
}

func NewConstantFlod() *ConstantFold {
	bat := batch.NewWithSize(00000000)
	bat.Zs = []int64{0}
	return &ConstantFold{
		bat: bat,
	}
}

// always true
func (r *ConstantFold) Match(n *plan.Node) bool {
	return true
}

func (r *ConstantFold) Apply(n *plan.Node, _ *plan.Query) {
	if n.Limit != nil {
		n.Limit = r.constantFold(n.Limit)
	}
	if n.Offset != nil {
		n.Offset = r.constantFold(n.Offset)
	}
	if len(n.OnList) > 0 {
		for i := range n.OnList {
			n.OnList[i] = r.constantFold(n.OnList[i])
		}
	}
	if len(n.WhereList) > 0 {
		for i := range n.WhereList {
			n.WhereList[i] = r.constantFold(n.WhereList[i])
		}
	}
	if len(n.ProjectList) > 0 {
		for i := range n.ProjectList {
			n.ProjectList[i] = r.constantFold(n.ProjectList[i])
		}
	}
}

func (r *ConstantFold) constantFold(e *plan.Expr) *plan.Expr {
	ef, ok := e.Expr.(*plan.Expr_F)
	if !ok {
		return e
	}
	for i := range ef.F.Args {
		ef.F.Args[i] = r.constantFold(ef.F.Args[i])
	}
	vec, err := colexec.EvalExpr(r.bat, nil, e)
	if err != nil {
		return e
	}
	c := getConstantValue(vec)
	if c == nil {
		return e
	}
	ec := &plan.Expr_C{
		C: c,
	}
	e.Expr = ec
	return e
}

func getConstantValue(vec *vector.Vector) *plan.Const {
	switch vec.Typ.Oid {
	case types.T_bool:
		return &plan.Const{
			Isnull: nulls.Any(vec.Nsp),
			Value: &plan.Const_Bval{
				Bval: vec.Col.([]bool)[0],
			},
		}
	case types.T_int64:
		return &plan.Const{
			Isnull: nulls.Any(vec.Nsp),
			Value: &plan.Const_Ival{
				Ival: vec.Col.([]int64)[0],
			},
		}
	case types.T_float64:
		return &plan.Const{
			Isnull: nulls.Any(vec.Nsp),
			Value: &plan.Const_Dval{
				Dval: vec.Col.([]float64)[0],
			},
		}
	case types.T_varchar:
		return &plan.Const{
			Isnull: nulls.Any(vec.Nsp),
			Value: &plan.Const_Sval{
				Sval: string(vec.Col.(*types.Bytes).Data),
			},
		}
	default:
		return nil
	}
}
