package sum

import (
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/sql/colexec/aggregation"
	"matrixbase/pkg/vectorize/sum"
	"matrixbase/pkg/vm/process"
)

func NewIntSumCount(typ types.Type) *intSumCount {
	return &intSumCount{typ: typ}
}

func (a *intSumCount) Reset() {
	a.cnt = 0
	a.sum = 0
}

func (a *intSumCount) Type() types.Type {
	return a.typ
}

func (a *intSumCount) Dup() aggregation.Aggregation {
	return &intSumCount{typ: a.typ}
}

func (a *intSumCount) Fill(sels []int64, vec *vector.Vector) error {
	if n := len(sels); n > 0 {
		switch vec.Typ.Oid {
		case types.T_int8:
			a.sum += sum.Int8SumSels(vec.Col.([]int8), sels)
		case types.T_int16:
			a.sum += sum.Int16SumSels(vec.Col.([]int16), sels)
		case types.T_int32:
			a.sum += sum.Int32SumSels(vec.Col.([]int32), sels)
		case types.T_int64:
			a.sum += sum.Int64SumSels(vec.Col.([]int64), sels)
		}
		a.cnt += int64(n - vec.Nsp.FilterCount(sels))
	} else {
		switch vec.Typ.Oid {
		case types.T_int8:
			a.sum += sum.Int8Sum(vec.Col.([]int8))
		case types.T_int16:
			a.sum += sum.Int16Sum(vec.Col.([]int16))
		case types.T_int32:
			a.sum += sum.Int32Sum(vec.Col.([]int32))
		case types.T_int64:
			a.sum += sum.Int64Sum(vec.Col.([]int64))
		}
		a.cnt += int64(vec.Length() - vec.Nsp.Length())
	}
	return nil
}

func (a *intSumCount) Eval() interface{} {
	return []interface{}{a.cnt, float64(a.sum)}
}

func (a *intSumCount) EvalCopy(proc *process.Process) (*vector.Vector, error) {
	vec := vector.New(a.typ)
	vec.SetCol([][]interface{}{[]interface{}{a.cnt, float64(a.sum)}})
	return vec, nil
}
