package sum

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/vectorize/sum"
	"matrixone/pkg/vm/process"
)

func NewFloatSumCount(typ types.Type) *floatSumCount {
	return &floatSumCount{typ: typ}
}

func (a *floatSumCount) Reset() {
	a.cnt = 0
	a.sum = 0
}

func (a *floatSumCount) Type() types.Type {
	return a.typ
}

func (a *floatSumCount) Dup() aggregation.Aggregation {
	return &floatSumCount{typ: a.typ}
}

func (a *floatSumCount) Fill(sels []int64, vec *vector.Vector) error {
	if n := len(sels); n > 0 {
		switch vec.Typ.Oid {
		case types.T_float32:
			a.sum += float64(sum.Float32SumSels(vec.Col.([]float32), sels))
		case types.T_float64:
			a.sum += sum.Float64SumSels(vec.Col.([]float64), sels)
		}
		a.cnt += int64(n - vec.Nsp.FilterCount(sels))
	} else {
		switch vec.Typ.Oid {
		case types.T_float32:
			a.sum += float64(sum.Float32Sum(vec.Col.([]float32)))
		case types.T_float64:
			a.sum += sum.Float64Sum(vec.Col.([]float64))
		}
		a.cnt += int64(vec.Length() - vec.Nsp.Length())
	}
	return nil
}

func (a *floatSumCount) Eval() interface{} {
	return []interface{}{a.cnt, a.sum}
}

func (a *floatSumCount) EvalCopy(proc *process.Process) (*vector.Vector, error) {
	vec := vector.New(a.typ)
	vec.SetCol([][]interface{}{[]interface{}{a.cnt, a.sum}})
	return vec, nil
}
