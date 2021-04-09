package avg

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/vectorize/sum"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
)

func NewFloat(typ types.Type) *floatAvg {
	return &floatAvg{typ: typ}
}

func (a *floatAvg) Reset() {
	a.cnt = 0
	a.sum = 0
}

func (a *floatAvg) Type() types.Type {
	return a.typ
}

func (a *floatAvg) Dup() aggregation.Aggregation {
	return &floatAvg{typ: a.typ}
}

func (a *floatAvg) Fill(sels []int64, vec *vector.Vector) error {
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

func (a *floatAvg) Eval() interface{} {
	if a.cnt == 0 {
		return nil
	}
	return a.sum / float64(a.cnt)
}

func (a *floatAvg) EvalCopy(proc *process.Process) (*vector.Vector, error) {
	data, err := proc.Alloc(8)
	if err != nil {
		return nil, err
	}
	vec := vector.New(a.typ)
	if a.cnt == 0 {
		vec.Nsp.Add(0)
		copy(data[mempool.CountSize:], encoding.EncodeFloat64(0))
	} else {
		copy(data[mempool.CountSize:], encoding.EncodeFloat64(a.sum/float64(a.cnt)))
	}
	vec.Data = data
	vec.Col = encoding.DecodeFloat64Slice(data[mempool.CountSize : mempool.CountSize+8])
	return vec, nil
}
