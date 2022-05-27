package unaryfunc

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vectorize/sin"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

func Sin[T constraints.Integer | constraints.Float](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0] // so these kinds of indexing are always guaranteed success because all the checks are done in the plan?
	inputValues := inputVector.Col.([]T)
	resultType := types.Type{Oid: types.T_float64, Size: 8}
	resultElementSize := int(resultType.Size)
	if inputVector.IsConst {
		resultVector := vector.New(resultType)
		resultValues := make([]float64, 1)
		nulls.Set(resultVector.Nsp, inputVector.Nsp)
		vector.SetCol(resultVector, sin.Sin[T](inputValues, resultValues))
		resultVector.IsConst = true
		resultVector.Length = inputVector.Length
		return resultVector, nil
	} else {
		data, err := mheap.Alloc(proc.Mp, int64(resultElementSize*len(inputValues)))
		if err != nil {
			return nil, err
		}
		resultVector := vector.New(resultType)
		resultVector.Data = data
		resultValues := encoding.DecodeFloat64Slice(resultVector.Data)
		resultValues = resultValues[:len(inputValues)]
		nulls.Set(resultVector.Nsp, inputVector.Nsp)
		vector.SetCol(resultVector, sin.Sin[T](inputValues, resultValues))
		return resultVector, nil
	}
}

/*
Typ:        types.T_int64,
ReturnType: types.T_float64,
Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
	origVecCol := origVec.Col.([]int64)
	resultVector, err := process.Get(proc, 8*int64(len(origVecCol)), types.Type{Oid: types.T_float64, Size: 8})
	if err != nil {
		return nil, err
	}
	results := encoding.DecodeFloat64Slice(resultVector.Data)
	results = results[:len(origVecCol)]
	resultVector.Col = results
	nulls.Set(resultVector.Nsp, origVec.Nsp)
	vector.SetCol(resultVector, sin.SinInt64(origVecCol, results))
	return resultVector, nil
}

*/
