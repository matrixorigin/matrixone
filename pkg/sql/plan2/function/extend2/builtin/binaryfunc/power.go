package binaryfunc

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vectorize/power"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Power(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	// should we differentiate the function signature for binary/unaryfunc/variadic operators?
	left, right := vectors[0], vectors[1]
	leftValues, rightValues := left.Col.([]float64), right.Col.([]float64)
	resultType := types.Type{Oid: types.T_float64, Size: 8}
	resultElementSize := int(resultType.Size)
	switch {
	case left.IsConst && right.IsConst:
		// in the case where the result is a const, I chose to return only one row containing the const,
		// this may require corresponding changes in the frontend, is this okay?
		resultVector := vector.New(left.Typ)
		resultValues := make([]float64, 1)
		nulls.Or(left.Nsp, right.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, power.Power(leftValues, rightValues, resultValues)) // if our input contains null, this step may be redundant,
		resultVector.IsConst = true
		resultVector.Length = left.Length
		return resultVector, nil
	case left.IsConst && !right.IsConst:
		data, err := mheap.Alloc(proc.Mp, int64(resultElementSize*len(rightValues)))
		if err != nil {
			return nil, err
		}
		resultVector := vector.New(left.Typ)
		resultVector.Data = data
		resultValues := encoding.DecodeFloat64Slice(resultVector.Data)
		resultValues = resultValues[:len(rightValues)]
		nulls.Or(left.Nsp, right.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, power.PowerScalarLeftConst(leftValues[0], rightValues, resultValues))
		return resultVector, nil
	case !left.IsConst && right.IsConst:
		data, err := mheap.Alloc(proc.Mp, int64(resultElementSize*len(leftValues)))
		if err != nil {
			return nil, err
		}
		resultVector := vector.New(left.Typ)
		resultVector.Data = data
		resultValues := encoding.DecodeFloat64Slice(resultVector.Data)
		resultValues = resultValues[:len(leftValues)]
		nulls.Or(left.Nsp, right.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, power.PowerScalarRightConst(rightValues[0], leftValues, resultValues))
		return resultVector, nil
	}
	data, err := mheap.Alloc(proc.Mp, int64(resultElementSize*len(rightValues)))
	if err != nil {
		return nil, err
	}
	resultVector := vector.New(left.Typ)
	resultVector.Data = data
	resultValues := encoding.DecodeFloat64Slice(resultVector.Data)
	resultValues = resultValues[:len(leftValues)]
	nulls.Or(left.Nsp, right.Nsp, resultVector.Nsp)
	vector.SetCol(resultVector, power.Power(leftValues, rightValues, resultValues))
	return resultVector, nil
}
