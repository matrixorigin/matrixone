package binaryfunc

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vectorize/power"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Power(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	left, right := vectors[0], vectors[1]
	leftValues, rightValues := left.Col.([]float64), right.Col.([]float64)
	resultType := types.Type{Oid: types.T_float64, Size: 8}
	resultElementSize := int(resultType.Size)
	switch {
	case left.IsConst && right.IsConst:
		if left.ConstVectorIsNull() || right.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(left.Typ)
		resultValues := make([]float64, 1)
		vector.SetCol(resultVector, power.Power(leftValues, rightValues, resultValues)) // if our input contains null, this step may be redundant,
		return resultVector, nil
	case left.IsConst && !right.IsConst:
		if left.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(left.Typ), nil
		}
		resultVector, err := proc.AllocVector(left.Typ, int64(resultElementSize*len(rightValues)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeFloat64Slice(resultVector.Data)
		resultValues = resultValues[:len(rightValues)]
		nulls.Set(resultVector.Nsp, right.Nsp)
		vector.SetCol(resultVector, power.PowerScalarLeftConst(leftValues[0], rightValues, resultValues))
		return resultVector, nil
	case !left.IsConst && right.IsConst:
		if right.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(left.Typ), nil
		}
		resultVector, err := proc.AllocVector(left.Typ, int64(resultElementSize*len(rightValues)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeFloat64Slice(resultVector.Data)
		resultValues = resultValues[:len(leftValues)]
		nulls.Set(resultVector.Nsp, right.Nsp)
		vector.SetCol(resultVector, power.PowerScalarRightConst(rightValues[0], leftValues, resultValues))
		return resultVector, nil
	}
	resultVector, err := proc.AllocVector(left.Typ, int64(resultElementSize*len(rightValues)))
	if err != nil {
		return nil, err
	}
	resultValues := encoding.DecodeFloat64Slice(resultVector.Data)
	resultValues = resultValues[:len(leftValues)]
	nulls.Or(left.Nsp, right.Nsp, resultVector.Nsp)
	vector.SetCol(resultVector, power.Power(leftValues, rightValues, resultValues))
	return resultVector, nil
}
