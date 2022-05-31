package binary

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vectorize/findinset"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func FindInSet(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	left, right := vectors[0], vectors[1]
	leftValues, rightValues := left.Col.(*types.Bytes), right.Col.(*types.Bytes)
	resultType := types.Type{Oid: types.T_uint64, Size: 8}
	resultElementSize := int(resultType.Size)
	switch {
	case left.IsConst && right.IsConst:
		if left.ConstVectorIsNull() || right.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(left.Typ)
		resultValues := make([]uint64, 1)
		vector.SetCol(resultVector, findinset.FindInSetWithAllConst(leftValues, rightValues, resultValues))
		return resultVector, nil
	case left.IsConst && !right.IsConst:
		if left.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(left.Typ), nil
		}
		resultVector, err := proc.AllocVector(resultType, int64(resultElementSize*len(rightValues.Lengths)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeUint64Slice(resultVector.Data)
		resultValues = resultValues[:len(rightValues.Lengths)]
		nulls.Set(resultVector.Nsp, right.Nsp)
		vector.SetCol(resultVector, findinset.FindInSetWithLeftConst(leftValues, rightValues, resultValues))
		return resultVector, nil
	case !left.IsConst && right.IsConst:
		if right.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(left.Typ), nil
		}
		resultVector, err := proc.AllocVector(left.Typ, int64(resultElementSize*len(leftValues.Lengths)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeUint64Slice(resultVector.Data)
		resultValues = resultValues[:len(leftValues.Lengths)]
		nulls.Set(resultVector.Nsp, left.Nsp)
		vector.SetCol(resultVector, findinset.FindInSetWithRightConst(leftValues, rightValues, resultValues))
		return resultVector, nil
	}
	resultVector, err := proc.AllocVector(left.Typ, int64(resultElementSize*len(rightValues.Lengths)))
	if err != nil {
		return nil, err
	}
	resultValues := encoding.DecodeUint64Slice(resultVector.Data)
	resultValues = resultValues[:len(rightValues.Lengths)]
	nulls.Or(left.Nsp, right.Nsp, resultVector.Nsp)
	vector.SetCol(resultVector, findinset.FindInSet(leftValues, rightValues, resultValues))
	return resultVector, nil
}
