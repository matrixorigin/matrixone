package extend2

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vectorize/add"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

func Plus[T constraints.Integer | constraints.Float](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	left, right := vectors[0], vectors[1]
	leftValues, rightValues := left.Col.([]T), right.Col.([]T)
	resultElementSize := left.Typ.Oid.FixedLength()
	switch {
	case left.IsConst && right.IsConst:
		if left.ConstVectorIsNull() || right.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(left.Typ), nil
		}
		resultVector := vector.NewConst(left.Typ)
		resultValues := make([]T, 1)
		vector.SetCol(resultVector, add.NumericAdd[T](leftValues, rightValues, resultValues))
		return resultVector, nil
	case left.IsConst && !right.IsConst:
		if left.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(left.Typ), nil
		}
		resultVector, err := proc.AllocVector(left.Typ, int64(resultElementSize*len(rightValues)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeFixedSlice[T](resultVector.Data, resultElementSize)
		resultValues = resultValues[:len(rightValues)]
		nulls.Set(resultVector.Nsp, right.Nsp)
		vector.SetCol(resultVector, add.NumericAddScalar[T](leftValues[0], rightValues, resultValues))
		return resultVector, nil
	case !left.IsConst && right.IsConst:
		if right.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(left.Typ), nil
		}
		resultVector, err := proc.AllocVector(left.Typ, int64(resultElementSize*len(leftValues)))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeFixedSlice[T](resultVector.Data, resultElementSize)
		resultValues = resultValues[:len(leftValues)]
		nulls.Set(resultVector.Nsp, right.Nsp)
		vector.SetCol(resultVector, add.NumericAddScalar[T](rightValues[0], leftValues, resultValues))
		return resultVector, nil
	}
	resultVector, err := proc.AllocVector(left.Typ, int64(resultElementSize*len(leftValues)))
	if err != nil {
		return nil, err
	}
	resultValues := encoding.DecodeFixedSlice[T](resultVector.Data, resultElementSize)
	nulls.Or(left.Nsp, right.Nsp, resultVector.Nsp)
	vector.SetCol(resultVector, add.NumericAdd[T](leftValues, rightValues, resultValues))
	return resultVector, nil
}
