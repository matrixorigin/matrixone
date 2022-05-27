package multifunc

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/pi"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Pi(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	resultType := types.Type{Oid: types.T_float64, Size: 8}
	resultVector := vector.New(resultType)
	result := make([]float64, 1)
	result[0] = pi.GetPi()
	vector.SetCol(resultVector, result)
	resultVector.IsConst = true
	return resultVector, nil
}
