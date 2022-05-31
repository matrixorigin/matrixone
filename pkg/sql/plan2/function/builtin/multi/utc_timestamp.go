package multi

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func UTCTimestamp(_ []*vector.Vector, _ *process.Process) (*vector.Vector, error) {
	resultType := types.Type{Oid: types.T_datetime, Size: 8}
	resultVector := vector.NewConst(resultType)
	result := make([]types.Datetime, 1)
	result[0] = timestamp.GetUTCTimestamp()
	vector.SetCol(resultVector, result)
	return resultVector, nil
}
