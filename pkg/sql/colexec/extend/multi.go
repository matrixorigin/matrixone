package extend

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/sql/colexec/extend/overload"
	"matrixone/pkg/vm/process"
)

func (e *MultiExtend) IsLogical() bool {
	return overload.IsLogical(e.Op)
}

func (_ *MultiExtend) IsConstant() bool {
	return false
}

func (_ *MultiExtend) Attributes() []string {
	return nil
}

func (_ *MultiExtend) ReturnType() types.T {
	return 0
}

func (_ *MultiExtend) Eval(_ *batch.Batch, _ *process.Process) (*vector.Vector, types.T, error) {
	return nil, 0, nil
}

func (_ *MultiExtend) Eq(_ Extend) bool {
	return false
}

func (_ *MultiExtend) String() string {
	return ""
}
