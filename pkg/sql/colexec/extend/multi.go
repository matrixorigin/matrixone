package extend

import (
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/sql/colexec/extend/overload"
	"matrixbase/pkg/vm/process"
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

func (_ *MultiExtend) String() string {
	return ""
}
