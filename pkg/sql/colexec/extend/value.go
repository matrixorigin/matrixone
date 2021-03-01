package extend

import (
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/vm/process"
)

func (a *ValueExtend) IsLogical() bool {
	return false
}

func (_ *ValueExtend) IsConstant() bool {
	return true
}

func (a *ValueExtend) ReturnType() types.T {
	return a.V.Typ.Oid
}

func (_ *ValueExtend) Attributes() []string {
	return nil
}

func (a *ValueExtend) Eval(_ *batch.Batch, _ *process.Process) (*vector.Vector, types.T, error) {
	return a.V, a.V.Typ.Oid, nil
}

func (a *ValueExtend) String() string {
	return a.V.String()
}
