package extend

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/process"
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
