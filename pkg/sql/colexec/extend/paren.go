package extend

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/process"
)

func (e *ParenExtend) IsLogical() bool {
	return e.E.IsLogical()
}

func (_ *ParenExtend) IsConstant() bool {
	return false
}

func (e *ParenExtend) ReturnType() types.T {
	return e.E.ReturnType()
}

func (e *ParenExtend) Attributes() []string {
	return e.E.Attributes()
}

func (e *ParenExtend) Eval(bat *batch.Batch, proc *process.Process) (*vector.Vector, types.T, error) {
	return e.E.Eval(bat, proc)
}

func (e *ParenExtend) String() string {
	return "(" + e.E.String() + ")"
}
