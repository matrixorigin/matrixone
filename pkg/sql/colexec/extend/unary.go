package extend

import (
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/sql/colexec/extend/overload"
	"matrixbase/pkg/vm/process"
)

func (e *UnaryExtend) IsLogical() bool {
	return overload.IsLogical(e.Op)
}

func (_ *UnaryExtend) IsConstant() bool {
	return false
}

func (e *UnaryExtend) Attributes() []string {
	return e.E.Attributes()
}

func (e *UnaryExtend) ReturnType() types.T {
	if fn, ok := UnaryReturnTypes[e.Op]; ok {
		return fn(e.E)
	}
	return types.T_any
}

func (e *UnaryExtend) Eval(bat *batch.Batch, proc *process.Process) (*vector.Vector, types.T, error) {
	vs, typ, err := e.E.Eval(bat, proc)
	if err != nil {
		return nil, 0, err
	}
	vec, err := overload.UnaryEval(e.Op, typ, e.E.IsConstant(), vs, proc)
	if err != nil {
		return nil, 0, err
	}
	return vec, e.ReturnType(), nil
}

func (e *UnaryExtend) String() string {
	if fn, ok := UnaryStrings[e.Op]; ok {
		return fn(e.E)
	}
	return ""
}
