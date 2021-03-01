package extend

import (
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/vm/process"
)

func (a *Attribute) IsLogical() bool {
	return false
}

func (_ *Attribute) IsConstant() bool {
	return false
}

func (a *Attribute) Attributes() []string {
	return []string{a.Name}
}

func (a *Attribute) ReturnType() types.T {
	return a.Type
}

func (a *Attribute) Eval(bat *batch.Batch, proc *process.Process) (*vector.Vector, types.T, error) {
	vec, err := bat.GetVector(a.Name, proc)
	if err != nil {
		return nil, 0, err
	}
	proc.Mp.Inc(vec.Data)
	return vec, a.Type, nil
}

func (a *Attribute) String() string {
	return a.Name
}
