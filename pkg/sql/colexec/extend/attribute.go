package extend

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/process"
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
	if len(bat.Sels) > 0 {
		return vec.Shuffle(bat.Sels), a.Type, nil
	}
	return vec, a.Type, nil
}

func (a *Attribute) String() string {
	return a.Name
}
