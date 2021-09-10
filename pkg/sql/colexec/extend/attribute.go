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
	return bat.GetVector(a.Name), a.Type, nil
}

func (a *Attribute) Eq(e Extend) bool {
	if b, ok := e.(*Attribute); ok {
		return a.Name == b.Name && a.Type == b.Type
	}
	return false
}

func (a *Attribute) String() string {
	return a.Name
}
