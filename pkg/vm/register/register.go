package register

import (
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/vm/process"
)

func Get(proc *process.Process, size int64, typ types.Type) (*vector.Vector, error) {
	for i, t := range proc.Reg.Ts {
		v := t.(*vector.Vector)
		if int64(cap(v.Data)) == size {
			vec := vector.New(typ)
			vec.Data = v.Data[:size]
			proc.Reg.Ts = append(proc.Reg.Ts[:i], proc.Reg.Ts[i+1:])
			return vec, nil
		}
	}
	data, err := proc.Alloc(size)
	if err != nil {
		return nil, err
	}
	vec := vector.New(typ)
	vec.Data = data
	return vec, nil
}

func Put(proc *process.Process, vec *vector.Vector) {
	proc.Reg.Ts = append(proc.Reg.Ts, vec)
}

func FreeRegisters(proc *process.Process) {
	var vec *vector.Vector

	for _, t := range proc.Reg.Ts {
		vec = t.(*vector.Vector)
		vec.Free(proc)
	}
	proc.Reg.Ts = proc.Reg.Ts[:0]
}
