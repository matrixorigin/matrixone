package restrict

import (
	"bytes"
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/vm/process"
	"matrixone/pkg/vm/register"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("σ(%s)", n.E))
}

func Prepare(_ *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	n.Attrs = n.E.Attributes()
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	if proc.Reg.Ax == nil {
		return false, nil
	}
	bat := proc.Reg.Ax.(*batch.Batch)
	if bat == nil || bat.Attrs == nil {
		return false, nil
	}
	n := arg.(*Argument)
	if _, ok := n.E.(*extend.Attribute); ok { // mysql treats any attribute as true
		proc.Reg.Ax = bat
		return false, nil
	}
	if es := extend.AndExtends(n.E, []extend.Extend{}); len(es) > 0 {
		for _, e := range es {
			vec, _, err := e.Eval(bat, proc)
			if err != nil {
				bat.Clean(proc)
				return false, nil
			}
			sels := vec.Col.([]int64)
			if len(sels) == 0 {
				bat.Clean(proc)
				bat.Attrs = nil
				proc.Reg.Ax = bat
				return false, nil
			}
			for i, vec := range bat.Vecs {
				bat.Vecs[i] = vec.Shuffle(sels)
			}
			register.Put(proc, vec)
		}
		for _, vec := range bat.Vecs { // reset reference count of vector
			if vec.Ref == 0 {
				vec.Ref = 2
			}
		}
		bat.Reduce(n.Attrs, proc)
		proc.Reg.Ax = bat
		return false, nil
	}
	vec, _, err := n.E.Eval(bat, proc)
	if err != nil {
		bat.Clean(proc)
		return false, err
	}
	bat.SelsData = vec.Data
	bat.Sels = vec.Col.([]int64)
	if len(bat.Sels) > 0 {
		bat.Reduce(n.Attrs, proc)
		proc.Reg.Ax = bat
	} else {
		bat.Clean(proc)
		bat.Attrs = nil
		proc.Reg.Ax = bat
	}
	return false, nil
}
