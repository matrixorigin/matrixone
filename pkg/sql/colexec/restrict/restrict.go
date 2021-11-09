package restrict

import (
	"bytes"
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/process"
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
	bat := proc.Reg.InputBatch
	if bat == nil || len(bat.Ring.Zs) == 0 {
		return false, nil
	}
	n := arg.(*Argument)
	vec, _, err := n.E.Eval(bat, proc)
	if err != nil {
		batch.Clean(bat, proc.Mp)
		return false, err
	}
	sels := vec.Col.([]int64)
	if len(sels) == 0 {
		bat.Ring.Zs = bat.Ring.Zs[:0]
		proc.Reg.InputBatch = bat
		return false, nil
	}
	batch.Reduce(bat, n.E.Attributes(), proc.Mp)
	for i, vec := range bat.Vecs {
		if bat.Vecs[i], err = vector.Shuffle(vec, sels, proc.Mp); err != nil {
			batch.Clean(bat, proc.Mp)
			return false, err
		}
	}
	process.Put(proc, vec)
	proc.Reg.InputBatch = bat
	return false, nil
}
