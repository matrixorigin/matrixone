package limit

import (
	"bytes"
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("limit(%v)", n.Limit))
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

// returning only the first n tuples from its input
func Call(proc *process.Process, arg interface{}) (bool, error) {
	if proc.Reg.Ax == nil {
		return false, nil
	}
	bat := proc.Reg.Ax.(*batch.Batch)
	if bat == nil || bat.Attrs == nil {
		return false, nil
	}
	n := arg.(*Argument)
	if n.Seen >= n.Limit {
		proc.Reg.Ax = nil
		bat.Clean(proc)
		return true, nil
	}
	if len(bat.Sels) > 0 {
		bat.Shuffle(proc)
	}
	length := bat.Length()
	newSeen := n.Seen + uint64(length)
	if newSeen >= n.Limit { // limit - seen
		bat.SetLength(int(n.Limit - n.Seen))
		proc.Reg.Ax = bat
		n.Seen = newSeen
		return true, nil
	}
	n.Seen = newSeen
	proc.Reg.Ax = bat
	return false, nil
}
