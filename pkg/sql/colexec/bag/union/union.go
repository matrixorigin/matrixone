package union

import (
	"bytes"
	"fmt"
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("%s ∪  %s", n.R, n.S))
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

// sql union all is just a fake bag union
func Call(proc *process.Process, arg interface{}) (bool, error) {
	for {
		if len(proc.Reg.Ws) == 0 {
			proc.Reg.Ax = nil
			return true, nil
		}
		reg := proc.Reg.Ws[0]
		v := <-reg.Ch
		if v == nil {
			reg.Wg.Done()
			proc.Reg.Ws = proc.Reg.Ws[1:]
			continue
		}
		reg.Wg.Done()
		proc.Reg.Ax = v.(*batch.Batch)
		break
	}
	return false, nil
}
