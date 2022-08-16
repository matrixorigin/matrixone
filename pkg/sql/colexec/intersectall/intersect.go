package intersectall

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	Build = iota
	Probe
	End
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" intersect all ")
}

func Prepare(proc *process.Process, arg any) error {
	var err error

	ap := arg.(*Argument)
	ap.ctr = new(container)
	//false:忽略null值
	//true: 处理null值
	if ap.ctr.mp, err = hashmap.NewStrMap(true, ap.Ibucket, ap.Nbucket, proc.GetMheap()); err != nil {
		return err
	}
	ap.ctr.inBuckets = make([]uint8, hashmap.UnitLimit)
	return nil
}

// Call
func Call(idx int, proc *process.Process, arg any) (bool, error) {
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	ap := arg.(*Argument)
	ctr := ap.ctr
	for {
		switch ctr.state {
		case Build:
			println("TODO")
		case Probe:
			println("TODO")
		case End:
			proc.SetInputBatch(nil)
			return true, nil
		}
	}
	return true, nil
}
