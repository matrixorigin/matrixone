package vm

import (
	"matrixbase/pkg/sql/colexec/output"
	"matrixbase/pkg/sql/colexec/projection"
	"matrixbase/pkg/vm/process"
)

func Run(ins Instructions, proc *process.Process) (bool, error) {
	var end bool
	var err error

	for _, in := range ins {
		switch in.Op {
		case Nub:
		case Top:
		case Limit:
		case Merge:
		case Group:
		case Order:
		case Transfer:
		case Restrict:
		case Summarize:
		case Projection:
			end, err = projection.Call(proc, in.Arg)
		case ExtendProjection:
		case SetUnion:
		case SetIntersect:
		case SetDifference:
		case MultisetUnion:
		case MultisetIntersect:
		case MultisetDifference:
		case EqJoin:
		case SemiJoin:
		case InnerJoin:
		case NaturalJoin:
		case Output:
			end, err = output.Call(proc, in.Arg)
		}
		if err != nil {
			return false, err
		}
		if end {
			return true, nil
		}
	}
	return false, nil
}
