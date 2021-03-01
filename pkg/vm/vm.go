package vm

import (
	"matrixbase/pkg/sql/colexec/output"
	"matrixbase/pkg/sql/colexec/projection"
	"matrixbase/pkg/sql/colexec/restrict"
	"matrixbase/pkg/vm/process"
)

func Prepare(ins Instructions, proc *process.Process) error {
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
			if err := restrict.Prepare(proc, in.Arg); err != nil {
				return err
			}
		case Summarize:
		case Projection:
			if err := projection.Prepare(proc, in.Arg); err != nil {
				return err
			}
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
		}
	}
	return nil
}
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
			end, err = restrict.Call(proc, in.Arg)
		case Summarize:
		case Projection:
			end, err = projection.Call(proc, in.Arg)
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
