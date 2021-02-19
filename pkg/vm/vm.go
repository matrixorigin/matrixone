package vm

import "matrixbase/pkg/vm/process"

func Run(ins Instructions, proc *process.Process) (bool, error) {
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
		}
	}
	return false, nil
}
