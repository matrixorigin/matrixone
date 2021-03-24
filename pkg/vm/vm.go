package vm

import (
	"bytes"
	"matrixbase/pkg/sql/colexec/hashset/intersect"
	"matrixbase/pkg/sql/colexec/hashset/natural"
	"matrixbase/pkg/sql/colexec/limit"
	"matrixbase/pkg/sql/colexec/offset"
	"matrixbase/pkg/sql/colexec/output"
	"matrixbase/pkg/sql/colexec/projection"
	"matrixbase/pkg/sql/colexec/restrict"
	"matrixbase/pkg/sql/colexec/transfer"
	"matrixbase/pkg/vm/process"
)

func String(ins Instructions, buf *bytes.Buffer) {
	for i, in := range ins {
		if i > 0 {
			buf.WriteString(" -> ")
		}
		switch in.Op {
		case Nub:
		case Top:
		case Limit:
			limit.String(in.Arg, buf)
		case Group:
		case Order:
		case Offset:
			offset.String(in.Arg, buf)
		case Transfer:
			transfer.String(in.Arg, buf)
		case Restrict:
			restrict.String(in.Arg, buf)
		case Summarize:
		case Projection:
			projection.String(in.Arg, buf)
		case SetUnion:
		case SetIntersect:
			intersect.String(in.Arg, buf)
		case SetDifference:
		case SetNaturalJoin:
			natural.String(in.Arg, buf)
		case Output:
			output.String(in.Arg, buf)
		}
	}
}

func Clean(ins Instructions, proc *process.Process) {
	for _, in := range ins {
		switch in.Op {
		case Nub:
		case Top:
		case Limit:
		case Group:
		case Order:
		case Offset:
		case Transfer:
		case Restrict:
		case Summarize:
		case Projection:
		case SetUnion:
		case SetIntersect:
		case SetDifference:
		case Output:
		}
	}
}

func Prepare(ins Instructions, proc *process.Process) error {
	for _, in := range ins {
		switch in.Op {
		case Nub:
		case Top:
		case Limit:
			if err := limit.Prepare(proc, in.Arg); err != nil {
				return err
			}
		case Group:
		case Order:
		case Offset:
			if err := offset.Prepare(proc, in.Arg); err != nil {
				return err
			}
		case Transfer:
			if err := transfer.Prepare(proc, in.Arg); err != nil {
				return err
			}
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
			if err := intersect.Prepare(proc, in.Arg); err != nil {
				return err
			}
		case SetDifference:
		case SetNaturalJoin:
			if err := natural.Prepare(proc, in.Arg); err != nil {
				return err
			}
		case Output:
			if err := output.Prepare(proc, in.Arg); err != nil {
				return err
			}
		}
	}
	return nil
}

func Run(ins Instructions, proc *process.Process) (bool, error) {
	var ok bool
	var end bool
	var err error

	for _, in := range ins {
		switch in.Op {
		case Nub:
		case Top:
		case Limit:
			ok, err = limit.Call(proc, in.Arg)
		case Group:
		case Order:
		case Offset:
			ok, err = offset.Call(proc, in.Arg)
		case Transfer:
			ok, err = transfer.Call(proc, in.Arg)
		case Restrict:
			ok, err = restrict.Call(proc, in.Arg)
		case Summarize:
		case Projection:
			ok, err = projection.Call(proc, in.Arg)
		case SetUnion:
		case SetIntersect:
			ok, err = intersect.Call(proc, in.Arg)
		case SetDifference:
		case SetNaturalJoin:
			ok, err = natural.Call(proc, in.Arg)
		case Output:
			ok, err = output.Call(proc, in.Arg)
		}
		if err != nil {
			return false, err
		}
		if ok {
			end = true
		}
	}
	return end, nil
}
