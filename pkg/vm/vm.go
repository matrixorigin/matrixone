package vm

import (
	"bytes"
	"matrixbase/pkg/sql/colexec/dedup"
	bnatural "matrixbase/pkg/sql/colexec/hashbag/natural"
	bunion "matrixbase/pkg/sql/colexec/hashbag/union"
	"matrixbase/pkg/sql/colexec/hashset/inner"
	"matrixbase/pkg/sql/colexec/hashset/intersect"
	"matrixbase/pkg/sql/colexec/hashset/natural"
	"matrixbase/pkg/sql/colexec/limit"
	"matrixbase/pkg/sql/colexec/mergededup"
	"matrixbase/pkg/sql/colexec/mergetop"
	"matrixbase/pkg/sql/colexec/offset"
	"matrixbase/pkg/sql/colexec/output"
	"matrixbase/pkg/sql/colexec/projection"
	"matrixbase/pkg/sql/colexec/restrict"
	"matrixbase/pkg/sql/colexec/top"
	"matrixbase/pkg/sql/colexec/transfer"
	"matrixbase/pkg/vm/process"
)

func String(ins Instructions, buf *bytes.Buffer) {
	for i, in := range ins {
		if i > 0 {
			buf.WriteString(" -> ")
		}
		switch in.Op {
		case Top:
			top.String(in.Arg, buf)
		case Dedup:
			dedup.String(in.Arg, buf)
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
		case SetInnerJoin:
			inner.String(in.Arg, buf)
		case SetNaturalJoin:
			natural.String(in.Arg, buf)
		case BagUnion:
			bunion.String(in.Arg, buf)
		case BagNaturalJoin:
			bnatural.String(in.Arg, buf)
		case Output:
			output.String(in.Arg, buf)
		case MergeTop:
			mergetop.String(in.Arg, buf)
		case MergeDedup:
			mergededup.String(in.Arg, buf)
		}
	}
}

func Clean(ins Instructions, proc *process.Process) {
	for _, in := range ins {
		switch in.Op {
		case Top:
		case Dedup:
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
		case Top:
			if err := top.Prepare(proc, in.Arg); err != nil {
				return err
			}
		case Dedup:
			if err := dedup.Prepare(proc, in.Arg); err != nil {
				return err
			}
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
		case SetInnerJoin:
			if err := inner.Prepare(proc, in.Arg); err != nil {
				return err
			}
		case SetNaturalJoin:
			if err := natural.Prepare(proc, in.Arg); err != nil {
				return err
			}
		case BagUnion:
			if err := bunion.Prepare(proc, in.Arg); err != nil {
				return err
			}
		case BagNaturalJoin:
			if err := bnatural.Prepare(proc, in.Arg); err != nil {
				return err
			}
		case Output:
			if err := output.Prepare(proc, in.Arg); err != nil {
				return err
			}
		case MergeTop:
			if err := mergetop.Prepare(proc, in.Arg); err != nil {
				return err
			}
		case MergeDedup:
			if err := mergededup.Prepare(proc, in.Arg); err != nil {
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
		case Top:
			ok, err = top.Call(proc, in.Arg)
		case Dedup:
			ok, err = dedup.Call(proc, in.Arg)
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
		case SetInnerJoin:
			ok, err = inner.Call(proc, in.Arg)
		case SetNaturalJoin:
			ok, err = natural.Call(proc, in.Arg)
		case BagUnion:
			ok, err = bunion.Call(proc, in.Arg)
		case BagNaturalJoin:
			ok, err = bnatural.Call(proc, in.Arg)
		case Output:
			ok, err = output.Call(proc, in.Arg)
		case MergeTop:
			ok, err = mergetop.Call(proc, in.Arg)
		case MergeDedup:
			ok, err = mergededup.Call(proc, in.Arg)
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
