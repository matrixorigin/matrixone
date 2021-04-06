package mergesum

import (
	"bytes"
	"fmt"
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/encoding"
	"matrixbase/pkg/sql/colexec/aggregation"
	"matrixbase/pkg/sql/colexec/aggregation/aggfunc"
	"matrixbase/pkg/vm/process"
	"matrixbase/pkg/vm/register"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString("γ([")
	for i, e := range n.Es {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%s(%s) -> %s", aggregation.AggName[e.Op], e.Name, e.Alias))
	}
	buf.WriteString("]")
}

func Prepare(proc *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	n.Ctr.attrs = make([]string, len(n.Es))
	for i, e := range n.Es {
		n.Ctr.attrs[i] = e.Alias
	}
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	ctr := &n.Ctr
	for {
		if len(proc.Reg.Ws) == 0 {
			break
		}
		for i := 0; i < len(proc.Reg.Ws); i++ {
			reg := proc.Reg.Ws[i]
			v := <-reg.Ch
			if v == nil {
				reg.Wg.Done()
				proc.Reg.Ws = append(proc.Reg.Ws[:i], proc.Reg.Ws[i+1:]...)
				i--
				continue
			}
			bat := v.(*batch.Batch)
			if bat.Attrs == nil {
				reg.Wg.Done()
				continue
			}
			if err := ctr.processBatch(bat, n.Es, proc); err != nil {
				reg.Wg.Done()
				ctr.clean(bat, proc)
				return true, err
			}
			bat.Clean(proc)
			reg.Wg.Done()
		}
	}
	if err := ctr.eval(n.Es, proc); err != nil {
		ctr.clean(nil, proc)
		return true, err
	}
	proc.Reg.Ax = ctr.bat
	ctr.bat = nil
	ctr.clean(nil, proc)
	register.FreeRegisters(proc)
	return true, nil
}

func (ctr *Container) eval(es []aggregation.Extend, proc *process.Process) error {
	var err error

	ctr.bat = batch.New(true, ctr.attrs)
	for i, e := range es {
		if ctr.bat.Vecs[i], err = e.Agg.EvalCopy(proc); err != nil {
			ctr.bat.Vecs = ctr.bat.Vecs[:i]
			return err
		}
		copy(ctr.bat.Vecs[i].Data, encoding.EncodeUint64(proc.Refer[e.Alias]))
	}
	return nil
}

func (ctr *Container) processBatch(bat *batch.Batch, es []aggregation.Extend, proc *process.Process) error {
	for i, e := range es {
		vec, err := bat.GetVector(e.Name, proc)
		if err != nil {
			ctr.bat.Vecs = ctr.bat.Vecs[:i]
			return err
		}
		{
			if e.Agg == nil {
				switch e.Op {
				case aggregation.Avg:
					e.Agg = aggfunc.NewAvg(vec.Typ)
				case aggregation.Max:
					e.Agg = aggfunc.NewMax(vec.Typ)
				case aggregation.Min:
					e.Agg = aggfunc.NewMin(vec.Typ)
				case aggregation.Sum:
					e.Agg = aggfunc.NewSum(vec.Typ)
				case aggregation.Count:
					e.Agg = aggfunc.NewCount(vec.Typ)
				case aggregation.StarCount:
					e.Agg = aggfunc.NewStarCount(vec.Typ)
				case aggregation.SumCount:
					e.Agg = aggfunc.NewSumCount(vec.Typ)
				default:
					ctr.bat.Vecs = ctr.bat.Vecs[:i]
					return fmt.Errorf("unsupport aggregation operator '%v'", e.Op)
				}
				if e.Agg == nil {
					ctr.bat.Vecs = ctr.bat.Vecs[:i]
					return fmt.Errorf("unsupport sumcount aggregation operator '%v' for %s", e.Op, vec.Typ)
				}
				es[i].Agg = e.Agg
			}
		}
		if err := e.Agg.Fill(bat.Sels, vec); err != nil {
			ctr.bat.Vecs = ctr.bat.Vecs[:i]
			return err
		}
	}
	return nil
}

func (ctr *Container) clean(bat *batch.Batch, proc *process.Process) {
	if bat != nil {
		bat.Clean(proc)
	}
	if ctr.bat != nil {
		ctr.bat.Clean(proc)
	}
	register.FreeRegisters(proc)
}
