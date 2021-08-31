package mergesum

import (
	"bytes"
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/sql/colexec/aggregation/aggfunc"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
	"reflect"
	"unsafe"
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
	n.Ctr.refer = n.Refer
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
		switch ctr.state {
		case Build:
			if err := ctr.build(n, proc); err != nil {
				ctr.clean(proc)
				ctr.state = End
				return true, err
			}
			ctr.state = Eval
		case Eval:
			if err := ctr.eval(n.Es, proc); err != nil {
				ctr.clean(proc)
				ctr.state = End
				return true, err
			}
			proc.Reg.Ax = ctr.bat
			ctr.bat = nil
			ctr.clean(proc)
			ctr.state = End
			return true, nil
		case End:
			proc.Reg.Ax = nil
			return true, nil
		}
	}
}

func (ctr *Container) build(n *Argument, proc *process.Process) error {
	for {
		if len(proc.Reg.Ws) == 0 {
			break
		}
		for i := 0; i < len(proc.Reg.Ws); i++ {
			reg := proc.Reg.Ws[i]
			v := <-reg.Ch
			if v == nil {
				reg.Ch = nil
				reg.Wg.Done()
				proc.Reg.Ws = append(proc.Reg.Ws[:i], proc.Reg.Ws[i+1:]...)
				i--
				continue
			}
			bat := v.(*batch.Batch)
			if bat == nil || bat.Attrs == nil {
				reg.Wg.Done()
				continue
			}
			if err := ctr.processBatch(bat, n.Es, proc); err != nil {
				reg.Ch = nil
				reg.Wg.Done()
				bat.Clean(proc)
				return err
			}
			bat.Clean(proc)
			reg.Wg.Done()
		}
	}
	return nil
}

func (ctr *Container) eval(es []aggregation.Extend, proc *process.Process) error {
	var err error

	if es[0].Agg == nil {
		ctr.bat = batch.New(true, nil)
		return nil
	}
	ctr.bat = batch.New(true, ctr.attrs)
	for i, e := range es {
		if ctr.bat.Vecs[i], err = e.Agg.EvalCopy(proc); err != nil {
			ctr.bat.Vecs = ctr.bat.Vecs[:i]
			return err
		}
		switch e.Agg.Type().Oid {
		case types.T_tuple:
			data, err := proc.Alloc(0)
			if err != nil {
				ctr.bat.Vecs = ctr.bat.Vecs[:i]
				return err
			}
			ctr.bat.Vecs[i].Data = data[:mempool.CountSize]
		}
		count := ctr.refer[e.Alias]
		hp := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&count)), Len: 8, Cap: 8}
		copy(ctr.bat.Vecs[i].Data, *(*[]byte)(unsafe.Pointer(&hp)))
	}
	return nil
}

func (ctr *Container) processBatch(bat *batch.Batch, es []aggregation.Extend, proc *process.Process) error {
	for i, e := range es {
		vec := bat.GetVector(e.Name, proc)
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

func (ctr *Container) clean(proc *process.Process) {
	if ctr.bat != nil {
		ctr.bat.Clean(proc)
	}
	{
		for _, reg := range proc.Reg.Ws {
			if reg.Ch != nil {
				v := <-reg.Ch
				switch {
				case v == nil:
					reg.Ch = nil
					reg.Wg.Done()
				default:
					bat := v.(*batch.Batch)
					if bat == nil || bat.Attrs == nil {
						reg.Ch = nil
						reg.Wg.Done()
					} else {
						bat.Clean(proc)
						reg.Ch = nil
						reg.Wg.Done()
					}
				}
			}
		}
	}
}
