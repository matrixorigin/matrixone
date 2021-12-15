// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package summarize

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggregation"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggregation/aggfunc"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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

func Prepare(_ *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	{
		n.Ctr.attrs = make([]string, len(n.Es))
		for i, e := range n.Es {
			n.Ctr.attrs[i] = e.Alias
		}
		n.Ctr.refer = n.Refer
	}
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	if proc.Reg.InputBatch == nil {
		return false, nil
	}
	bat := proc.Reg.InputBatch.(*batch.Batch)
	if bat == nil || bat.Attrs == nil {
		return false, nil
	}
	n := arg.(*Argument)
	ctr := &n.Ctr
	if err := ctr.processBatch(bat, n.Es, proc); err != nil {
		bat.Clean(proc)
		ctr.clean(proc)
		return false, err
	}
	bat.Clean(proc)
	proc.Reg.InputBatch = ctr.bat
	ctr.bat = nil
	return false, nil
}

func (ctr *Container) processBatch(bat *batch.Batch, es []aggregation.Extend, proc *process.Process) error {
	var err error

	ctr.bat = batch.New(true, ctr.attrs)
	for i, e := range es {
		vec := bat.GetVector(e.Name)
		{
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
		if err = e.Agg.Fill(bat.Sels, vec); err != nil {
			ctr.bat.Vecs = ctr.bat.Vecs[:i]
			return err
		}
		if ctr.bat.Vecs[i], err = e.Agg.EvalCopy(proc); err != nil {
			ctr.bat.Vecs = ctr.bat.Vecs[:i]
			return err
		}
		ctr.bat.Vecs[i].Ref = ctr.refer[e.Alias]
	}
	return nil
}

func (ctr *Container) clean(proc *process.Process) {
	if ctr.bat != nil {
		ctr.bat.Clean(proc)
	}
}
