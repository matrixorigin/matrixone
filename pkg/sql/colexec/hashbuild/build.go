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

package hashbuild

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" hash build ")
}

func Prepare(proc *process.Process, arg any) (err error) {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.InitReceiver(proc, false)
	if ap.NeedHashMap {
		if ap.ctr.mp, err = hashmap.NewStrMap(false, ap.Ibucket, ap.Nbucket, proc.Mp()); err != nil {
			return err
		}
		ap.ctr.vecs = make([]*vector.Vector, len(ap.Conditions))

		ctr := ap.ctr
		ctr.evecs = make([]evalVector, len(ap.Conditions))
		for i := range ctr.evecs {
			ctr.evecs[i].executor, err = colexec.NewExpressionExecutor(proc, ap.Conditions[i])
			if err != nil {
				return err
			}
		}
	}
	ap.ctr.bat = batch.NewWithSize(len(ap.Typs))
	for i, typ := range ap.Typs {
		ap.ctr.bat.Vecs[i] = vector.NewVec(typ)
	}

	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, _ bool) (bool, error) {
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	ap := arg.(*Argument)
	ctr := ap.ctr
	for {
		switch ctr.state {
		case BuildHashMap:
			if err := ctr.build(ap, proc, anal, isFirst); err != nil {
				ctr.cleanHashMap()
				return false, err
			}
			if ap.ctr.mp != nil {
				anal.Alloc(ap.ctr.mp.Size())
			}
			ctr.state = HandleRuntimeFilter

		case HandleRuntimeFilter:
			if err := ctr.handleRuntimeFilter(ap, proc); err != nil {
				return false, err
			}

		case Eval:
			if ctr.bat != nil && ctr.bat.Length() != 0 {
				if ap.NeedHashMap {
					ctr.bat.AuxData = hashmap.NewJoinMap(ctr.sels, nil, ctr.mp, ctr.hasNull)
				}

				proc.SetInputBatch(ctr.bat)
				ctr.mp = nil
				ctr.bat = nil
				ctr.sels = nil
			} else {
				ctr.cleanHashMap()
				proc.SetInputBatch(nil)
			}
			ctr.state = End
			return false, nil

		default:
			proc.SetInputBatch(nil)
			return true, nil
		}
	}
}

func (ctr *container) build(ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool) error {
	var err error

	for {
		bat, _, err := ctr.ReceiveFromSingleReg(0, anal)
		if err != nil {
			return err
		}

		if bat == nil {
			break
		}
		if bat.Length() == 0 {
			bat.Clean(proc.Mp())
			continue
		}
		anal.Input(bat, isFirst)
		anal.Alloc(int64(bat.Size()))
		if ctr.bat, err = ctr.bat.Append(proc.Ctx, proc.Mp(), bat); err != nil {
			return err
		}
		bat.Clean(proc.Mp())
	}
	if ctr.bat == nil || ctr.bat.Length() == 0 || !ap.NeedHashMap {
		return nil
	}

	if err = ctr.evalJoinCondition(ctr.bat, proc); err != nil {
		return err
	}

	itr := ctr.mp.NewIterator()
	count := ctr.bat.Length()
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}

		oldRowNumberOfHashTable := ctr.mp.GroupCount()
		vals, zvals, err := itr.Insert(i, n, ctr.vecs)
		if err != nil {
			return err
		}
		for k, v := range vals[:n] {
			if zvals[k] == 0 {
				ctr.hasNull = true
				continue
			}
			if v == 0 {
				continue
			}

			for v > oldRowNumberOfHashTable {
				ctr.sels = append(ctr.sels, make([]int32, 0))
				oldRowNumberOfHashTable++
			}
			ai := int64(v) - 1
			ctr.sels[ai] = append(ctr.sels[ai], int32(i+k))
		}
	}
	return nil
}

func (ctr *container) handleRuntimeFilter(ap *Argument, proc *process.Process) error {
	if len(ap.RuntimeFilterSenders) == 0 {
		ctr.state = Eval
		return nil
	}

	var runtimeFilter *pipeline.RuntimeFilter

	sels := make([]int32, 0, len(ctr.sels))
	for _, sel := range ctr.sels {
		if len(sel) > 0 {
			sels = append(sels, sel[0])
		}
	}

	vec := ctr.vecs[0]
	if len(sels) == 0 || vec == nil || vec.Length() == 0 {
		select {
		case <-proc.Ctx.Done():
			ctr.state = End

		case ap.RuntimeFilterSenders[0].Chan <- nil:
			ctr.state = Eval
		}

		return nil
	}

	// Composite primary key
	if len(ctr.vecs) > 1 && len(ctr.sels) <= plan.BloomFilterCardLimit {
		bat := batch.NewWithSize(len(ctr.vecs))
		bat.SetRowCount(ctr.vecs[0].Length())
		copy(bat.Vecs, ctr.vecs)

		newVec, err := colexec.EvalExpressionOnce(proc, ap.RuntimeFilterSenders[0].Spec.Expr, []*batch.Batch{bat})
		if err != nil {
			return err
		}

		vec = newVec
	}

	defer func() {
		if vec != ctr.vecs[0] {
			vec.Free(proc.Mp())
			vec = nil
		}
	}()

	if len(ctr.sels) <= plan.InFilterCardLimit {
		inList := vector.NewVec(*vec.GetType())
		if err := inList.Union(vec, sels, proc.Mp()); err != nil {
			return err
		}

		defer inList.Free(proc.Mp())

		colexec.SortInFilter(inList)
		data, err := inList.MarshalBinary()
		if err != nil {
			return err
		}

		runtimeFilter = &pipeline.RuntimeFilter{
			Typ:  pipeline.RuntimeFilter_IN,
			Data: data,
		}
	} else if len(ctr.sels) <= plan.BloomFilterCardLimit {
		zm := objectio.NewZM(vec.GetType().Oid, vec.GetType().Scale)
		for i := range sels {
			bs := vec.GetRawBytesAt(int(sels[i]))
			index.UpdateZM(zm, bs)
		}

		runtimeFilter = &pipeline.RuntimeFilter{
			Typ:  pipeline.RuntimeFilter_MIN_MAX,
			Data: zm,
		}
	} else {
		runtimeFilter = &pipeline.RuntimeFilter{
			Typ: pipeline.RuntimeFilter_NO_FILTER,
		}
	}

	select {
	case <-proc.Ctx.Done():
		ctr.state = End

	case ap.RuntimeFilterSenders[0].Chan <- runtimeFilter:
		ctr.state = Eval
	}

	return nil
}

func (ctr *container) evalJoinCondition(bat *batch.Batch, proc *process.Process) error {
	for i := range ctr.evecs {
		vec, err := ctr.evecs[i].executor.Eval(proc, []*batch.Batch{bat})
		if err != nil {
			ctr.cleanEvalVectors(proc.Mp())
			return err
		}
		ctr.vecs[i] = vec
		ctr.evecs[i].vec = vec
	}
	return nil
}
