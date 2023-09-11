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
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const batchSize = 8192

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" hash build ")
}

func Prepare(proc *process.Process, arg any) (err error) {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	if len(proc.Reg.MergeReceivers) > 1 {
		ap.ctr.InitReceiver(proc, true)
		ap.ctr.isMerge = true
	} else {
		ap.ctr.InitReceiver(proc, false)
	}

	if ap.NeedHashMap {
		ap.ctr.vecs = make([]*vector.Vector, len(ap.Conditions))
		ctr := ap.ctr
		ctr.evecs = make([]evalVector, len(ap.Conditions))
		ctr.keyWidth = 0
		for i, expr := range ap.Conditions {
			typ := expr.Typ
			width := types.T(typ.Id).TypeLen()
			// todo : for varlena type, always go strhashmap
			if types.T(typ.Id).FixedLength() < 0 {
				width = 128
			}
			ctr.keyWidth += width
			ctr.evecs[i].executor, err = colexec.NewExpressionExecutor(proc, ap.Conditions[i])
			if err != nil {
				return err
			}
		}

		if ctr.keyWidth <= 8 {
			if ctr.intHashMap, err = hashmap.NewIntHashMap(false, ap.Ibucket, ap.Nbucket, proc.Mp()); err != nil {
				return err
			}
		} else {
			if ctr.strHashMap, err = hashmap.NewStrMap(false, ap.Ibucket, ap.Nbucket, proc.Mp()); err != nil {
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

func Call(idx int, proc *process.Process, arg any, isFirst bool, _ bool) (process.ExecStatus, error) {
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
				return process.ExecNext, err
			}
			if ap.ctr.intHashMap != nil {
				anal.Alloc(ap.ctr.intHashMap.Size())
			} else if ap.ctr.strHashMap != nil {
				anal.Alloc(ap.ctr.strHashMap.Size())
			}
			ctr.state = HandleRuntimeFilter

		case HandleRuntimeFilter:
			if err := ctr.handleRuntimeFilter(ap, proc); err != nil {
				return process.ExecNext, err
			}

		case Eval:
			if ctr.bat != nil && ctr.inputBatchRowCount != 0 {
				if ap.NeedHashMap {
					if ctr.keyWidth <= 8 {
						ctr.bat.AuxData = hashmap.NewJoinMap(ctr.multiSels, nil, ctr.intHashMap, nil, ctr.hasNull, ap.IsDup)
					} else {
						ctr.bat.AuxData = hashmap.NewJoinMap(ctr.multiSels, nil, nil, ctr.strHashMap, ctr.hasNull, ap.IsDup)
					}
				}

				proc.SetInputBatch(ctr.bat)
				ctr.intHashMap = nil
				ctr.strHashMap = nil
				ctr.bat = nil
				ctr.multiSels = nil
			} else {
				ctr.cleanHashMap()
				proc.SetInputBatch(nil)
			}
			ctr.state = End
			return process.ExecNext, nil

		default:
			proc.SetInputBatch(nil)
			return process.ExecStop, nil
		}
	}
}

func (ctr *container) mergeBuildBatches(ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool) error {
	var err error

	batches := make([]*batch.Batch, 0)
	rowCount := 0
	var tmpBatch *batch.Batch

	for {
		var bat *batch.Batch
		if ap.ctr.isMerge {
			bat, _, err = ctr.ReceiveFromAllRegs(anal)
		} else {
			bat, _, err = ctr.ReceiveFromSingleReg(0, anal)
		}

		if err != nil {
			return err
		}

		if bat == nil {
			break
		}
		if bat.IsEmpty() {
			proc.PutBatch(bat)
			continue
		}
		anal.Input(bat, isFirst)
		anal.Alloc(int64(bat.Size()))

		rowCount += bat.RowCount()
		if bat.RowCount() >= batchSize/2 {
			batches = append(batches, bat)
		} else {
			if tmpBatch == nil {
				tmpBatch, err = bat.Dup(proc.Mp())
				if err != nil {
					return err
				}
			} else {
				tmpBatch, err = tmpBatch.Append(proc.Ctx, proc.Mp(), bat)
				if err != nil {
					return err
				}
			}
			if tmpBatch.RowCount() >= batchSize {
				batches = append(batches, tmpBatch)
				tmpBatch = nil
			}
			proc.PutBatch(bat)
		}
	}

	if tmpBatch != nil {
		batches = append(batches, tmpBatch)
	}
	err = ctr.bat.PreExtend(proc.Mp(), rowCount)
	if err != nil {
		return err
	}

	for i := range batches {
		if ctr.bat, err = ctr.bat.Append(proc.Ctx, proc.Mp(), batches[i]); err != nil {
			return err
		}
		proc.PutBatch(batches[i])
	}
	return nil
}

func (ctr *container) buildHashmapByMergedBatch(ap *Argument, proc *process.Process) error {
	if ctr.bat == nil || ctr.bat.RowCount() == 0 || !ap.NeedHashMap {
		return nil
	}
	var err error
	if err = ctr.evalJoinCondition(ctr.bat, proc); err != nil {
		return err
	}

	var itr hashmap.Iterator
	if ctr.keyWidth <= 8 {
		itr = ctr.intHashMap.NewIterator()
	} else {
		itr = ctr.strHashMap.NewIterator()
	}
	count := ctr.bat.RowCount()

	if ap.HashOnPK {
		// if hash on primary key, prealloc hashmap size to the count of batch
		if ctr.keyWidth <= 8 {
			err = ctr.intHashMap.PreAlloc(uint64(count), proc.Mp())
		} else {
			err = ctr.strHashMap.PreAlloc(uint64(count), proc.Mp())
		}
		if err != nil {
			return err
		}
	} else {
		ctr.multiSels = make([][]int32, count)
	}

	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}

		// if not hash on primary key, estimate the hashmap size after 8192 rows
		//preAlloc to improve performance and reduce memory reAlloc
		if !ap.HashOnPK && count > hashmap.HashMapSizeThreshHold && i == hashmap.HashMapSizeEstimate {
			if ctr.keyWidth <= 8 {
				groupCount := ctr.intHashMap.GroupCount()
				rate := float64(groupCount) / float64(i)
				hashmapCount := uint64(float64(count) * rate)
				if hashmapCount > groupCount {
					err = ctr.intHashMap.PreAlloc(hashmapCount-groupCount, proc.Mp())
				}
			} else {
				groupCount := ctr.strHashMap.GroupCount()
				rate := float64(groupCount) / float64(i)
				hashmapCount := uint64(float64(count) * rate)
				if hashmapCount > groupCount {
					err = ctr.strHashMap.PreAlloc(hashmapCount-groupCount, proc.Mp())
				}
			}
			if err != nil {
				return err
			}
		}

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
			ai := int64(v) - 1

			if !ap.HashOnPK {
				if ctr.multiSels[ai] == nil {
					ctr.multiSels[ai] = make([]int32, 0)
				}
				ctr.multiSels[ai] = append(ctr.multiSels[ai], int32(i+k))
			}
		}
	}
	return nil
}

func (ctr *container) build(ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool) error {
	err := ctr.mergeBuildBatches(ap, proc, anal, isFirst)
	if err != nil {
		return err
	}
	ctr.inputBatchRowCount = ctr.bat.RowCount()
	err = ctr.buildHashmapByMergedBatch(ap, proc)
	if err != nil {
		return err
	}
	if !ap.NeedMergedBatch && ctr.inputBatchRowCount >= 8192 {
		// if do not need merged batch, free it now to save memory
		// for further optimization, do not merge input batches to get best performance
		ctr.cleanBatch(proc.Mp())
		ctr.bat = batch.NewWithSize(len(ap.Typs))
	}
	return nil
}

func (ctr *container) handleRuntimeFilter(ap *Argument, proc *process.Process) error {
	if len(ap.RuntimeFilterSenders) == 0 {
		ctr.state = Eval
		return nil
	}

	if ap.RuntimeFilterSenders[0].Spec.Expr == nil {
		runtimeFilter := &pipeline.RuntimeFilter{
			Typ: pipeline.RuntimeFilter_NO_FILTER,
		}

		select {
		case <-proc.Ctx.Done():
			ctr.state = End

		case ap.RuntimeFilterSenders[0].Chan <- runtimeFilter:
			ctr.state = Eval
		}

		return nil
	}

	vec := ctr.vecs[0]
	if ctr.inputBatchRowCount == 0 || vec == nil || vec.Length() == 0 {
		select {
		case <-proc.Ctx.Done():
			ctr.state = End

		case ap.RuntimeFilterSenders[0].Chan <- nil:
			ctr.state = Eval
		}

		return nil
	}

	var runtimeFilter *pipeline.RuntimeFilter
	var hashmapCount uint64
	if ctr.keyWidth <= 8 {
		hashmapCount = ctr.intHashMap.GroupCount()
	} else {
		hashmapCount = ctr.strHashMap.GroupCount()
	}

	var sels []int32
	if !ap.HashOnPK {
		sels = make([]int32, 0, hashmapCount)
		for _, sel := range ctr.multiSels {
			if len(sel) > 0 {
				sels = append(sels, sel[0])
			}
		}
	}

	inFilterCardLimit := int64(plan.InFilterCardLimit)
	v, ok := runtime.ProcessLevelRuntime().GetGlobalVariables("runtime_filter_limit_in")
	if ok {
		inFilterCardLimit = v.(int64)
	}
	bloomFilterCardLimit := int64(plan.BloomFilterCardLimit)
	v, ok = runtime.ProcessLevelRuntime().GetGlobalVariables("runtime_filter_limit_bloom_filter")
	if ok {
		bloomFilterCardLimit = v.(int64)
	}

	// Composite primary key
	if len(ctr.vecs) > 1 && hashmapCount <= uint64(bloomFilterCardLimit) {
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

	var err error
	if hashmapCount <= uint64(inFilterCardLimit) {
		var inList *vector.Vector
		if ap.HashOnPK {
			if inList, err = vec.Dup(proc.Mp()); err != nil {
				return err
			}
		} else {
			inList = vector.NewVec(*vec.GetType())
			if err = inList.Union(vec, sels, proc.Mp()); err != nil {
				return err
			}
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
	} else if hashmapCount <= uint64(bloomFilterCardLimit) {
		zm := objectio.NewZM(vec.GetType().Oid, vec.GetType().Scale)
		if ap.HashOnPK {
			length := vec.Length()
			for i := 0; i < length; i++ {
				bs := vec.GetRawBytesAt(i)
				index.UpdateZM(zm, bs)
			}
		} else {
			for i := range sels {
				bs := vec.GetRawBytesAt(int(sels[i]))
				index.UpdateZM(zm, bs)
			}
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
