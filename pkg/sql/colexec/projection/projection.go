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

package projection

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "projection"

func (projection *Projection) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": projection(")
	for i, e := range projection.ProjectList {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(e.String())
	}
	buf.WriteString(")")
}

func (projection *Projection) OpType() vm.OpType {
	return vm.Projection
}

func (projection *Projection) Prepare(proc *process.Process) (err error) {
	if projection.OpAnalyzer == nil {
		projection.OpAnalyzer = process.NewAnalyzer(projection.GetIdx(), projection.IsFirst, projection.IsLast, "projection")
	} else {
		projection.OpAnalyzer.Reset()
	}

	if len(projection.ctr.projExecutors) == 0 {
		projection.ctr.projExecutors, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, projection.ProjectList)

		projection.ctr.buf = batch.NewWithSize(len(projection.ProjectList))
		if projection.GroupingSetCount > 0 {
			projection.ctr.expandBuf = batch.NewWithSize(len(projection.ProjectList))
		}
	}
	if projection.GroupingSetCount > 0 {
		if len(projection.ProjectList) < 3 || len(projection.GroupingFlags) == 0 ||
			len(projection.GroupingFlags)%projection.GroupingSetCount != 0 ||
			len(projection.GroupingFlags)/projection.GroupingSetCount > len(projection.ProjectList)-2 ||
			types.T(projection.ProjectList[len(projection.ProjectList)-2].Typ.Id) != types.T_bool ||
			types.T(projection.ProjectList[len(projection.ProjectList)-1].Typ.Id) != types.T_int64 {
			return moerr.NewInternalErrorNoCtx("invalid grouping-set projection metadata")
		}
	}
	return err
}

func (projection *Projection) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := projection.OpAnalyzer
	if projection.GroupingSetCount > 0 && projection.ctr.hasInput {
		return projection.emitGroupingSet(proc)
	}
	if projection.GroupingSetCount > 0 && projection.ctr.childDone {
		return projection.emitRuntimeEmptyGroupingSet(proc)
	}

	result, err := vm.ChildrenCall(projection.GetChildren(0), proc, analyzer)
	if err != nil {
		return result, err
	}

	if result.Batch == nil {
		if projection.GroupingSetCount > 0 {
			projection.ctr.childDone = true
			if !projection.ctr.inputSeen {
				return projection.emitRuntimeEmptyGroupingSet(proc)
			}
		}
		return result, nil
	}
	if result.Batch.IsEmpty() || result.Batch.Last() {
		return result, nil
	}
	bat := result.Batch
	projection.ctr.inputSeen = true

	// keep shuffleIDX unchanged
	projection.ctr.buf.ShuffleIDX = bat.ShuffleIDX
	batches := []*batch.Batch{bat}
	for i := range projection.ctr.projExecutors {
		vec, err := projection.ctr.projExecutors[i].Eval(proc, batches, nil)
		if err != nil {
			return vm.CancelResult, err
		}
		// for projection operator, all Vectors of projectBat come from executor.Eval
		// and will not be modified within projection operator. so we can used the result of executor.Eval directly.
		// (if operator will modify vector/agg of batch, you should make a copy)
		// however, it should be noted that since they directly come from executor.Eval
		// these vectors cannot be free by batch.Clean directly and must be handed over executor.Free
		projection.ctr.buf.Vecs[i] = vec
	}
	projection.maxAllocSize = max(projection.maxAllocSize, projection.ctr.buf.Size())
	projection.ctr.buf.SetRowCount(bat.RowCount())
	if projection.GroupingSetCount > 0 {
		projection.ctr.hasInput = true
		projection.ctr.nextSet = 0
		return projection.emitGroupingSet(proc)
	}

	result.Batch = projection.ctr.buf
	return result, nil
}

// emitRuntimeEmptyGroupingSet emits one key-only row for each grouping set
// whose keys are all rolled up when the child produced no data. The
// penultimate true marker tells Group to publish the key while treating the row
// as GroupNotMatched for every aggregate, which preserves empty aggregate
// states (COUNT(*) = 0, SUM = NULL, and so on).
func (projection *Projection) emitRuntimeEmptyGroupingSet(proc *process.Process) (vm.CallResult, error) {
	groupCount := len(projection.GroupingFlags) / projection.GroupingSetCount
	set := -1
	for projection.ctr.nextSet < projection.GroupingSetCount {
		candidate := projection.ctr.nextSet
		projection.ctr.nextSet++
		active := false
		for i := 0; i < groupCount; i++ {
			if projection.GroupingFlags[candidate*groupCount+i] {
				active = true
				break
			}
		}
		if !active {
			set = candidate
			break
		}
	}
	if set < 0 {
		return vm.CancelResult, nil
	}

	projection.freeExpandOwned(proc)
	output := projection.ctr.expandBuf
	for i := 0; i < groupCount; i++ {
		vec := vector.NewRollupConst(planTypeToType(projection.ProjectList[i].Typ), 1, proc.Mp())
		projection.ctr.expandOwned = append(projection.ctr.expandOwned, vec)
		output.Vecs[i] = vec
	}
	for i := groupCount; i < len(output.Vecs)-2; i++ {
		vec, err := vector.NewConstNullWithAllocation(planTypeToType(projection.ProjectList[i].Typ), 1, nil)
		if err != nil {
			return vm.CancelResult, err
		}
		projection.ctr.expandOwned = append(projection.ctr.expandOwned, vec)
		output.Vecs[i] = vec
	}
	marker, err := vector.NewConstFixed(types.T_bool.ToType(), true, 1, proc.Mp())
	if err != nil {
		return vm.CancelResult, err
	}
	projection.ctr.expandOwned = append(projection.ctr.expandOwned, marker)
	output.Vecs[len(output.Vecs)-2] = marker
	setID, err := vector.NewConstFixed(types.T_int64.ToType(), int64(set), 1, proc.Mp())
	if err != nil {
		return vm.CancelResult, err
	}
	projection.ctr.expandOwned = append(projection.ctr.expandOwned, setID)
	output.Vecs[len(output.Vecs)-1] = setID
	output.SetRowCount(1)
	projection.maxAllocSize = max(projection.maxAllocSize, output.Size())
	return vm.CallResult{Batch: output, Status: vm.ExecNext}, nil
}

func (projection *Projection) emitGroupingSet(proc *process.Process) (vm.CallResult, error) {
	projection.freeExpandOwned(proc)
	set := projection.ctr.nextSet
	groupCount := len(projection.GroupingFlags) / projection.GroupingSetCount
	rowCount := projection.ctr.buf.RowCount()
	output := projection.ctr.expandBuf
	output.ShuffleIDX = projection.ctr.buf.ShuffleIDX

	for i := 0; i < groupCount; i++ {
		if projection.GroupingFlags[set*groupCount+i] {
			output.Vecs[i] = projection.ctr.buf.Vecs[i]
			continue
		}
		typ := planTypeToType(projection.ProjectList[i].Typ)
		vec := vector.NewRollupConst(typ, rowCount, proc.Mp())
		projection.ctr.expandOwned = append(projection.ctr.expandOwned, vec)
		output.Vecs[i] = vec
	}
	for i := groupCount; i < len(output.Vecs)-1; i++ {
		output.Vecs[i] = projection.ctr.buf.Vecs[i]
	}
	setID, err := vector.NewConstFixed(types.T_int64.ToType(), int64(set), rowCount, proc.Mp())
	if err != nil {
		return vm.CancelResult, err
	}
	projection.ctr.expandOwned = append(projection.ctr.expandOwned, setID)
	output.Vecs[len(output.Vecs)-1] = setID
	output.SetRowCount(rowCount)
	projection.maxAllocSize = max(projection.maxAllocSize, output.Size())

	projection.ctr.nextSet++
	if projection.ctr.nextSet == projection.GroupingSetCount {
		projection.ctr.hasInput = false
		projection.ctr.nextSet = 0
	}
	return vm.CallResult{Batch: output, Status: vm.ExecNext}, nil
}

func planTypeToType(typ plan.Type) types.Type {
	return types.NewWithCharset(types.T(typ.Id), typ.Width, typ.Scale, uint8(typ.Charset))
}
