// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package fuzzyfilter

import (
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const maxCheckDupCount = 2000

/*
This operator is used to implement a way to ensure primary keys/unique keys are not duplicate in `INSERT` and `LOAD` statements,
 You can think of it as a special type of join, but it saves more memory and is generally faster.

the BIG idea is to store
    pk columns to be loaded
    pk columns already exist
both in a bitmap-like data structure, let's say bloom filter below

if the final bloom filter claim that
    case 1: have no duplicate keys
        pass duplicate constraint directly
    case 2: Not sure if there are duplicate keys because of hash collision
        start a background SQL to double check

Note:
1. backgroud SQL may slow, so some optimizations could be applied
	Using statistical information, when the data to be loaded is larger, the allowed false positive probability is lower,
		avoiding too much content that needs to be checked.
    manually check whether collision keys duplicate or not,
        if duplicate, then return error timely
	For uint[8|16|32], or int[8|16|32], use bitmap directly to avoid false positives and hashing

2. there is a corner case that no need to run background SQL
    on duplicate key update
*/

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(" fuzzy check duplicate constraint")
}

func (arg *Argument) Prepare(proc *process.Process) (err error) {
	arg.InitReceiver(proc, false)
	rowCount := int64(arg.N)
	if rowCount < 100000 {
		rowCount = 100000
	}

	inFilterCardLimit := int64(plan.InFilterCardLimit)
	v, ok := runtime.ProcessLevelRuntime().GetGlobalVariables("runtime_filter_limit_in")
	if ok {
		inFilterCardLimit = v.(int64)
	}
	arg.inFilterCardLimit = inFilterCardLimit

	if err := arg.generate(proc); err != nil {
		return err
	}

	useRoaring := IfCanUseRoaringFilter(types.T(arg.PkTyp.Id))

	if useRoaring {
		arg.roaringFilter = newroaringFilter(types.T(arg.PkTyp.Id))
	} else {
		//@see https://hur.st/bloomfilter/
		var probability float64
		if rowCount < 100001 {
			probability = 0.00001
		} else if rowCount < 1000001 {
			probability = 0.000003
		} else if rowCount < 10000001 {
			probability = 0.000001
		} else if rowCount < 100000001 {
			probability = 0.0000005
		} else if rowCount < 1000000001 {
			probability = 0.0000002
		} else {
			probability = 0.0000001
		}
		arg.bloomFilter = bloomfilter.New(rowCount, probability)
	}

	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	anal := proc.GetAnalyze(arg.info.Idx, arg.info.ParallelIdx, arg.info.ParallelMajor)
	anal.Start()
	defer anal.Stop()

	if arg.roaringFilter != nil {
		return arg.filterByRoaring(proc, anal)
	} else {
		return arg.filterByBloom(proc, anal)
	}
}

func (arg *Argument) filterByBloom(proc *process.Process, anal process.Analyze) (vm.CallResult, error) {
	result := vm.NewCallResult()
	for {
		switch arg.state {
		case Build:

			bat, _, err := arg.ReceiveFromSingleReg(1, anal)
			if err != nil {
				return result, err
			}

			if bat == nil {
				arg.state = HandleRuntimeFilter
				continue
			}

			if bat.IsEmpty() {
				proc.PutBatch(bat)
				continue
			}

			pkCol := bat.GetVector(0)
			arg.appendPassToRuntimeFilter(pkCol, proc)
			arg.bloomFilter.TestAndAdd(pkCol, func(exist bool, i int) {
				if exist {
					if arg.collisionCnt < maxCheckDupCount {
						arg.appendCollisionKey(proc, i, bat)
						return
					}
					logutil.Warnf("too many collision for fuzzy filter")
				}
			})

			proc.PutBatch(bat)
			continue

		case HandleRuntimeFilter:
			if err := arg.handleRuntimeFilter(proc); err != nil {
				return result, err
			}

		case Probe:

			bat, _, err := arg.ReceiveFromSingleReg(0, anal)
			if err != nil {
				return result, err
			}

			if bat == nil {
				// fmt.Println("probe cnt = ", arg.probeCnt)
				// this will happen in such case:create unique index from a table that unique col have no data
				if arg.rbat == nil || arg.collisionCnt == 0 {
					result.Status = vm.ExecStop
					return result, nil
				}

				// send collisionKeys to output operator to run background SQL
				arg.rbat.SetRowCount(arg.rbat.Vecs[0].Length())
				result.Batch = arg.rbat
				result.Status = vm.ExecStop
				arg.state = End
				return result, nil
			}

			if bat.IsEmpty() {
				proc.PutBatch(bat)
				continue
			}

			pkCol := bat.GetVector(0)

			// arg.probeCnt += pkCol.Length()

			arg.bloomFilter.Test(pkCol, func(exist bool, i int) {
				if exist {
					if arg.collisionCnt < maxCheckDupCount {
						arg.appendCollisionKey(proc, i, bat)
					}
				}
			})
			proc.PutBatch(bat)
			continue
		case End:
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (arg *Argument) filterByRoaring(proc *process.Process, anal process.Analyze) (vm.CallResult, error) {
	result := vm.NewCallResult()
	for {
		switch arg.state {
		case Build:

			bat, _, err := arg.ReceiveFromSingleReg(1, anal)
			if err != nil {
				return result, err
			}

			if bat == nil {
				arg.state = HandleRuntimeFilter
				continue
			}

			if bat.IsEmpty() {
				proc.PutBatch(bat)
				continue
			}

			pkCol := bat.GetVector(0)
			arg.appendPassToRuntimeFilter(pkCol, proc)

			idx, dupVal := arg.roaringFilter.testAndAddFunc(arg.roaringFilter, pkCol)
			proc.PutBatch(bat)

			if idx == -1 {
				continue
			} else {
				return result, moerr.NewDuplicateEntry(proc.Ctx, valueToString(dupVal), arg.PkName)
			}

		case HandleRuntimeFilter:
			if err := arg.handleRuntimeFilter(proc); err != nil {
				return result, err
			}

		case Probe:

			bat, _, err := arg.ReceiveFromSingleReg(0, anal)
			if err != nil {
				return result, err
			}

			if bat == nil {
				// fmt.Println("probe cnt = ", arg.probeCnt)
				result.Batch = arg.rbat
				result.Status = vm.ExecStop
				arg.state = End
				return result, nil
			}

			if bat.IsEmpty() {
				proc.PutBatch(bat)
				continue
			}

			pkCol := bat.GetVector(0)

			// arg.probeCnt += pkCol.Length()

			idx, dupVal := arg.roaringFilter.testFunc(arg.roaringFilter, pkCol)
			proc.PutBatch(bat)

			if idx == -1 {
				continue
			} else {
				return result, moerr.NewDuplicateEntry(proc.Ctx, valueToString(dupVal), arg.PkName)
			}
		case End:
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

// =========================================================================
// utils functions

func (arg *Argument) appendPassToRuntimeFilter(v *vector.Vector, proc *process.Process) {
	if arg.pass2RuntimeFilter != nil {
		el := arg.pass2RuntimeFilter.Length()
		al := v.Length()

		if int64(el)+int64(al) <= arg.inFilterCardLimit {
			arg.pass2RuntimeFilter.UnionMulti(v, 0, al, proc.Mp())
		} else {
			proc.PutVector(arg.pass2RuntimeFilter)
			arg.pass2RuntimeFilter = nil
		}
	}
}

// appendCollisionKey will append collision key into rbat
func (arg *Argument) appendCollisionKey(proc *process.Process, idx int, bat *batch.Batch) {
	pkCol := bat.GetVector(0)
	arg.rbat.GetVector(0).UnionOne(pkCol, int64(idx), proc.GetMPool())
	arg.collisionCnt++
}

// rbat will contain the keys that have hash collisions
func (arg *Argument) generate(proc *process.Process) error {
	rbat := batch.NewWithSize(1)
	rbat.SetVector(0, proc.GetVector(plan.MakeTypeByPlan2Type(arg.PkTyp)))
	arg.pass2RuntimeFilter = proc.GetVector(plan.MakeTypeByPlan2Type(arg.PkTyp))
	arg.rbat = rbat
	return nil
}

func (arg *Argument) handleRuntimeFilter(proc *process.Process) error {
	ctr := arg

	if len(arg.RuntimeFilterSenders) == 0 {
		ctr.state = Probe
		return nil
	}

	var runtimeFilter *pipeline.RuntimeFilter
	//                                                 the number of data insert is greater than inFilterCardLimit
	if arg.RuntimeFilterSenders[0].Spec.Expr == nil || arg.pass2RuntimeFilter == nil {
		runtimeFilter = &pipeline.RuntimeFilter{
			Typ: pipeline.RuntimeFilter_PASS,
		}
	}

	if runtimeFilter != nil {
		sendFilter(arg, proc, runtimeFilter)
	}

	//bloomFilterCardLimit := int64(plan.BloomFilterCardLimit)
	//v, ok = runtime.ProcessLevelRuntime().GetGlobalVariables("runtime_filter_limit_bloom_filter")
	//if ok {
	//	bloomFilterCardLimit = v.(int64)
	//}

	colexec.SortInFilter(arg.pass2RuntimeFilter)
	data, err := arg.pass2RuntimeFilter.MarshalBinary()
	if err != nil {
		return err
	}

	runtimeFilter = &pipeline.RuntimeFilter{
		Typ:  pipeline.RuntimeFilter_IN,
		Data: data,
	}

	sendFilter(arg, proc, runtimeFilter)

	return nil

}

func sendFilter(arg *Argument, proc *process.Process, runtimeFilter *pipeline.RuntimeFilter) {
	anal := proc.GetAnalyze(arg.info.Idx, arg.info.ParallelIdx, arg.info.ParallelMajor)
	start := time.Now()
	select {
	case <-proc.Ctx.Done():
		arg.state = End

	case arg.RuntimeFilterSenders[0].Chan <- runtimeFilter:
		arg.state = Probe
	}
	anal.WaitStop(start)

}
