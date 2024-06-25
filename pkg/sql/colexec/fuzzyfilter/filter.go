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

	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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

An intuitive way to understand this Join, please refer to the following code snippet:

	Fuzzy filter:
		<- Build on Sink scan
			Test and add
		<- Probe on Table scan
			Test

Sink scan needs Test_and_Add because we can't be sure if the data passed in by the sink scan itself is duplicated (whereas table scan data is certainly not duplicated).


if the final bloom filter claim that
    case 1: have no duplicate keys
        pass duplicate constraint directly
    case 2: Not sure if there are duplicate keys because of hash collision
        start a background SQL to double check


opt:
1. backgroud SQL may slow, so some optimizations could be applied
	Using statistical information, when the data to be loaded is larger, the allowed false positive probability is lower,
		avoiding too much content that needs to be checked.
    manually check whether collision keys duplicate or not,
        if duplicate, then return error timely
	For uint[8|16|32], or int[8|16|32], use bitmap directly to avoid false positives and hashing

2. there is a corner case that no need to run background SQL
    on duplicate key update

3. see the comment of func arg.Call
*/

const argName = "fuzzy_filter"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": fuzzy check duplicate constraint")
}

func (arg *Argument) Prepare(proc *process.Process) (err error) {
	ctr := new(container)
	arg.ctr = ctr
	ctr.InitReceiver(proc, false)
	rowCount := int64(arg.N)
	if rowCount < 1000 {
		rowCount = 1000
	}

	if err := arg.generate(proc); err != nil {
		return err
	}

	useRoaring := IfCanUseRoaringFilter(types.T(arg.PkTyp.Id))

	if useRoaring {
		ctr.roaringFilter = newroaringFilter(types.T(arg.PkTyp.Id))
	} else {
		//@see https://hur.st/bloomfilter/
		var probability float64
		if rowCount < 10_0001 {
			probability = 0.00001
		} else if rowCount < 100_0001 {
			probability = 0.000003
		} else if rowCount < 1000_0001 {
			probability = 0.000001
		} else if rowCount < 1_0000_0001 {
			probability = 0.0000005
		} else if rowCount < 10_0000_0001 {
			probability = 0.0000002
		} else {
			probability = 0.0000001
		}
		ctr.bloomFilter = bloomfilter.New(rowCount, probability)
	}

	return nil
}

/*
opt3 : As mentioned before, you should think of fuzzy as a special kind of join, which also has a Build phase and a Probe phase.

The previous pseudo-code has no problem with correctness, but the memory overhead in some scenarios can be significant,
especially when the sink scan has much LARGER data than the table scan.
Therefore, build stage also needs to be built on smaller children.

# Flow of optimized pseudo-code
if Stats(Table Scan) > Stats(Sink Scan)

	Build on Sink scan
		Test and Add
		-> can be optimized to Add if the sinkScan data can guarantee uniqueness
	Probe on Table scan
		Test

else

	Build on Table scan
		Add
	Probe on Sink scan
		Test and Add
		-> can be optimized to Test if the sinkScan data can guarantee uniqueness
*/
func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	result := vm.NewCallResult()
	ctr := arg.ctr
	for {
		switch ctr.state {
		case Build:

			buildIdx := arg.BuildIdx

			msg := ctr.ReceiveFromSingleReg(buildIdx, anal)
			if msg.Err != nil {
				return result, msg.Err
			}

			bat := msg.Batch
			if bat == nil {
				if arg.ifBuildOnSink() {
					ctr.state = HandleRuntimeFilter
				} else {
					ctr.state = Probe
				}
				continue
			}

			if bat.IsEmpty() {
				proc.PutBatch(bat)
				continue
			}

			pkCol := bat.GetVector(0)
			arg.appendPassToRuntimeFilter(pkCol, proc)

			err := arg.handleBuild(proc, pkCol)
			if err != nil {
				proc.PutBatch(bat)
				return result, err
			}

			proc.PutBatch(bat)
			continue

		case HandleRuntimeFilter:
			if err := arg.handleRuntimeFilter(proc); err != nil {
				return result, err
			}
			ctr.state = Probe

		case Probe:

			probeIdx := arg.getProbeIdx()

			msg := ctr.ReceiveFromSingleReg(probeIdx, anal)
			if msg.Err != nil {
				return result, msg.Err
			}

			bat := msg.Batch
			if bat == nil {
				// fmt.Println("probe cnt = ", arg.probeCnt)
				// this will happen in such case:create unique index from a table that unique col have no data
				if ctr.rbat == nil || ctr.collisionCnt == 0 {
					result.Status = vm.ExecStop
					return result, nil
				}

				// send collisionKeys to output operator to run background SQL
				ctr.rbat.SetRowCount(ctr.rbat.Vecs[0].Length())
				result.Batch = ctr.rbat
				result.Status = vm.ExecStop
				ctr.state = End
				if err := arg.Callback(ctr.rbat); err != nil {
					return result, err
				} else {
					return result, nil
				}
			}

			if bat.IsEmpty() {
				proc.PutBatch(bat)
				continue
			}

			pkCol := bat.GetVector(0)

			// arg.probeCnt += pkCol.Length()
			err := arg.handleProbe(proc, pkCol)
			if err != nil {
				proc.PutBatch(bat)
				return result, err
			}

			proc.PutBatch(bat)
			continue
		case End:
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

// =========================================================================
// utils functions

func (arg *Argument) handleBuild(proc *process.Process, pkCol *vector.Vector) error {
	buildOnSink := arg.ifBuildOnSink()

	if buildOnSink { // build fuzzy on sink scan
		if arg.IfInsertFromUnique {
			arg.add(pkCol)
		} else {
			// The data source of sink scan cannot ensure whether the data itself is duplicated
			err := arg.testAndAdd(proc, pkCol)
			if err != nil {
				return err
			}
		}
	} else { // build on table scan
		arg.add(pkCol)
	}

	return nil
}

func (arg *Argument) handleProbe(proc *process.Process, pkCol *vector.Vector) error {
	buildOnSink := arg.ifBuildOnSink()
	probeOnSink := !buildOnSink

	if probeOnSink {
		if arg.IfInsertFromUnique {
			err := arg.test(proc, pkCol)
			if err != nil {
				return err
			}
		} else {
			err := arg.testAndAdd(proc, pkCol)
			if err != nil {
				return err
			}
		}
	} else { // probe on table scan
		err := arg.test(proc, pkCol)
		if err != nil {
			return err
		}
	}
	return nil
}

func (arg *Argument) handleRuntimeFilter(proc *process.Process) error {
	ctr := arg.ctr

	if arg.RuntimeFilterSpec == nil {
		return nil
	}

	var runtimeFilter process.RuntimeFilterMessage
	runtimeFilter.Tag = arg.RuntimeFilterSpec.Tag

	//                                                 the number of data insert is greater than inFilterCardLimit
	if arg.RuntimeFilterSpec.Expr == nil || ctr.pass2RuntimeFilter == nil {
		runtimeFilter.Typ = process.RuntimeFilter_PASS
		proc.SendRuntimeFilter(runtimeFilter, arg.RuntimeFilterSpec)
		return nil
	}

	//bloomFilterCardLimit := int64(plan.BloomFilterCardLimit)
	//v, ok = runtime.ProcessLevelRuntime().GetGlobalVariables("runtime_filter_limit_bloom_filter")
	//if ok {
	//	bloomFilterCardLimit = v.(int64)
	//}

	ctr.pass2RuntimeFilter.InplaceSort()
	data, err := ctr.pass2RuntimeFilter.MarshalBinary()
	if err != nil {
		return err
	}

	runtimeFilter.Typ = process.RuntimeFilter_IN
	runtimeFilter.Data = data
	proc.SendRuntimeFilter(runtimeFilter, arg.RuntimeFilterSpec)
	return nil
}

func (arg *Argument) appendPassToRuntimeFilter(v *vector.Vector, proc *process.Process) {
	ctr := arg.ctr
	if ctr.pass2RuntimeFilter != nil && arg.RuntimeFilterSpec != nil {
		el := ctr.pass2RuntimeFilter.Length()
		al := v.Length()

		if int64(el)+int64(al) <= int64(arg.RuntimeFilterSpec.UpperLimit) {
			ctr.pass2RuntimeFilter.UnionMulti(v, 0, al, proc.Mp())
		} else {
			proc.PutVector(ctr.pass2RuntimeFilter)
			ctr.pass2RuntimeFilter = nil
		}
	}
}

// appendCollisionKey will append collision key into rbat
func (arg *Argument) appendCollisionKey(proc *process.Process, idx int, pkCol *vector.Vector) {
	ctr := arg.ctr
	ctr.rbat.GetVector(0).UnionOne(pkCol, int64(idx), proc.GetMPool())
	ctr.collisionCnt++
}

// rbat will contain the keys that have hash collisions
func (arg *Argument) generate(proc *process.Process) error {
	ctr := arg.ctr
	rbat := batch.NewWithSize(1)
	rbat.SetVector(0, proc.GetVector(plan.MakeTypeByPlan2Type(arg.PkTyp)))
	ctr.pass2RuntimeFilter = proc.GetVector(plan.MakeTypeByPlan2Type(arg.PkTyp))
	ctr.rbat = rbat
	return nil
}
