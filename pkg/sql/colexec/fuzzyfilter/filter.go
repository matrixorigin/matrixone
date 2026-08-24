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
	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/runtimefilter"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
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

const opName = "fuzzy_filter"

func (fuzzyFilter *FuzzyFilter) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": fuzzy check duplicate constraint")
}

func (fuzzyFilter *FuzzyFilter) OpType() vm.OpType {
	return vm.FuzzyFilter
}

func (fuzzyFilter *FuzzyFilter) Prepare(proc *process.Process) (err error) {
	if fuzzyFilter.OpAnalyzer == nil {
		fuzzyFilter.OpAnalyzer = process.NewAnalyzer(fuzzyFilter.GetIdx(), fuzzyFilter.IsFirst, fuzzyFilter.IsLast, "fuzzy_filter")
	} else {
		fuzzyFilter.OpAnalyzer.Reset()
	}

	ctr := &fuzzyFilter.ctr
	// Prepare starts a new execution generation. Reset deliberately keeps the
	// previous generation's terminal gate closed so repeated cleanup is
	// idempotent.
	ctr.runtimeFilterDone = false
	if ctr.rbat == nil {
		rowCount := int64(fuzzyFilter.N)
		if rowCount < 1000 {
			rowCount = 1000
		}

		if err := fuzzyFilter.generate(); err != nil {
			return err
		}

		useRoaring := IfCanUseRoaringFilter(types.T(fuzzyFilter.PkTyp.Id))

		if useRoaring {
			ctr.roaringFilter = newRoaringFilter(types.T(fuzzyFilter.PkTyp.Id))
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
	}

	ctr.runtimeFilterUsable = false
	if fuzzyFilter.RuntimeFilterSpec != nil {
		buildExpr := runtimefilter.BuildKeyExpr(
			fuzzyFilter.RuntimeFilterSpec)
		if buildExpr == nil || buildExpr.GetCol() == nil ||
			buildExpr.GetCol().ColPos != 0 {
			if ctr.pass2RuntimeFilter != nil {
				ctr.pass2RuntimeFilter.Free(proc.Mp())
				ctr.pass2RuntimeFilter = nil
			}
			return nil
		}
		pkType := plan.MakeTypeByPlan2Type(fuzzyFilter.PkTyp)
		ctr.runtimeFilterUsable = runtimefilter.ExactKeyEncoding(
			fuzzyFilter.RuntimeFilterSpec,
			pkType,
		) != keycodec.ExactRuntimeFilterUnsupported
	}
	if ctr.runtimeFilterUsable {
		if fuzzyFilter.allocationAccount == nil ||
			fuzzyFilter.runtimeFilterAllocation == nil {
			return mpool.ErrAllocationAccountInvalid
		}
		if ctr.pass2RuntimeFilter == nil {
			ctr.pass2RuntimeFilter, err =
				vector.NewOffHeapVecWithTypeAndAllocation(
					plan.MakeTypeByPlan2Type(fuzzyFilter.PkTyp),
					fuzzyFilter.runtimeFilterAllocation,
				)
			if err != nil {
				return err
			}
		}
	} else if ctr.pass2RuntimeFilter != nil {
		// FuzzyFilter must still execute its uniqueness check, but an
		// unprovable optional runtime filter must not retain a second copy of
		// every build key.
		ctr.pass2RuntimeFilter.Free(proc.Mp())
		ctr.pass2RuntimeFilter = nil
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
func (fuzzyFilter *FuzzyFilter) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := fuzzyFilter.OpAnalyzer

	result := vm.NewCallResult()
	ctr := &fuzzyFilter.ctr
	for {
		switch ctr.state {
		case Build:
			buildIdx := fuzzyFilter.BuildIdx

			input, err := vm.ChildrenCall(fuzzyFilter.GetChildren(buildIdx), proc, analyzer)
			if err != nil {
				fuzzyFilter.finalizeBuildFailure(proc)
				return result, err
			}
			bat := input.Batch

			if bat == nil {
				if fuzzyFilter.ifBuildOnSink() {
					ctr.state = HandleRuntimeFilter
				} else {
					ctr.state = Probe
				}
				continue
			}

			if bat.IsEmpty() {
				continue
			}

			pkCol := bat.GetVector(0)
			if err := fuzzyFilter.appendPassToRuntimeFilter(pkCol, proc); err != nil {
				fuzzyFilter.finalizeBuildFailure(proc)
				return result, err
			}

			err = fuzzyFilter.handleBuild(proc, pkCol)
			if err != nil {
				fuzzyFilter.finalizeBuildFailure(proc)
				return result, err
			}

			continue

		case HandleRuntimeFilter:
			if err := fuzzyFilter.handleRuntimeFilter(proc); err != nil {
				fuzzyFilter.finalizeBuildFailure(proc)
				return result, err
			}
			ctr.state = Probe

		case Probe:
			probeIdx := fuzzyFilter.getProbeIdx()

			input, err := vm.ChildrenCall(fuzzyFilter.GetChildren(probeIdx), proc, analyzer)
			if err != nil {
				fuzzyFilter.finalizeBuildFailure(proc)
				return result, err
			}
			bat := input.Batch

			if bat == nil {
				// fmt.Println("probe cnt = ", arg.probeCnt)
				// this will happen in such case:create unique index from a table that unique col have no data
				if ctr.rbat == nil || ctr.collisionCnt == 0 {
					fuzzyFilter.ensureRuntimeFilterTerminal(proc)
					result.Status = vm.ExecStop
					return result, nil
				}

				// send collisionKeys to output operator to run background SQL
				ctr.rbat.SetRowCount(ctr.rbat.Vecs[0].Length())
				result.Batch = ctr.rbat
				result.Status = vm.ExecStop
				ctr.state = End
				if err := fuzzyFilter.Callback(ctr.rbat); err != nil {
					fuzzyFilter.finalizeBuildFailure(proc)
					return result, err
				} else {
					fuzzyFilter.ensureRuntimeFilterTerminal(proc)
					return result, nil
				}
			}

			if bat.IsEmpty() {
				continue
			}

			pkCol := bat.GetVector(0)

			// arg.probeCnt += pkCol.Length()
			err = fuzzyFilter.handleProbe(proc, pkCol)
			if err != nil {
				fuzzyFilter.finalizeBuildFailure(proc)
				return result, err
			}

			continue
		case End:
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (fuzzyFilter *FuzzyFilter) finalizeBuildFailure(proc *process.Process) {
	if fuzzyFilter.RuntimeFilterSpec == nil ||
		fuzzyFilter.ctr.runtimeFilterDone {
		return
	}
	message.FinalizeRuntimeFilterOnBuildError(
		fuzzyFilter.RuntimeFilterSpec, proc.GetMessageBoard())
	fuzzyFilter.ctr.runtimeFilterDone = true
}

// A valid planner attaches a runtime filter only when the fuzzy build side can
// publish it. Keep malformed/stale plans live too: successful completion
// without a producer phase must fail open rather than strand a scan receiver.
func (fuzzyFilter *FuzzyFilter) ensureRuntimeFilterTerminal(proc *process.Process) {
	fuzzyFilter.finalizeBuildFailure(proc)
}

// =========================================================================
// utils functions

func (fuzzyFilter *FuzzyFilter) handleBuild(proc *process.Process, pkCol *vector.Vector) error {
	buildOnSink := fuzzyFilter.ifBuildOnSink()

	if buildOnSink { // build fuzzy on sink scan
		if fuzzyFilter.IfInsertFromUnique {
			fuzzyFilter.add(pkCol)
		} else {
			// The data source of sink scan cannot ensure whether the data itself is duplicated
			err := fuzzyFilter.testAndAdd(proc, pkCol)
			if err != nil {
				return err
			}
		}
	} else { // build on table scan
		fuzzyFilter.add(pkCol)
	}

	return nil
}

func (fuzzyFilter *FuzzyFilter) handleProbe(proc *process.Process, pkCol *vector.Vector) error {
	buildOnSink := fuzzyFilter.ifBuildOnSink()
	probeOnSink := !buildOnSink

	if probeOnSink {
		if fuzzyFilter.IfInsertFromUnique {
			err := fuzzyFilter.test(proc, pkCol)
			if err != nil {
				return err
			}
		} else {
			err := fuzzyFilter.testAndAdd(proc, pkCol)
			if err != nil {
				return err
			}
		}
	} else { // probe on table scan
		err := fuzzyFilter.test(proc, pkCol)
		if err != nil {
			return err
		}
	}
	return nil
}

func (fuzzyFilter *FuzzyFilter) handleRuntimeFilter(proc *process.Process) error {
	ctr := &fuzzyFilter.ctr

	if fuzzyFilter.RuntimeFilterSpec == nil {
		return nil
	}

	var runtimeFilter message.RuntimeFilterMessage
	runtimeFilter.Tag = fuzzyFilter.RuntimeFilterSpec.Tag

	//                                                 the number of data insert is greater than inFilterCardLimit
	if !ctr.runtimeFilterUsable || ctr.pass2RuntimeFilter == nil {
		fuzzyFilter.sendRuntimeFilterPass(proc)
		return nil
	}

	encoding := runtimefilter.ExactKeyEncoding(
		fuzzyFilter.RuntimeFilterSpec,
		*ctr.pass2RuntimeFilter.GetType(),
	)
	if encoding == keycodec.ExactRuntimeFilterUnsupported {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		message.SendRuntimeFilter(runtimeFilter, fuzzyFilter.RuntimeFilterSpec, proc.GetMessageBoard())
		ctr.runtimeFilterDone = true
		return nil
	}
	if ctr.pass2RuntimeFilter.Length() == 0 {
		runtimeFilter.Typ = message.RuntimeFilter_DROP
		message.SendRuntimeFilter(runtimeFilter, fuzzyFilter.RuntimeFilterSpec, proc.GetMessageBoard())
		ctr.runtimeFilterDone = true
		return nil
	}
	if encoding == keycodec.ExactRuntimeFilterFloatZeroClosed {
		if err := runtimefilter.CloseFloatSignedZero(
			ctr.pass2RuntimeFilter, proc.Mp()); err != nil {
			if fuzzyFilter.fallbackRuntimeFilter(proc, err) {
				return nil
			}
			fuzzyFilter.abandonRuntimeFilter(proc)
			return err
		}
	}
	if ctr.pass2RuntimeFilter.Length() > int(fuzzyFilter.RuntimeFilterSpec.UpperLimit) {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		message.SendRuntimeFilter(runtimeFilter, fuzzyFilter.RuntimeFilterSpec, proc.GetMessageBoard())
		ctr.runtimeFilterDone = true
		return nil
	}

	// InplaceSort reorders data but NOT the null bitmap.
	// Reset bitmap before sort to avoid corruption.
	ctr.pass2RuntimeFilter.GetNulls().Reset()
	ctr.pass2RuntimeFilter.InplaceSort()
	data, release, err := runtimefilter.MarshalExactFilterVector(
		ctr.pass2RuntimeFilter,
		proc.Mp(),
		fuzzyFilter.allocationAccount,
		mpool.AllocationOwnerFuzzyFilter,
		fuzzyFilterAllocationSiteRuntimeFilterPayload,
	)
	if err != nil {
		if fuzzyFilter.fallbackRuntimeFilter(proc, err) {
			return nil
		}
		fuzzyFilter.abandonRuntimeFilter(proc)
		return err
	}

	runtimeFilter.Typ = message.RuntimeFilter_IN
	runtimeFilter.Card = int32(ctr.pass2RuntimeFilter.Length())
	runtimeFilter.Data = data
	runtimeFilter.SetMemoryRelease(release)
	message.SendRuntimeFilter(runtimeFilter, fuzzyFilter.RuntimeFilterSpec, proc.GetMessageBoard())
	ctr.runtimeFilterDone = true
	return nil
}

func (fuzzyFilter *FuzzyFilter) sendRuntimeFilterPass(
	proc *process.Process,
) {
	if fuzzyFilter.RuntimeFilterSpec == nil ||
		fuzzyFilter.ctr.runtimeFilterDone {
		return
	}
	message.SendRuntimeFilter(
		message.RuntimeFilterMessage{
			Tag: fuzzyFilter.RuntimeFilterSpec.Tag,
			Typ: message.RuntimeFilter_PASS,
		},
		fuzzyFilter.RuntimeFilterSpec,
		proc.GetMessageBoard(),
	)
	fuzzyFilter.ctr.runtimeFilterDone = true
}

func (fuzzyFilter *FuzzyFilter) fallbackRuntimeFilter(
	proc *process.Process,
	err error,
) bool {
	kind := runtimefilter.ClassifyOptionalFallback(err)
	if kind == runtimefilter.OptionalFallbackNone {
		return false
	}
	if fuzzyFilter.OpAnalyzer != nil {
		stats := fuzzyFilter.OpAnalyzer.GetOpStats()
		if kind == runtimefilter.OptionalFallbackBudgetAdmission {
			stats.AddExtraStat(
				"FuzzyFilterRuntimeFilterBudgetFallbacks", 1)
		} else {
			stats.AddExtraStat(
				"FuzzyFilterRuntimeFilterAllocationFallbacks", 1)
		}
	}
	fuzzyFilter.abandonRuntimeFilter(proc)
	fuzzyFilter.sendRuntimeFilterPass(proc)
	return true
}

func (fuzzyFilter *FuzzyFilter) abandonRuntimeFilter(
	proc *process.Process,
) {
	ctr := &fuzzyFilter.ctr
	ctr.runtimeFilterUsable = false
	if ctr.pass2RuntimeFilter != nil {
		ctr.pass2RuntimeFilter.Free(proc.Mp())
		ctr.pass2RuntimeFilter = nil
	}
}

func (fuzzyFilter *FuzzyFilter) appendPassToRuntimeFilter(
	v *vector.Vector, proc *process.Process,
) (err error) {
	ctr := &fuzzyFilter.ctr
	if ctr.runtimeFilterUsable &&
		ctr.pass2RuntimeFilter != nil &&
		fuzzyFilter.RuntimeFilterSpec != nil {
		if runtimefilter.ExactKeyEncoding(
			fuzzyFilter.RuntimeFilterSpec,
			*v.GetType(),
		) == keycodec.ExactRuntimeFilterUnsupported {
			fuzzyFilter.abandonRuntimeFilter(proc)
			return nil
		}
		el := ctr.pass2RuntimeFilter.Length()
		al := v.Length()

		if int64(el)+int64(al) <= int64(fuzzyFilter.RuntimeFilterSpec.UpperLimit) {
			if err = ctr.pass2RuntimeFilter.UnionBatch(
				v, 0, al, nil, proc.Mp(),
			); err != nil {
				err = runtimefilter.MarkOptionalAllocationError(err)
				if fuzzyFilter.fallbackRuntimeFilter(proc, err) {
					return nil
				}
				fuzzyFilter.abandonRuntimeFilter(proc)
				return err
			}
		} else {
			fuzzyFilter.abandonRuntimeFilter(proc)
		}
	}
	return
}

// appendCollisionKey will append collision key into rbat
func (fuzzyFilter *FuzzyFilter) appendCollisionKey(proc *process.Process, idx int, pkCol *vector.Vector) {
	ctr := &fuzzyFilter.ctr
	ctr.rbat.GetVector(0).UnionOne(pkCol, int64(idx), proc.GetMPool())
	ctr.collisionCnt++
}

// rbat will contain the keys that have hash collisions
func (fuzzyFilter *FuzzyFilter) generate() error {
	ctr := &fuzzyFilter.ctr
	rbat := batch.NewWithSize(1)
	rbat.SetVector(0, vector.NewVec(plan.MakeTypeByPlan2Type(fuzzyFilter.PkTyp)))
	ctr.rbat = rbat
	return nil
}
