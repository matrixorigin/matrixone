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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "hash_build"

func (hashBuild *HashBuild) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": hash build ")
}

func (hashBuild *HashBuild) OpType() vm.OpType {
	return vm.HashBuild
}

func (hashBuild *HashBuild) Prepare(proc *process.Process) (err error) {
	if hashBuild.OpAnalyzer == nil {
		hashBuild.OpAnalyzer = process.NewAnalyzer(hashBuild.GetIdx(), hashBuild.IsFirst, hashBuild.IsLast, "hash build")
	} else {
		hashBuild.OpAnalyzer.Reset()
	}

	if !hashBuild.NeedHashMap {
		return nil
	}

	if hashBuild.IsShuffle && hashBuild.RuntimeFilterSpec == nil {
		return moerr.NewInternalError(proc.Ctx, "shuffle hash build must have runtime filter")
	}

	hashBuild.ctr.hashmapBuilder.IsDedup = hashBuild.IsDedup
	hashBuild.ctr.hashmapBuilder.OnDuplicateAction = hashBuild.OnDuplicateAction
	hashBuild.ctr.hashmapBuilder.DedupColName = hashBuild.DedupColName
	hashBuild.ctr.hashmapBuilder.DedupColTypes = hashBuild.DedupColTypes

	return hashBuild.ctr.hashmapBuilder.Prepare(hashBuild.Conditions, hashBuild.DelColIdx, proc)
}

func (hashBuild *HashBuild) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := hashBuild.OpAnalyzer
	result := vm.NewCallResult()
	ctr := &hashBuild.ctr
	for {
		switch ctr.state {
		case BuildHashMap:
			if err := ctr.build(hashBuild, proc, analyzer); err != nil {
				return result, err
			}

			ctr.state = HandleRuntimeFilter

		case HandleRuntimeFilter:
			if err := ctr.handleRuntimeFilter(hashBuild, proc); err != nil {
				return result, err
			}

			ctr.state = SendJoinMap

		case SendJoinMap:
			if hashBuild.JoinMapTag <= 0 {
				return result, moerr.NewInternalError(proc.Ctx, "wrong joinmap message tag!")
			}

			var jm *message.JoinMap
			if ctr.hashmapBuilder.InputBatchRowCount > 0 {
				jm = message.NewJoinMap(ctr.hashmapBuilder.MultiSels, ctr.hashmapBuilder.IntHashMap, ctr.hashmapBuilder.StrHashMap, ctr.hashmapBuilder.DelRows, ctr.hashmapBuilder.Batches.Buf, proc.Mp())
				jm.SetPushedRuntimeFilterIn(ctr.runtimeFilterIn)
				if hashBuild.NeedBatches {
					jm.SetRowCount(int64(ctr.hashmapBuilder.InputBatchRowCount))
				}
				jm.IncRef(hashBuild.JoinMapRefCnt)
			}

			message.SendMessage(message.JoinMapMsg{
				JoinMapPtr: jm,
				IsShuffle:  hashBuild.IsShuffle,
				ShuffleIdx: hashBuild.ShuffleIdx,
				Tag:        hashBuild.JoinMapTag,
			}, proc.GetMessageBoard())

			ctr.state = SendSucceed

		case SendSucceed:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (ctr *container) collectBuildBatches(hashBuild *HashBuild, proc *process.Process, analyzer process.Analyzer) error {
	for {
		result, err := vm.ChildrenCall(hashBuild.GetChildren(0), proc, analyzer)
		if err != nil {
			return err
		}
		if result.Batch == nil {
			break
		}
		if result.Batch.IsEmpty() {
			continue
		}

		analyzer.Alloc(int64(result.Batch.Size()))
		ctr.hashmapBuilder.InputBatchRowCount += result.Batch.RowCount()
		err = ctr.hashmapBuilder.Batches.CopyIntoBatches(result.Batch, proc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) build(hashBuild *HashBuild, proc *process.Process, analyzer process.Analyzer) error {
	err := ctr.collectBuildBatches(hashBuild, proc, analyzer)
	if err != nil {
		return err
	}

	if hashBuild.NeedHashMap {
		needUniqueVec := true
		if hashBuild.IsShuffle || hashBuild.RuntimeFilterSpec == nil || hashBuild.RuntimeFilterSpec.Expr == nil {
			needUniqueVec = false
		}

		err = ctr.hashmapBuilder.BuildHashmap(hashBuild.HashOnPK, hashBuild.NeedAllocateSels, needUniqueVec, proc)
		if err != nil {
			return err
		}
	}

	if !hashBuild.NeedBatches {
		// if do not need merged batch, free it now to save memory
		ctr.hashmapBuilder.Batches.Clean(proc.Mp())
	}

	analyzer.Alloc(ctr.hashmapBuilder.GetSize())
	return nil
}

// calculateBloomFilterProbability calculates the false positive rate for bloom filter
// based on row count. Reference fuzzyfilter experience, choose different false positive rates
// based on row count to balance memory usage and filtering accuracy.
func calculateBloomFilterProbability(rowCount int) float64 {
	switch {
	case rowCount < 10_0001:
		return 0.00001
	case rowCount < 100_0001:
		return 0.000003
	case rowCount < 1000_0001:
		return 0.000001
	case rowCount < 1_0000_0001:
		return 0.0000005
	case rowCount < 10_0000_0001:
		return 0.0000002
	default:
		return 0.0000001
	}
}

func (ctr *container) handleRuntimeFilter(hashBuild *HashBuild, proc *process.Process) error {
	if hashBuild.IsShuffle {
		//only support runtime filter pass for now in shuffle join
		var runtimeFilter message.RuntimeFilterMessage
		runtimeFilter.Tag = hashBuild.RuntimeFilterSpec.Tag
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		message.SendRuntimeFilter(runtimeFilter, hashBuild.RuntimeFilterSpec, proc.GetMessageBoard())
		return nil
	}

	if hashBuild.RuntimeFilterSpec == nil {
		return nil
	}

	var runtimeFilter message.RuntimeFilterMessage
	runtimeFilter.Tag = hashBuild.RuntimeFilterSpec.Tag

	spec := hashBuild.RuntimeFilterSpec

	// use bloom filter runtime filter when requested
	if spec.UseBloomFilter {
		// currently only support single-column key for bloom runtime filter
		// composite key still uses original IN / PASS logic
		if spec.Expr != nil && spec.Expr.GetF() != nil {
			runtimeFilter.Typ = message.RuntimeFilter_PASS
			message.SendRuntimeFilter(runtimeFilter, spec, proc.GetMessageBoard())
			return nil
		}

		// No data, directly DROP
		if ctr.hashmapBuilder.InputBatchRowCount == 0 ||
			len(ctr.hashmapBuilder.UniqueJoinKeys) == 0 ||
			ctr.hashmapBuilder.UniqueJoinKeys[0].Length() == 0 {
			runtimeFilter.Typ = message.RuntimeFilter_DROP
			message.SendRuntimeFilter(runtimeFilter, spec, proc.GetMessageBoard())
			return nil
		}

		keyVec := ctr.hashmapBuilder.UniqueJoinKeys[0]
		rowCount := keyVec.Length()

		// Send UniqueJoinKeys instead of Bloomfilter
		data, err := keyVec.MarshalBinary()
		if err != nil {
			return err
		}

		runtimeFilter.Typ = message.RuntimeFilter_BLOOMFILTER
		runtimeFilter.Card = int32(rowCount)
		runtimeFilter.Data = data
		message.SendRuntimeFilter(runtimeFilter, spec, proc.GetMessageBoard())

		// UniqueJoinKeys will be released uniformly in defer
		return nil
	}

	if spec.Expr == nil {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		message.SendRuntimeFilter(runtimeFilter, spec, proc.GetMessageBoard())
		return nil
	} else if ctr.hashmapBuilder.InputBatchRowCount == 0 || len(ctr.hashmapBuilder.UniqueJoinKeys) == 0 || ctr.hashmapBuilder.UniqueJoinKeys[0].Length() == 0 {
		runtimeFilter.Typ = message.RuntimeFilter_DROP
		message.SendRuntimeFilter(runtimeFilter, spec, proc.GetMessageBoard())
		return nil
	}

	hashmapCount := ctr.hashmapBuilder.GetGroupCount()
	inFilterCardLimit := spec.UpperLimit

	defer func() {
		for i := range ctr.hashmapBuilder.UniqueJoinKeys {
			ctr.hashmapBuilder.UniqueJoinKeys[i].Free(proc.Mp())
		}
		ctr.hashmapBuilder.UniqueJoinKeys = nil
	}()

	if hashmapCount > uint64(inFilterCardLimit) {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		message.SendRuntimeFilter(runtimeFilter, spec, proc.GetMessageBoard())
		return nil
	} else {
		rowCount := ctr.hashmapBuilder.UniqueJoinKeys[0].Length()

		var data []byte
		var err error
		// Composite primary key
		if spec.Expr.GetF() != nil {
			bat := batch.NewWithSize(len(ctr.hashmapBuilder.UniqueJoinKeys))
			bat.SetRowCount(rowCount)
			copy(bat.Vecs, ctr.hashmapBuilder.UniqueJoinKeys)

			vec, free, erg := colexec.GetReadonlyResultFromExpression(proc, spec.Expr, []*batch.Batch{bat})
			if erg != nil {
				return erg
			}
			vec.InplaceSort()
			data, err = vec.MarshalBinary()
			free()
		} else {
			ctr.hashmapBuilder.UniqueJoinKeys[0].InplaceSort()
			data, err = ctr.hashmapBuilder.UniqueJoinKeys[0].MarshalBinary()
		}

		if err != nil {
			return err
		}

		runtimeFilter.Typ = message.RuntimeFilter_IN
		runtimeFilter.Card = int32(rowCount)
		runtimeFilter.Data = data
		message.SendRuntimeFilter(runtimeFilter, spec, proc.GetMessageBoard())
		ctr.runtimeFilterIn = true
	}
	return nil
}
