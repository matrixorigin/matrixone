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
	"encoding/binary"
	"fmt"
	"os"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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

	if len(hashBuild.ctr.keyExecutors) == 0 {
		hashBuild.ctr.keyExecutors = make([]colexec.ExpressionExecutor, len(hashBuild.Conditions))
		for i, expr := range hashBuild.Conditions {
			hashBuild.ctr.keyExecutors[i], err = colexec.NewExpressionExecutor(proc, expr)
			if err != nil {
				return err
			}
		}
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
	ap := hashBuild
	ctr := &ap.ctr
	for {
		switch ctr.state {
		case BuildHashMap:
			if err := ctr.build(ap, proc, analyzer); err != nil {
				return result, err
			}
			if ctr.spilled {
				ctr.state = Spill
			} else {
				analyzer.Alloc(ctr.hashmapBuilder.GetSize())
				ctr.state = HandleRuntimeFilter
			}

		case Spill:
			// we have spilled the batches, and the remaining batches are for partition 0.
			// we need to build the hash map for partition 0 now.

			if ap.NeedHashMap {
				needUniqueVec := true
				if ap.RuntimeFilterSpec == nil || ap.RuntimeFilterSpec.Expr == nil {
					needUniqueVec = false
				}
				err := ctr.hashmapBuilder.BuildHashmap(ap.HashOnPK, ap.NeedAllocateSels, needUniqueVec, proc)
				if err != nil {
					return result, err
				}
			}
			if !ap.NeedBatches {
				ctr.hashmapBuilder.Batches.Clean(proc.Mp())
			}
			analyzer.Alloc(ctr.hashmapBuilder.GetSize())
			ctr.state = HandleRuntimeFilter

		case HandleRuntimeFilter:
			if err := ctr.handleRuntimeFilter(ap, proc); err != nil {
				return result, err
			}
			ctr.state = SendJoinMap

		case SendJoinMap:
			var jm *message.JoinMap
			if ctr.hashmapBuilder.InputBatchRowCount > 0 || ctr.spilled {
				jm = message.NewJoinMap(ctr.hashmapBuilder.MultiSels, ctr.hashmapBuilder.IntHashMap, ctr.hashmapBuilder.StrHashMap, ctr.hashmapBuilder.DelRows, ctr.hashmapBuilder.Batches.Buf, proc.Mp())
				jm.SetPushedRuntimeFilterIn(ctr.runtimeFilterIn)
				if ap.NeedBatches {
					jm.SetRowCount(int64(ctr.hashmapBuilder.InputBatchRowCount))
				}
				jm.IncRef(ap.JoinMapRefCnt)
				jm.Spilled = ctr.spilled
				jm.PartitionCnt = ctr.partitionCnt
				jm.BuildOpID = ap.GetOperatorID()
			}
			if ap.JoinMapTag <= 0 {
				panic("wrong joinmap message tag!")
			}
			message.SendMessage(message.JoinMapMsg{JoinMapPtr: jm, Tag: ap.JoinMapTag}, proc.GetMessageBoard())
			ctr.state = SendSucceed

		case SendSucceed:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (ctr *container) build(ap *HashBuild, proc *process.Process, analyzer process.Analyzer) error {
	for {
		result, err := vm.ChildrenCall(ap.GetChildren(0), proc, analyzer)
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
		ctr.memUsed += int64(result.Batch.Size())

		if ap.Spillable && ctr.memUsed > ap.SpillMemoryThreshold {
			ctr.spilled = true
		}
	}

	if ctr.spilled {
		const numPartitions = 8
		ctr.partitionCnt = numPartitions

		partitionedBatches := make([][]*batch.Batch, numPartitions)
		for i := range partitionedBatches {
			partitionedBatches[i] = make([]*batch.Batch, 0)
		}

		// Partition each batch
		for _, bat := range ctr.hashmapBuilder.Batches.Buf {
			rowCount := bat.RowCount()
			hashes := make([]uint64, rowCount)

			for i, _ := range ap.Conditions {
				vec, err := ctr.keyExecutors[i].Eval(proc, []*batch.Batch{bat}, nil)
				if err != nil {
					return err
				}

				// Compute hash values
				hashCol := make([]uint64, rowCount)
				if vec.GetType().IsFixedLen() && vec.GetType().TypeSize() == 8 {
					// fast path for int64/uint64/float64/date/timestamp/decimal64
					cols := vector.MustFixedColNoTypeCheck[int64](vec)
					hashtable.Int64BatchHash(unsafe.Pointer(&cols[0]), &hashCol[0], rowCount)
				} else {
					// generic path
					for i := 0; i < rowCount; i++ {
						bytes := vec.GetBytesAt(i)
						var state [3]uint64
						hashtable.BytesBatchGenHashStatesWithSeed(&bytes, &state, 1, 0)
						hashCol[i] = state[0]
					}
				}

				// Combine hash values
				if i == 0 {
					copy(hashes, hashCol)
				} else {
					for i := range hashes {
						hashes[i] ^= hashCol[i]
					}
				}
			}

			// Distribute rows to partitions using selection vectors
			sels := make([][]int64, numPartitions)
			for i := range sels {
				sels[i] = make([]int64, 0, rowCount/numPartitions+1)
			}

			for row := 0; row < rowCount; row++ {
				partition := hashes[row] % numPartitions
				sels[partition] = append(sels[partition], int64(row))
			}

			// Create new batches for each partition
			for i := range sels {
				if len(sels[i]) > 0 {
					newBat := batch.NewWithSize(len(bat.Vecs))
					for j, vec := range bat.Vecs {
						newBat.Vecs[j] = vector.NewVec(*vec.GetType())
					}
					for col := range bat.Vecs {
						if err := newBat.Vecs[col].Union(bat.Vecs[col], sels[i], proc.Mp()); err != nil {
							return err
						}
					}
					newBat.SetRowCount(len(sels[i]))
					partitionedBatches[i] = append(partitionedBatches[i], newBat)
				}
			}
			bat.Clean(proc.Mp())
		}

		// Spill partitions 1-7 to disk
		for i := 1; i < numPartitions; i++ {
			if len(partitionedBatches[i]) == 0 {
				continue
			}

			// Create a deterministic file name for the partition
			partitionFileName := fmt.Sprintf("hashbuild_spill_%s_%d_%d", proc.QueryId(), ap.GetOperatorID(), i)

			// Create writer using local file system
			writer, err := os.Create(partitionFileName)
			if err != nil {
				return err
			}

			// Serialize and write batches
			// Write count of batches first? No, read until EOF or logic to read all.
			// We write batch by batch.
			for _, bat := range partitionedBatches[i] {
				data, err := bat.MarshalBinary()
				if err != nil {
					writer.Close()
					return err
				}
				// Write size
				sizeBuf := make([]byte, 4)
				binary.LittleEndian.PutUint32(sizeBuf, uint32(len(data)))
				if _, err := writer.Write(sizeBuf); err != nil {
					writer.Close()
					return err
				}
				// Write data
				if _, err = writer.Write(data); err != nil {
					writer.Close()
					return err
				}
				bat.Clean(proc.Mp())
			}
			writer.Close()
		}

		// Keep partition 0 in memory
		ctr.hashmapBuilder.Batches.Buf = partitionedBatches[0]
		ctr.hashmapBuilder.InputBatchRowCount = 0
		for _, bat := range partitionedBatches[0] {
			ctr.hashmapBuilder.InputBatchRowCount += bat.RowCount()
		}

	} else if ap.NeedHashMap {
		needUniqueVec := true
		if ap.RuntimeFilterSpec == nil || ap.RuntimeFilterSpec.Expr == nil {
			needUniqueVec = false
		}
		err := ctr.hashmapBuilder.BuildHashmap(ap.HashOnPK, ap.NeedAllocateSels, needUniqueVec, proc)
		if err != nil {
			return err
		}
		if !ap.NeedBatches {
			ctr.hashmapBuilder.Batches.Clean(proc.Mp())
		}
	}
	return nil
}

func (ctr *container) handleRuntimeFilter(ap *HashBuild, proc *process.Process) error {
	if ap.RuntimeFilterSpec == nil {
		return nil
	}

	var runtimeFilter message.RuntimeFilterMessage
	runtimeFilter.Tag = ap.RuntimeFilterSpec.Tag

	if ap.RuntimeFilterSpec.Expr == nil {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		message.SendRuntimeFilter(runtimeFilter, ap.RuntimeFilterSpec, proc.GetMessageBoard())
		return nil
	} else if ctr.hashmapBuilder.InputBatchRowCount == 0 || len(ctr.hashmapBuilder.UniqueJoinKeys) == 0 || ctr.hashmapBuilder.UniqueJoinKeys[0].Length() == 0 {
		runtimeFilter.Typ = message.RuntimeFilter_DROP
		message.SendRuntimeFilter(runtimeFilter, ap.RuntimeFilterSpec, proc.GetMessageBoard())
		return nil
	}

	hashmapCount := ctr.hashmapBuilder.GetGroupCount()

	inFilterCardLimit := ap.RuntimeFilterSpec.UpperLimit
	//inFilterCardLimit := plan.GetInFilterCardLimit()
	//bloomFilterCardLimit := int64(plan.BloomFilterCardLimit)
	//v, ok = runtime.ProcessLevelRuntime().GetGlobalVariables("runtime_filter_limit_bloom_filter")
	//if ok {
	//	bloomFilterCardLimit = v.(int64)
	//}

	defer func() {
		for i := range ctr.hashmapBuilder.UniqueJoinKeys {
			ctr.hashmapBuilder.UniqueJoinKeys[i].Free(proc.Mp())
		}
		ctr.hashmapBuilder.UniqueJoinKeys = nil
	}()

	if hashmapCount > uint64(inFilterCardLimit) {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		message.SendRuntimeFilter(runtimeFilter, ap.RuntimeFilterSpec, proc.GetMessageBoard())
		return nil
	} else {
		rowCount := ctr.hashmapBuilder.UniqueJoinKeys[0].Length()

		var data []byte
		var err error
		// Composite primary key
		if ap.RuntimeFilterSpec.Expr.GetF() != nil {
			bat := batch.NewWithSize(len(ctr.hashmapBuilder.UniqueJoinKeys))
			bat.SetRowCount(rowCount)
			copy(bat.Vecs, ctr.hashmapBuilder.UniqueJoinKeys)

			vec, free, erg := colexec.GetReadonlyResultFromExpression(proc, ap.RuntimeFilterSpec.Expr, []*batch.Batch{bat})
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
		message.SendRuntimeFilter(runtimeFilter, ap.RuntimeFilterSpec, proc.GetMessageBoard())
		ctr.runtimeFilterIn = true
	}
	return nil
}
