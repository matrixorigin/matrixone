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

package join

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "join"

func (innerJoin *InnerJoin) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": inner join ")
}

func (innerJoin *InnerJoin) OpType() vm.OpType {
	return vm.Join
}

func (innerJoin *InnerJoin) Prepare(proc *process.Process) (err error) {

	if innerJoin.OpAnalyzer == nil {
		innerJoin.OpAnalyzer = process.NewAnalyzer(innerJoin.GetIdx(), innerJoin.IsFirst, innerJoin.IsLast, "innerJoin")
	} else {
		innerJoin.OpAnalyzer.Reset()
	}
	if len(innerJoin.ctr.joinBats) == 0 {
		innerJoin.ctr.joinBats = make([]*batch.Batch, 2)
	}
	if len(innerJoin.ctr.vecs) == 0 {
		innerJoin.ctr.vecs = make([]*vector.Vector, len(innerJoin.Conditions[0]))
		innerJoin.ctr.executor = make([]colexec.ExpressionExecutor, len(innerJoin.Conditions[0]))
		for i := range innerJoin.ctr.executor {
			innerJoin.ctr.executor[i], err = colexec.NewExpressionExecutor(proc, innerJoin.Conditions[0][i])
			if err != nil {
				return err
			}
		}
		if innerJoin.Cond != nil {
			innerJoin.ctr.expr, err = colexec.NewExpressionExecutor(proc, innerJoin.Cond)
			if err != nil {
				return err
			}
		}
	}
	return err
}

func (innerJoin *InnerJoin) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := innerJoin.OpAnalyzer
	ctr := &innerJoin.ctr
	input := vm.NewCallResult()
	result := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			err = innerJoin.build(analyzer, proc)
			if err != nil {
				return result, err
			}

			if ctr.mp == nil && !innerJoin.IsShuffle {
				ctr.state = End
			} else {
				ctr.state = Probe
			}
		case Probe:
			if innerJoin.ctr.inbat == nil {
				input, err = vm.ChildrenCall(innerJoin.Children[0], proc, analyzer)
				if err != nil {
					return input, err
				}
				bat := input.Batch
				if bat == nil {
					if ctr.spilled {
						ctr.state = LoopSpilledPartitions
					} else {
						ctr.state = End
					}
					continue
				}
				if bat.Last() {
					result.Batch = bat
					return result, nil
				}
				if bat.IsEmpty() {
					continue
				}
				if ctr.mp == nil {
					continue
				}
				ctr.inbat = bat
				ctr.lastRow = 0
			}

			startrow := innerJoin.ctr.lastRow
			if err := ctr.probe(innerJoin, proc, &result); err != nil {
				return result, err
			}

			if innerJoin.ctr.lastRow == startrow && ctr.inbat != nil &&
				(result.Batch == nil || result.Batch.IsEmpty()) {
				return result, moerr.NewInternalErrorNoCtx("inner join hanging")
			}

			return result, nil

		case LoopSpilledPartitions:
			// Start looping from partition 1
			ctr.currentPartition = 1
			ctr.state = ProcessSpilledPartition

		case ProcessSpilledPartition:
			// Load build side partition
			buildFileName := fmt.Sprintf("hashbuild_spill_%s_%d_%d", proc.QueryId(), ctr.mp.BuildOpID, ctr.currentPartition)

			// Open and read the partition file
			reader, err := os.Open(buildFileName)
			if err != nil {
				if os.IsNotExist(err) {
					// No more partitions
					ctr.state = End
					continue
				}
				return result, err
			}
			defer reader.Close()

			// Read all batches for this partition
			var buildBatches []*batch.Batch
			for {
				// Read batch size first
				sizeBuf := make([]byte, 4)
				_, err := io.ReadFull(reader, sizeBuf)
				if err == io.EOF {
					break
				}
				if err != nil {
					return result, err
				}
				batchSize := binary.LittleEndian.Uint32(sizeBuf)

				// Read batch data
				data := make([]byte, batchSize)
				_, err = io.ReadFull(reader, data)
				if err != nil {
					return result, err
				}

				// Unmarshal batch
				bat := &batch.Batch{}
				if err := bat.UnmarshalBinary(data); err != nil {
					return result, err
				}
				buildBatches = append(buildBatches, bat)
			}
			reader.Close()
			os.Remove(buildFileName) // Cleanup build spill file

			// Build hash map for this partition
			ctr.hashmapBuilder.Batches.Buf = buildBatches
			ctr.hashmapBuilder.InputBatchRowCount = 0
			for _, bat := range buildBatches {
				ctr.hashmapBuilder.InputBatchRowCount += bat.RowCount()
			}

			// Build the hashmap for this partition
			needUniqueVec := true
			if innerJoin.RuntimeFilterSpecs == nil || len(innerJoin.RuntimeFilterSpecs) == 0 || innerJoin.RuntimeFilterSpecs[0].Expr == nil {
				needUniqueVec = false
			}
			err = ctr.hashmapBuilder.BuildHashmap(innerJoin.HashOnPK, true, needUniqueVec, proc)
			if err != nil {
				return result, err
			}

			// Create a new JoinMap for this partition
			ctr.mp = message.NewJoinMap(ctr.hashmapBuilder.MultiSels, ctr.hashmapBuilder.IntHashMap, ctr.hashmapBuilder.StrHashMap, ctr.hashmapBuilder.DelRows, ctr.hashmapBuilder.Batches.Buf, proc.Mp())
			ctr.itr = ctr.mp.NewIterator()

			// Load probe side partition
			probeFileName := fmt.Sprintf("join_probe_spill_%s_%d_%d", proc.QueryId(), innerJoin.GetOperatorID(), ctr.currentPartition)

			probeReader, err := os.Open(probeFileName)
			if err != nil {
				if os.IsNotExist(err) {
					// No probe data for this partition, but build data existed (and processed).
					// Skip to next partition.
					// Clean up for this iteration
					ctr.hashmapBuilder.Free(proc)
					ctr.mp.Free()

					ctr.currentPartition++
					if ctr.currentPartition >= ctr.partitionCnt {
						ctr.state = End
					}
					continue
				}
				return result, err
			}
			defer probeReader.Close()

			// Read probe batch
			// Note: Probe file might contain multiple batches appended
			data, err := io.ReadAll(probeReader)
			if err != nil {
				return result, err
			}
			probeReader.Close()
			os.Remove(probeFileName) // Cleanup probe spill file

			if len(data) > 0 {
				probeBat := &batch.Batch{}
				if err := probeBat.UnmarshalBinary(data); err != nil {
					return result, err
				}

				// Probe the hash map with the partition data
				ctr.inbat = probeBat
				ctr.lastRow = 0
				ctr.probeState = psNextBatch

				if err := ctr.probeInMemory(innerJoin, proc, &result); err != nil {
					return result, err
				}
				// If result found, return it. The loop will continue in this state until probeInMemory returns nil batch
				if result.Batch != nil && !result.Batch.IsEmpty() {
					return result, nil
				}
			}

			// Clean up for this iteration
			ctr.hashmapBuilder.Free(proc)
			ctr.mp.Free()
			if ctr.inbat != nil {
				ctr.inbat.Clean(proc.Mp())
				ctr.inbat = nil
			}

			ctr.currentPartition++
			if ctr.currentPartition >= ctr.partitionCnt {
				ctr.state = End
			}
			// Continue to next partition immediately if no result from this one
			continue

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (innerJoin *InnerJoin) build(analyzer process.Analyzer, proc *process.Process) (err error) {
	ctr := &innerJoin.ctr
	start := time.Now()
	defer analyzer.WaitStop(start)
	ctr.mp, err = message.ReceiveJoinMap(innerJoin.JoinMapTag, innerJoin.IsShuffle, innerJoin.ShuffleIdx, proc.GetMessageBoard(), proc.Ctx)
	if err != nil {
		return err
	}
	if ctr.mp != nil {
		ctr.maxAllocSize = max(ctr.maxAllocSize, ctr.mp.Size())
		ctr.spilled = ctr.mp.Spilled
		ctr.partitionCnt = ctr.mp.PartitionCnt
		if ctr.spilled {
			// Prepare the hashmap builder for spilled partitions
			ctr.hashmapBuilder.IsDedup = false // Dedup logic is already handled by the build side
			if err := ctr.hashmapBuilder.Prepare(innerJoin.Conditions[1], -1, proc); err != nil {
				return err
			}
		}
	}
	if ctr.mp != nil {
		ctr.batchRowCount = ctr.mp.GetRowCount()
	}
	return nil
}

func (ctr *container) setupResultAndCondition(ap *InnerJoin, proc *process.Process) error {
	mpbat := ctr.mp.GetBatches()
	if ctr.rbat == nil {
		ctr.rbat = batch.NewOffHeapWithSize(len(ap.Result))
		for i, rp := range ap.Result {
			if rp.Rel == 0 {
				ctr.rbat.Vecs[i] = vector.NewOffHeapVecWithType(*ap.ctr.inbat.Vecs[rp.Pos].GetType())
				// for inner join, if left batch is sorted , then output batch is sorted
				ctr.rbat.Vecs[i].SetSorted(ap.ctr.inbat.Vecs[rp.Pos].GetSorted())
			} else {
				ctr.rbat.Vecs[i] = vector.NewOffHeapVecWithType(*mpbat[0].Vecs[rp.Pos].GetType())
			}
		}
	} else {
		ctr.rbat.CleanOnlyData()
		for i, rp := range ap.Result {
			if rp.Rel == 0 {
				ctr.rbat.Vecs[i].SetSorted(ap.ctr.inbat.Vecs[rp.Pos].GetSorted())
			}
		}
	}

	if err := ctr.evalJoinCondition(ap.ctr.inbat, proc); err != nil {
		return err
	}
	if ctr.joinBats[0] == nil {
		ctr.joinBats[0], ctr.cfs1 = colexec.NewJoinBatch(ap.ctr.inbat, proc.Mp())
	}
	if ctr.joinBats[1] == nil && ctr.batchRowCount > 0 {
		ctr.joinBats[1], ctr.cfs2 = colexec.NewJoinBatch(mpbat[0], proc.Mp())
	}
	if ctr.itr == nil {
		ctr.itr = ctr.mp.NewIterator()
	}
	return nil
}

func (ctr *container) probe(ap *InnerJoin, proc *process.Process, result *vm.CallResult) error {
	if err := ctr.setupResultAndCondition(ap, proc); err != nil {
		return err
	}

	if ctr.spilled {
		// Partition the incoming probe batch
		numPartitions := uint64(ctr.partitionCnt)
		rowCount := ap.ctr.inbat.RowCount()
		hashes := make([]uint64, rowCount)

		// Calculate hashes for join keys
		for i, _ := range ap.Conditions[0] {
			vec, err := ctr.executor[i].Eval(proc, []*batch.Batch{ap.ctr.inbat}, nil)
			if err != nil {
				return err
			}

			// Compute hash values
			hashCol := make([]uint64, rowCount)
			if vec.GetType().IsFixedLen() && vec.GetType().TypeSize() == 8 {
				cols := vector.MustFixedColNoTypeCheck[int64](vec)
				hashtable.Int64BatchHash(unsafe.Pointer(&cols[0]), &hashCol[0], rowCount)
			} else {
				for i := 0; i < rowCount; i++ {
					bytes := vec.GetBytesAt(i)
					var states [3]uint64
					hashtable.BytesBatchGenHashStatesWithSeed(&bytes, &states, 1, 0)
					hashCol[i] = states[0]
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
			sels[i] = make([]int64, 0, rowCount/int(numPartitions)+1)
		}

		for row := 0; row < rowCount; row++ {
			partition := hashes[row] % numPartitions
			sels[partition] = append(sels[partition], int64(row))
		}

		partitionedBatches := make([][]*batch.Batch, numPartitions)
		// Create new batches for each partition
		for i := range sels {
			if len(sels[i]) > 0 {
				newBat := batch.NewWithSize(len(ap.ctr.inbat.Vecs))
				for j, vec := range ap.ctr.inbat.Vecs {
					newBat.Vecs[j] = vector.NewVec(*vec.GetType())
				}
				for col := range ap.ctr.inbat.Vecs {
					if err := newBat.Vecs[col].Union(ap.ctr.inbat.Vecs[col], sels[i], proc.Mp()); err != nil {
						return err
					}
				}
				newBat.SetRowCount(len(sels[i]))
				partitionedBatches[i] = append(partitionedBatches[i], newBat)
			}
		}

		// Process partition 0 in memory
		if len(partitionedBatches[0]) > 0 {
			// Temporarily replace the input batch with partition 0's batch for probing
			originalInbat := ap.ctr.inbat
			ap.ctr.inbat = partitionedBatches[0][0]
			originalLastRow := ctr.lastRow
			ctr.lastRow = 0

			// Probe partition 0
			if err := ctr.probeInMemory(ap, proc, result); err != nil {
				return err
			}

			// Restore state
			ap.ctr.inbat = originalInbat
			ctr.lastRow = originalLastRow
			partitionedBatches[0][0].Clean(proc.Mp())
		}

		// Spill other partitions to disk
		for i := uint64(1); i < numPartitions; i++ {
			if len(partitionedBatches[i]) == 0 {
				continue
			}

			// Write probe partition to disk
			partitionFileName := fmt.Sprintf("join_probe_spill_%s_%d_%d", proc.QueryId(), ap.GetOperatorID(), i)
			writer, err := os.OpenFile(partitionFileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
			if err != nil {
				return err
			}

			for _, bat := range partitionedBatches[i] {
				data, err := bat.MarshalBinary()
				if err != nil {
					writer.Close()
					return err
				}
				_, err = writer.Write(data)
				if err != nil {
					writer.Close()
					return err
				}
				bat.Clean(proc.Mp())
			}
			writer.Close()
		}

		// Clean up the original input batch
		ap.ctr.inbat.Clean(proc.Mp())
		ap.ctr.inbat = nil
		ctr.lastRow = 0
		ctr.state = LoopSpilledPartitions
		return nil
	}

	return ctr.probeInMemory(ap, proc, result)
}

func (ctr *container) evalAndAppendOne(
	proc *process.Process, ap *InnerJoin, row, idx1, idx2 int64,
) (bool, error) {
	condPass, err := ctr.evalNonEqCondition(
		ap.ctr.inbat, row, proc, idx1, idx2,
	)
	if err != nil {
		return false, err
	}
	if condPass {
		err := ctr.appendOne(proc, ap, row, idx1, idx2)
		if err != nil {
			return false, err
		}
	}
	return condPass, nil
}

func (ctr *container) evalNonEqCondition(
	bat *batch.Batch, row int64, proc *process.Process, idx1, idx2 int64,
) (bool, error) {
	mpbat := ctr.mp.GetBatches()
	// TODO: fix the probe row in closure, only change the matched row in hashmap
	if err := colexec.SetJoinBatchValues(
		ctr.joinBats[0], bat, row, 1, ctr.cfs1,
	); err != nil {
		return false, err
	}
	if err := colexec.SetJoinBatchValues(
		ctr.joinBats[1], mpbat[idx1], idx2, 1, ctr.cfs2,
	); err != nil {
		return false, err
	}
	vec, err := ctr.expr.Eval(proc, ctr.joinBats, nil)
	if err != nil {
		return false, err
	}
	if vec.IsConstNull() || vec.GetNulls().Contains(0) {
		return false, nil
	}
	bs := vector.MustFixedColWithTypeCheck[bool](vec)
	if !bs[0] {
		return false, nil
	}
	return true, nil
}

func (ctr *container) appendOne(
	proc *process.Process, ap *InnerJoin, row, idx1, idx2 int64,
) error {
	mpbat := ctr.mp.GetBatches()
	for j, rp := range ap.Result {
		if rp.Rel == 0 {
			if err := ctr.rbat.Vecs[j].UnionOne(
				ctr.inbat.Vecs[rp.Pos], row, proc.Mp(),
			); err != nil {
				return err
			}
		} else {
			if err := ctr.rbat.Vecs[j].UnionOne(
				mpbat[idx1].Vecs[rp.Pos], idx2, proc.Mp(),
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ctr *container) evalJoinCondition(bat *batch.Batch, proc *process.Process) error {
	for i := range ctr.executor {
		vec, err := ctr.executor[i].Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			return err
		}
		ctr.vecs[i] = vec
	}
	return nil
}

func (ctr *container) probeInMemory(ap *InnerJoin, proc *process.Process, result *vm.CallResult) error {
	if err := ctr.setupResultAndCondition(ap, proc); err != nil {
		return err
	}

	mpbat := ctr.mp.GetBatches()
	inputRowCount := ap.ctr.inbat.RowCount()
	rowCount := 0

	for {
		switch ctr.probeState {
		case psSelsForOneRow:
			// Branch 1:handle remaining sels for ctr.lastRow - 1
			// a non-unique column row is mapped to multiple record in hashmap,
			// appending at most 8192 rows to result
			processCount := min(
				len(ctr.sels),
				colexec.DefaultBatchSize-rowCount,
			)
			sels := ctr.sels[:processCount]
			// remove processed sels
			ctr.sels = ctr.sels[processCount:]
			row := int64(ctr.lastRow - 1)
			if ap.Cond == nil {
				// append multi rows by batch
				for j, rp := range ap.Result {
					if rp.Rel == 0 {
						if err := ctr.rbat.Vecs[j].UnionMulti(
							ap.ctr.inbat.Vecs[rp.Pos],
							row,
							processCount, proc.Mp()); err != nil {
							return err
						}
					} else {
						for _, sel := range sels {
							idx1 := int64(sel / colexec.DefaultBatchSize)
							idx2 := int64(sel % colexec.DefaultBatchSize)
							if err := ctr.rbat.Vecs[j].UnionOne(
								mpbat[idx1].Vecs[rp.Pos],
								idx2,
								proc.Mp(),
							); err != nil {
								return err
							}
						}
					}
				}
				rowCount += processCount
			} else {
				// append one by one
				for _, sel := range sels {
					idx1 := int64(sel / colexec.DefaultBatchSize)
					idx2 := int64(sel % colexec.DefaultBatchSize)
					ok, err := ctr.evalAndAppendOne(proc, ap, row, idx1, idx2)
					if err != nil {
						return err
					}
					if ok {
						rowCount++
					}
				}
			}

			if len(ctr.sels) > 0 {
				ctr.probeState = psSelsForOneRow
			} else if ctr.vsidx < len(ctr.vs) {
				ctr.probeState = psBatchRow
			} else {
				ctr.probeState = psNextBatch
			}

			if rowCount >= colexec.DefaultBatchSize {
				ctr.rbat.AddRowCount(rowCount)
				result.Batch = ctr.rbat
				return nil
			}

		case psBatchRow:
			// Branch 2:fetch sels for ctr.lastRow
			z, v := ctr.zvs[ctr.vsidx], ctr.vs[ctr.vsidx]
			row := int64(ctr.lastRow)
			idx := v - 1
			// skip this row immediately to simplify the thinking paths
			ctr.lastRow++
			ctr.vsidx++
			if z == 0 || v == 0 {
				if ctr.vsidx >= len(ctr.vs) {
					ctr.probeState = psNextBatch
				}
				continue
			}
			idx1 := int64(idx / colexec.DefaultBatchSize)
			idx2 := int64(idx % colexec.DefaultBatchSize)
			if ap.HashOnPK || ctr.mp.HashOnUnique() {
				// this is a fast path for unique column,
				// appending at most 1 row to result
				if ap.Cond == nil {
					err := ctr.appendOne(proc, ap, row, idx1, idx2)
					if err != nil {
						return err
					}
					rowCount++
				} else {
					ok, err := ctr.evalAndAppendOne(proc, ap, row, idx1, idx2)
					if err != nil {
						return err
					}
					if ok {
						rowCount++
					}
				}
			} else {
				ctr.sels = ctr.mp.GetSels(idx)
			}

			if len(ctr.sels) > 0 {
				ctr.probeState = psSelsForOneRow
			} else if ctr.vsidx < len(ctr.vs) {
				ctr.probeState = psBatchRow
			} else {
				ctr.probeState = psNextBatch
			}

			if rowCount >= colexec.DefaultBatchSize {
				ctr.rbat.AddRowCount(rowCount)
				result.Batch = ctr.rbat
				return nil
			}

		case psNextBatch:
			// Branch 3:fetch next batch// Branch 3:fetch next batch
			if ctr.lastRow < inputRowCount {
				// preprocess for next batch
				hashBatch := min(inputRowCount-ctr.lastRow, hashmap.UnitLimit)
				ctr.vs, ctr.zvs = ctr.itr.Find(ctr.lastRow, hashBatch, ctr.vecs)
				ctr.vsidx = 0
				ctr.probeState = psBatchRow
			} else {
				// return current result and set the input batch to nil
				if inputRowCount > 0 {
					ap.ctr.inbat.Clean(proc.Mp())
				}
				ctr.rbat.AddRowCount(rowCount)
				result.Batch = ctr.rbat
				ap.ctr.lastRow = 0
				ap.ctr.inbat = nil
				ctr.probeState = psNextBatch
				return nil
			}
		}
	}
}
