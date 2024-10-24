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

package insert

import (
	"bytes"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "insert"

func (insert *Insert) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": insert")
}

func (insert *Insert) OpType() vm.OpType {
	return vm.Insert
}

func (insert *Insert) Prepare(proc *process.Process) error {
	if insert.OpAnalyzer == nil {
		insert.OpAnalyzer = process.NewAnalyzer(insert.GetIdx(), insert.IsFirst, insert.IsLast, "insert")
	} else {
		insert.OpAnalyzer.Reset()
	}

	insert.ctr.state = vm.Build
	if insert.ToWriteS3 {
		if len(insert.InsertCtx.PartitionTableIDs) > 0 {
			// If the target is partition table, just only apply writers for all partitioned sub tables
			s3Writers, err := colexec.NewPartitionS3Writer(insert.InsertCtx.TableDef)
			if err != nil {
				return err
			}
			insert.ctr.partitionS3Writers = s3Writers
		} else {
			// If the target is not partition table, you only need to operate the main table
			s3Writer, err := colexec.NewS3Writer(insert.InsertCtx.TableDef, 0)
			if err != nil {
				return err
			}
			insert.ctr.s3Writer = s3Writer
		}

		if insert.ctr.buf == nil {
			insert.initBufForS3()
		}
	} else {
		ref := insert.InsertCtx.Ref
		eng := insert.InsertCtx.Engine
		partitionNames := insert.InsertCtx.PartitionTableNames
		rel, partitionRels, err := colexec.GetRelAndPartitionRelsByObjRef(proc.Ctx, proc, eng, ref, partitionNames)
		if err != nil {
			return err
		}
		insert.ctr.source = rel
		insert.ctr.partitionSources = partitionRels

		if insert.ctr.buf == nil {
			insert.ctr.buf = batch.NewWithSize(len(insert.InsertCtx.Attrs))
			insert.ctr.buf.SetAttributes(insert.InsertCtx.Attrs)
		}
	}
	insert.ctr.affectedRows = 0
	return nil
}

// first parameter: true represents whether the current pipeline has ended
// first parameter: false
func (insert *Insert) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	analyzer := insert.OpAnalyzer
	analyzer.Start()

	t := time.Now()
	defer func() {
		analyzer.AddInsertTime(t)
		analyzer.Stop()
	}()

	if insert.ToWriteS3 {
		return insert.insert_s3(proc, analyzer)
	}
	return insert.insert_table(proc, analyzer)
}

func (insert *Insert) insert_s3(proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementInsertS3DurationHistogram.Observe(time.Since(start).Seconds())
	}()

	if insert.ctr.state == vm.Build {
		for {
			input, err := vm.ChildrenCall(insert.GetChildren(0), proc, analyzer)
			if err != nil {
				return input, err
			}

			if input.Batch == nil {
				insert.ctr.state = vm.Eval
				break
			}
			if input.Batch.IsEmpty() {
				continue
			}

			if insert.InsertCtx.AddAffectedRows {
				affectedRows := uint64(input.Batch.RowCount())
				atomic.AddUint64(&insert.ctr.affectedRows, affectedRows)
			}

			// If the target is partition table
			if len(insert.InsertCtx.PartitionTableIDs) > 0 {
				insert.ctr.buf = batch.NewWithSize(len(insert.InsertCtx.Attrs))
				insert.ctr.buf.Attrs = insert.InsertCtx.Attrs
				for i := range insert.ctr.buf.Attrs {
					insert.ctr.buf.Vecs[i] = vector.NewVec(*input.Batch.Vecs[i].GetType())
				}

				for partIdx := range len(insert.InsertCtx.PartitionTableIDs) {
					expect := int32(partIdx)
					err = colexec.FillPartitionBatchForInsert(proc, input.Batch, insert.ctr.buf, expect, insert.InsertCtx.PartitionIndexInBatch)
					if err != nil {
						insert.ctr.state = vm.End
						return vm.CancelResult, err
					}

					err = writeBatch(proc, insert.ctr.partitionS3Writers[partIdx], insert.ctr.buf, analyzer)
					if err != nil {
						insert.ctr.state = vm.End
						return vm.CancelResult, err
					}
					insert.ctr.buf.CleanOnlyData()
				}

				insert.initBufForS3()
			} else {
				// Normal non partition table
				// write to s3.
				input.Batch.Attrs = append(input.Batch.Attrs[:0], insert.InsertCtx.Attrs...)
				err = writeBatch(proc, insert.ctr.s3Writer, input.Batch, analyzer)
				if err != nil {
					insert.ctr.state = vm.End
					return vm.CancelResult, err
				}
			}
		}
	}

	result := vm.NewCallResult()
	result.Batch = insert.ctr.buf
	if insert.ctr.state == vm.Eval {

		// If the target is partition table
		if len(insert.InsertCtx.PartitionTableIDs) > 0 {
			for _, writer := range insert.ctr.partitionS3Writers {
				err := flushTailBatch(proc, writer, &result, analyzer)
				if err != nil {
					insert.ctr.state = vm.End
					return vm.CancelResult, err
				}
			}
		} else {
			// Normal non partition table
			writer := insert.ctr.s3Writer
			// handle the last Batch that batchSize less than DefaultBlockMaxRows
			// for more info, refer to the comments about reSizeBatch
			err := flushTailBatch(proc, writer, &result, analyzer)
			if err != nil {
				insert.ctr.state = vm.End
				return result, err
			}
		}
		insert.ctr.state = vm.End
		analyzer.Output(result.Batch)
		return result, nil
	}

	if insert.ctr.state == vm.End {
		return vm.CancelResult, nil
	}

	panic("bug")
}

func (insert *Insert) insert_table(proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	input, err := vm.ChildrenCall(insert.GetChildren(0), proc, analyzer)
	if err != nil {
		return input, err
	}

	if input.Batch == nil || input.Batch.IsEmpty() {
		return input, nil
	}

	affectedRows := uint64(input.Batch.RowCount())
	if len(insert.InsertCtx.PartitionTableIDs) > 0 {
		insert.ctr.buf.Attrs = insert.InsertCtx.Attrs
		for i := range insert.ctr.buf.Attrs {
			if insert.ctr.buf.Vecs[i] == nil {
				insert.ctr.buf.Vecs[i] = vector.NewVec(*input.Batch.Vecs[i].GetType())
			}
		}

		for partIdx := range len(insert.InsertCtx.PartitionTableIDs) {
			insert.ctr.buf.CleanOnlyData()
			expect := int32(partIdx)
			err = colexec.FillPartitionBatchForInsert(proc, input.Batch, insert.ctr.buf, expect, insert.InsertCtx.PartitionIndexInBatch)
			if err != nil {
				return input, err
			}

			crs := new(perfcounter.CounterSet)
			newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)
			err = insert.ctr.partitionSources[partIdx].Write(newCtx, insert.ctr.buf)
			if err != nil {
				return input, err
			}
			analyzer.AddS3RequestCount(crs)
			analyzer.AddDiskIO(crs)
		}
	} else {
		insert.ctr.buf.CleanOnlyData()
		for i := range insert.ctr.buf.Attrs {
			if insert.ctr.buf.Vecs[i] == nil {
				insert.ctr.buf.Vecs[i] = vector.NewVec(*input.Batch.Vecs[i].GetType())
			}
			if err = insert.ctr.buf.Vecs[i].UnionBatch(input.Batch.Vecs[i], 0, input.Batch.Vecs[i].Length(), nil, proc.GetMPool()); err != nil {
				return input, err
			}
		}
		insert.ctr.buf.SetRowCount(input.Batch.RowCount())

		crs := new(perfcounter.CounterSet)
		newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)

		// insert into table, insertBat will be deeply copied into txn's workspace.
		err = insert.ctr.source.Write(newCtx, insert.ctr.buf)
		if err != nil {
			return input, err
		}
		analyzer.AddS3RequestCount(crs)
		analyzer.AddDiskIO(crs)
	}

	if insert.InsertCtx.AddAffectedRows {
		atomic.AddUint64(&insert.ctr.affectedRows, affectedRows)
	}
	// `insertBat` does not include partition expression columns
	analyzer.Output(input.Batch)
	return input, nil
}

func writeBatch(proc *process.Process, writer *colexec.S3Writer, bat *batch.Batch, analyzer process.Analyzer) error {
	if writer.StashBatch(proc, bat) {
		crs := new(perfcounter.CounterSet)
		newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)

		blockInfos, stats, err := writer.SortAndSync(newCtx, proc)
		if err != nil {
			return err
		}
		analyzer.AddS3RequestCount(crs)
		analyzer.AddDiskIO(crs)

		err = writer.FillBlockInfoBat(blockInfos, stats, proc.GetMPool())
		if err != nil {
			return err
		}
	}
	return nil
}

func flushTailBatch(proc *process.Process, writer *colexec.S3Writer, result *vm.CallResult, analyzer process.Analyzer) error {
	crs := new(perfcounter.CounterSet)
	newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)

	blockInfos, stats, err := writer.FlushTailBatch(newCtx, proc)
	if err != nil {
		return err
	}
	analyzer.AddS3RequestCount(crs)
	analyzer.AddDiskIO(crs)

	// if stats is not zero, then the blockInfos must not be nil
	if !stats.IsZero() {
		err = writer.FillBlockInfoBat(blockInfos, stats, proc.GetMPool())
		if err != nil {
			return err
		}
	}

	return writer.Output(proc, result)
}
