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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
	insert.ctr = new(container)
	insert.ctr.state = vm.Build
	if insert.ToWriteS3 {
		if len(insert.InsertCtx.PartitionTableIDs) > 0 {
			// If the target is partition table, just only apply writers for all partitioned sub tables
			s3Writers, err := colexec.AllocPartitionS3Writer(proc, insert.InsertCtx.TableDef)
			if err != nil {
				return err
			}
			insert.ctr.partitionS3Writers = s3Writers
		} else {
			// If the target is not partition table, you only need to operate the main table
			s3Writer, err := colexec.AllocS3Writer(proc, insert.InsertCtx.TableDef)
			if err != nil {
				return err
			}
			insert.ctr.s3Writer = s3Writer
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
	}
	return nil
}

// first parameter: true represents whether the current pipeline has ended
// first parameter: false
func (insert *Insert) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(insert.GetIdx(), insert.GetParallelIdx(), insert.GetParallelMajor())
	anal.Start()
	t := time.Now()
	defer func() {
		anal.AddInsertTime(t)
		anal.Stop()
	}()

	if insert.ToWriteS3 {
		return insert.insert_s3(proc, anal)
	}
	return insert.insert_table(proc, anal)
}

func (insert *Insert) insert_s3(proc *process.Process, anal process.Analyze) (vm.CallResult, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementInsertS3DurationHistogram.Observe(time.Since(start).Seconds())
	}()

	if insert.ctr.state == vm.Build {
		for {
			result, err := vm.ChildrenCall(insert.GetChildren(0), proc, anal)
			if err != nil {
				return result, err
			}
			anal.Input(result.Batch, insert.IsFirst)

			if result.Batch == nil {
				insert.ctr.state = vm.Eval
				break
			}
			if result.Batch.IsEmpty() {
				continue
			}
			bat := result.Batch

			if insert.InsertCtx.AddAffectedRows {
				affectedRows := uint64(bat.RowCount())
				atomic.AddUint64(&insert.affectedRows, affectedRows)
			}

			// If the target is partition table
			if len(insert.InsertCtx.PartitionTableIDs) > 0 {
				insertBatches, err := colexec.GroupByPartitionForInsert(proc, bat, insert.InsertCtx.Attrs, insert.InsertCtx.PartitionIndexInBatch, len(insert.InsertCtx.PartitionTableIDs))
				if err != nil {
					return result, err
				}

				// write partition data to s3.
				for pidx, writer := range insert.ctr.partitionS3Writers {
					if err = writer.WriteS3Batch(proc, insertBatches[pidx]); err != nil {
						insert.ctr.state = vm.End
						insertBatches[pidx].Clean(proc.Mp())
						return result, err
					}
					insertBatches[pidx].Clean(proc.Mp())
				}
			} else {
				// Normal non partition table
				s3Writer := insert.ctr.s3Writer
				// write to s3.
				bat.Attrs = append(bat.Attrs[:0], insert.InsertCtx.Attrs...)
				if err := s3Writer.WriteS3Batch(proc, bat); err != nil {
					insert.ctr.state = vm.End
					return result, err
				}
			}
		}
	}

	result := vm.NewCallResult()
	if insert.ctr.state == vm.Eval {
		if insert.ctr.buf != nil {
			proc.PutBatch(insert.ctr.buf)
			insert.ctr.buf = nil
		}

		// If the target is partition table
		if len(insert.InsertCtx.PartitionTableIDs) > 0 {
			for _, writer := range insert.ctr.partitionS3Writers {
				if err := writer.WriteS3CacheBatch(proc); err != nil {
					insert.ctr.state = vm.End
					return result, err
				}
			}

			if err := collectAndOutput(proc, insert.ctr.partitionS3Writers, &result); err != nil {
				insert.ctr.state = vm.End
				return result, err
			}
		} else {
			// Normal non partition table
			s3Writer := insert.ctr.s3Writer
			// handle the last Batch that batchSize less than DefaultBlockMaxRows
			// for more info, refer to the comments about reSizeBatch
			if err := s3Writer.WriteS3CacheBatch(proc); err != nil {
				insert.ctr.state = vm.End
				return result, err
			}
			err := s3Writer.Output(proc, &result)
			if err != nil {
				return result, err
			}
		}
		insert.ctr.state = vm.End
		insert.ctr.buf = result.Batch
		anal.Output(result.Batch, insert.IsLast)
		return result, nil
	}

	if insert.ctr.state == vm.End {
		anal.Output(result.Batch, insert.IsLast)
		return result, nil
	}

	panic("bug")
}

func (insert *Insert) insert_table(proc *process.Process, anal process.Analyze) (vm.CallResult, error) {
	result, err := vm.ChildrenCall(insert.GetChildren(0), proc, anal)
	if err != nil {
		return result, err
	}
	anal.Input(result.Batch, insert.IsFirst)

	if result.Batch == nil || result.Batch.IsEmpty() {
		return result, nil
	}
	bat := result.Batch

	if insert.ctr.buf != nil {
		proc.PutBatch(insert.ctr.buf)
		insert.ctr.buf = nil
	}
	insert.ctr.buf = batch.NewWithSize(len(insert.InsertCtx.Attrs))
	insert.ctr.buf.Attrs = insert.InsertCtx.Attrs
	for i := range insert.ctr.buf.Attrs {
		vec := proc.GetVector(*bat.Vecs[i].GetType())
		if err = vec.UnionBatch(bat.Vecs[i], 0, bat.Vecs[i].Length(), nil, proc.GetMPool()); err != nil {
			vec.Free(proc.Mp())
			return result, err
		}
		insert.ctr.buf.SetVector(int32(i), vec)
	}
	insert.ctr.buf.SetRowCount(insert.ctr.buf.RowCount() + bat.RowCount())

	if len(insert.InsertCtx.PartitionTableIDs) > 0 {
		insertBatches, err := colexec.GroupByPartitionForInsert(proc, bat, insert.InsertCtx.Attrs, insert.InsertCtx.PartitionIndexInBatch, len(insert.InsertCtx.PartitionTableIDs))
		if err != nil {
			return result, err
		}
		for i, partitionBat := range insertBatches {
			err = insert.ctr.partitionSources[i].Write(proc.Ctx, partitionBat)
			if err != nil {
				partitionBat.Clean(proc.Mp())
				return result, err
			}
			proc.PutBatch(partitionBat)
		}
	} else {
		// insert into table, insertBat will be deeply copied into txn's workspace.
		err := insert.ctr.source.Write(proc.Ctx, insert.ctr.buf)
		if err != nil {
			return result, err
		}
	}

	if insert.InsertCtx.AddAffectedRows {
		affectedRows := uint64(insert.ctr.buf.Vecs[0].Length())
		atomic.AddUint64(&insert.affectedRows, affectedRows)
	}
	// `insertBat` does not include partition expression columns
	anal.Output(result.Batch, insert.IsLast)
	return result, nil
}

// Collect all partition subtables' s3writers  metaLoc information and output it
func collectAndOutput(proc *process.Process, s3Writers []*colexec.S3Writer, result *vm.CallResult) (err error) {
	attrs := []string{catalog.BlockMeta_TableIdx_Insert, catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats}
	res := batch.NewWithSize(len(attrs))
	res.SetAttributes(attrs)
	res.Vecs[0] = proc.GetVector(types.T_int16.ToType())
	res.Vecs[1] = proc.GetVector(types.T_text.ToType())
	res.Vecs[2] = proc.GetVector(types.T_binary.ToType())

	for _, w := range s3Writers {
		//deep copy.
		bat := w.GetBlockInfoBat()
		res, err = res.Append(proc.Ctx, proc.GetMPool(), bat)
		if err != nil {
			proc.PutBatch(res)
			return
		}
		res.SetRowCount(res.RowCount() + bat.RowCount())
		w.ResetBlockInfoBat(proc)
	}
	result.Batch = res
	return
}
