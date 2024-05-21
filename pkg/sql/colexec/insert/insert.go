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

const argName = "insert"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": insert")
}

func (arg *Argument) Prepare(proc *process.Process) error {
	arg.ctr = new(container)
	arg.ctr.state = vm.Build
	if arg.ToWriteS3 {
		if len(arg.InsertCtx.PartitionTableIDs) > 0 {
			// If the target is partition table, just only apply writers for all partitioned sub tables
			s3Writers, err := colexec.AllocPartitionS3Writer(proc, arg.InsertCtx.TableDef)
			if err != nil {
				return err
			}
			arg.ctr.partitionS3Writers = s3Writers
		} else {
			// If the target is not partition table, you only need to operate the main table
			s3Writer, err := colexec.AllocS3Writer(proc, arg.InsertCtx.TableDef)
			if err != nil {
				return err
			}
			arg.ctr.s3Writer = s3Writer
		}
	}
	ref := arg.InsertCtx.Ref
	eng := arg.InsertCtx.Engine
	partitionNames := arg.InsertCtx.PartitionTableNames
	rel, partitionRels, err := colexec.GetRelAndPartitionRelsByObjRef(proc.Ctx, proc, eng, ref, partitionNames)
	if err != nil {
		return err
	}
	arg.ctr.source = rel
	arg.ctr.partitionSources = partitionRels
	return nil
}

// first parameter: true represents whether the current pipeline has ended
// first parameter: false
func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	defer analyze(proc, arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())()
	if arg.ToWriteS3 {
		return arg.insert_s3(proc)
	}
	return arg.insert_table(proc)
}

func (arg *Argument) insert_s3(proc *process.Process) (vm.CallResult, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementInsertS3DurationHistogram.Observe(time.Since(start).Seconds())
	}()

	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())

	if arg.ctr.state == vm.Build {
		for {
			result, err := vm.ChildrenCall(arg.GetChildren(0), proc, anal)

			if err != nil {
				return result, err
			}
			if result.Batch == nil {
				arg.ctr.state = vm.Eval
				break
			}
			if result.Batch.IsEmpty() {
				continue
			}
			bat := result.Batch

			if arg.InsertCtx.AddAffectedRows {
				affectedRows := uint64(bat.RowCount())
				atomic.AddUint64(&arg.affectedRows, affectedRows)
			}

			// If the target is partition table
			if len(arg.InsertCtx.PartitionTableIDs) > 0 {
				insertBatches, err := colexec.GroupByPartitionForInsert(proc, bat, arg.InsertCtx.Attrs, arg.InsertCtx.PartitionIndexInBatch, len(arg.InsertCtx.PartitionTableIDs))
				if err != nil {
					return result, err
				}

				// write partition data to s3.
				for pidx, writer := range arg.ctr.partitionS3Writers {
					if err = writer.WriteS3Batch(proc, insertBatches[pidx]); err != nil {
						arg.ctr.state = vm.End
						insertBatches[pidx].Clean(proc.Mp())
						return result, err
					}
					insertBatches[pidx].Clean(proc.Mp())
				}
			} else {
				// Normal non partition table
				s3Writer := arg.ctr.s3Writer
				// write to s3.
				bat.Attrs = append(bat.Attrs[:0], arg.InsertCtx.Attrs...)
				if err := s3Writer.WriteS3Batch(proc, bat); err != nil {
					arg.ctr.state = vm.End
					return result, err
				}
			}
		}
	}

	result := vm.NewCallResult()
	if arg.ctr.state == vm.Eval {
		if arg.ctr.buf != nil {
			proc.PutBatch(arg.ctr.buf)
			arg.ctr.buf = nil
		}

		// If the target is partition table
		if len(arg.InsertCtx.PartitionTableIDs) > 0 {
			for _, writer := range arg.ctr.partitionS3Writers {
				if err := writer.WriteS3CacheBatch(proc); err != nil {
					arg.ctr.state = vm.End
					return result, err
				}
			}

			if err := collectAndOutput(proc, arg.ctr.partitionS3Writers, &result); err != nil {
				arg.ctr.state = vm.End
				return result, err
			}
		} else {
			// Normal non partition table
			s3Writer := arg.ctr.s3Writer
			// handle the last Batch that batchSize less than DefaultBlockMaxRows
			// for more info, refer to the comments about reSizeBatch
			if err := s3Writer.WriteS3CacheBatch(proc); err != nil {
				arg.ctr.state = vm.End
				return result, err
			}
			err := s3Writer.Output(proc, &result)
			if err != nil {
				return result, err
			}
		}
		arg.ctr.state = vm.End
		arg.ctr.buf = result.Batch
		return result, nil
	}

	if arg.ctr.state == vm.End {
		return result, nil
	}

	panic("bug")
}

func (arg *Argument) insert_table(proc *process.Process) (vm.CallResult, error) {

	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())

	result, err := vm.ChildrenCall(arg.GetChildren(0), proc, anal)

	if err != nil {
		return result, err
	}
	if result.Batch == nil || result.Batch.IsEmpty() {
		return result, nil
	}
	bat := result.Batch

	if arg.ctr.buf != nil {
		proc.PutBatch(arg.ctr.buf)
		arg.ctr.buf = nil
	}
	arg.ctr.buf = batch.NewWithSize(len(arg.InsertCtx.Attrs))
	arg.ctr.buf.Attrs = arg.InsertCtx.Attrs
	for i := range arg.ctr.buf.Attrs {
		vec := proc.GetVector(*bat.Vecs[i].GetType())
		if err = vec.UnionBatch(bat.Vecs[i], 0, bat.Vecs[i].Length(), nil, proc.GetMPool()); err != nil {
			vec.Free(proc.Mp())
			return result, err
		}
		arg.ctr.buf.SetVector(int32(i), vec)
	}
	arg.ctr.buf.SetRowCount(arg.ctr.buf.RowCount() + bat.RowCount())

	if len(arg.InsertCtx.PartitionTableIDs) > 0 {
		insertBatches, err := colexec.GroupByPartitionForInsert(proc, bat, arg.InsertCtx.Attrs, arg.InsertCtx.PartitionIndexInBatch, len(arg.InsertCtx.PartitionTableIDs))
		if err != nil {
			return result, err
		}
		for i, partitionBat := range insertBatches {
			err = arg.ctr.partitionSources[i].Write(proc.Ctx, partitionBat)
			if err != nil {
				partitionBat.Clean(proc.Mp())
				return result, err
			}
			proc.PutBatch(partitionBat)
		}
	} else {
		// insert into table, insertBat will be deeply copied into txn's workspace.
		err := arg.ctr.source.Write(proc.Ctx, arg.ctr.buf)
		if err != nil {
			return result, err
		}
	}

	if arg.InsertCtx.AddAffectedRows {
		affectedRows := uint64(arg.ctr.buf.Vecs[0].Length())
		atomic.AddUint64(&arg.affectedRows, affectedRows)
	}
	// `insertBat` does not include partition expression columns
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

func analyze(proc *process.Process, idx int, parallelIdx int, parallelMajor bool) func() {
	t := time.Now()
	anal := proc.GetAnalyze(idx, parallelIdx, parallelMajor)
	anal.Start()
	return func() {
		anal.Stop()
		anal.AddInsertTime(t)
	}
}
