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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString("insert")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.state = Process
	if ap.ToWriteS3 {
		if len(ap.InsertCtx.PartitionTableIDs) > 0 {
			// If the target is partition table, just only apply writers for all partitioned sub tables
			s3Writers, err := colexec.AllocPartitionS3Writer(proc, ap.InsertCtx.TableDef)
			if err != nil {
				return err
			}
			ap.ctr.partitionS3Writers = s3Writers
		} else {
			// If the target is not partition table, you only need to operate the main table
			s3Writer, err := colexec.AllocS3Writer(proc, ap.InsertCtx.TableDef)
			if err != nil {
				return err
			}
			ap.ctr.s3Writer = s3Writer
		}
	}
	return nil
}

// first parameter: true represents whether the current pipeline has ended
// first parameter: false
func Call(idx int, proc *process.Process, arg any, _ bool, _ bool) (bool, error) {
	defer analyze(proc, idx)()
	ap := arg.(*Argument)
	if ap.ctr.state == End {
		proc.SetInputBatch(nil)
		return true, nil
	}

	bat := proc.InputBatch()
	bat.FixedForRemoveZs()
	if bat == nil {
		// scenario 1 for cn write s3, more in the comment of S3Writer
		if ap.ToWriteS3 {
			// If the target is partition table
			if len(ap.InsertCtx.PartitionTableIDs) > 0 {
				for _, writer := range ap.ctr.partitionS3Writers {
					if err := writer.WriteS3CacheBatch(proc); err != nil {
						ap.ctr.state = End
						return false, err
					}
				}

				if err := collectAndOutput(proc, ap.ctr.partitionS3Writers); err != nil {
					ap.ctr.state = End
					return false, err
				}
			} else {
				// Normal non partition table
				s3Writer := ap.ctr.s3Writer
				// handle the last Batch that batchSize less than DefaultBlockMaxRows
				// for more info, refer to the comments about reSizeBatch
				if err := s3Writer.WriteS3CacheBatch(proc); err != nil {
					ap.ctr.state = End
					return false, err
				}
				err := s3Writer.Output(proc)
				if err != nil {
					return false, err
				}
			}
		}
		return true, nil
	}
	if bat.Length() == 0 {
		bat.Clean(proc.Mp())
		proc.SetInputBatch(batch.EmptyBatch)
		return false, nil
	}
	defer proc.PutBatch(bat)
	insertCtx := ap.InsertCtx

	// scenario 1 for cn write s3, more in the comment of S3Writer
	if ap.ToWriteS3 {
		// If the target is partition table
		if len(ap.InsertCtx.PartitionTableIDs) > 0 {
			insertBatches, err := colexec.GroupByPartitionForInsert(proc, bat, ap.InsertCtx.Attrs, ap.InsertCtx.PartitionIndexInBatch, len(ap.InsertCtx.PartitionTableIDs))
			if err != nil {
				return false, err
			}

			// write partition data to s3.
			for pidx, writer := range ap.ctr.partitionS3Writers {
				if err = writer.WriteS3Batch(proc, insertBatches[pidx]); err != nil {
					ap.ctr.state = End
					insertBatches[pidx].Clean(proc.Mp())
					return false, err
				}
				insertBatches[pidx].Clean(proc.Mp())
			}
		} else {
			// Normal non partition table
			s3Writer := ap.ctr.s3Writer
			// write to s3.
			bat.Attrs = append(bat.Attrs[:0], ap.InsertCtx.Attrs...)
			if err := s3Writer.WriteS3Batch(proc, bat); err != nil {
				ap.ctr.state = End
				return false, err
			}
		}
		proc.SetInputBatch(batch.EmptyBatch)

	} else {
		insertBat := batch.NewWithSize(len(ap.InsertCtx.Attrs))
		insertBat.Attrs = ap.InsertCtx.Attrs
		for i := range insertBat.Attrs {
			vec := proc.GetVector(*bat.Vecs[i].GetType())
			if err := vec.UnionBatch(bat.Vecs[i], 0, bat.Vecs[i].Length(), nil, proc.GetMPool()); err != nil {
				return false, err
			}
			insertBat.SetVector(int32(i), vec)
		}
		insertBat.Zs = append(insertBat.Zs, bat.Zs...)
		insertBat.SetRowCount(insertBat.RowCount() + bat.RowCount())

		if len(ap.InsertCtx.PartitionTableIDs) > 0 {
			insertBatches, err := colexec.GroupByPartitionForInsert(proc, bat, ap.InsertCtx.Attrs, ap.InsertCtx.PartitionIndexInBatch, len(ap.InsertCtx.PartitionTableIDs))
			if err != nil {
				return false, err
			}
			for i, partitionBat := range insertBatches {
				err := ap.InsertCtx.PartitionSources[i].Write(proc.Ctx, partitionBat)
				if err != nil {
					partitionBat.Clean(proc.Mp())
					return false, err
				}
				partitionBat.Clean(proc.Mp())
			}
		} else {
			// insert into table, insertBat will be deeply copied into txn's workspace.
			err := insertCtx.Rel.Write(proc.Ctx, insertBat)
			if err != nil {
				proc.SetInputBatch(nil)
				insertBat.Clean(proc.GetMPool())
				return false, err
			}
		}

		// `insertBat` does not include partition expression columns
		proc.SetInputBatch(nil)
		insertBat.Clean(proc.GetMPool())
	}

	if ap.InsertCtx.AddAffectedRows {
		affectedRows := uint64(bat.Vecs[0].Length())
		atomic.AddUint64(&ap.affectedRows, affectedRows)
	}
	return false, nil
}

// Collect all partition subtables' s3writers  metaLoc information and output it
func collectAndOutput(proc *process.Process, s3Writers []*colexec.S3Writer) (err error) {
	attrs := []string{catalog.BlockMeta_TableIdx_Insert, catalog.BlockMeta_BlockInfo}
	res := batch.NewWithSize(len(attrs))
	res.SetAttributes(attrs)
	res.Vecs[0] = proc.GetVector(types.T_int16.ToType())
	res.Vecs[1] = proc.GetVector(types.T_text.ToType())
	for _, w := range s3Writers {
		//deep copy.
		res, err = res.Append(proc.Ctx, proc.GetMPool(), w.GetMetaLocBat())
		if err != nil {
			return
		}
		res.Zs = append(res.Zs, w.GetMetaLocBat().Zs...)
		res.SetRowCount(res.RowCount() + w.GetMetaLocBat().RowCount())
		w.ResetMetaLocBat(proc)
	}
	res.CheckForRemoveZs("insert")
	proc.SetInputBatch(res)
	return
}

func analyze(proc *process.Process, idx int) func() {
	t := time.Now()
	anal := proc.GetAnalyze(idx)
	anal.Start()
	return func() {
		anal.Stop()
		anal.AddInsertTime(t)
	}
}
