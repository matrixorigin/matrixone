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

	"github.com/matrixorigin/matrixone/pkg/sql/colexec"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString("insert")
}

func Prepare(_ *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.state = Process
	if ap.ToWriteS3 {
		s3Writer, err := colexec.AllocS3Writer(ap.InsertCtx.TableDef)
		if err != nil {
			return err
		}
		ap.ctr.s3Writer = s3Writer
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
	s3Writer := ap.ctr.s3Writer
	bat := proc.InputBatch()
	if bat == nil {
		if ap.ToWriteS3 {
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
		return true, nil
	}
	if bat.Length() == 0 {
		bat.Clean(proc.Mp())
		return false, nil
	}
	defer proc.PutBatch(bat)
	insertCtx := ap.InsertCtx

	if ap.ToWriteS3 {
		// write to s3.
		if err := s3Writer.WriteS3Batch(bat, proc); err != nil {
			ap.ctr.state = End
			return false, err
		}
		err := s3Writer.Output(proc)
		if err != nil {
			return false, err
		}
	} else {
		insertBat := batch.NewWithSize(len(ap.InsertCtx.Attrs))
		insertBat.Attrs = ap.InsertCtx.Attrs
		for i := range insertBat.Attrs {
			vec := vector.NewVec(*bat.Vecs[i].GetType())
			if err := vec.UnionBatch(bat.Vecs[i], 0, bat.Vecs[i].Length(), nil, proc.GetMPool()); err != nil {
				return false, err
			}
			insertBat.SetVector(int32(i), vec)
		}
		insertBat.Zs = append(insertBat.Zs, bat.Zs...)

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
				return false, err
			}
		}

		// `insertBat` does not include partition expression columns
		if insertCtx.IsEnd {
			proc.SetInputBatch(nil)
			insertBat.Clean(proc.GetMPool())
		} else {
			proc.SetInputBatch(insertBat)
		}
	}

	affectedRows := uint64(bat.Vecs[0].Length())
	atomic.AddUint64(&ap.affectedRows, affectedRows)
	return false, nil
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
