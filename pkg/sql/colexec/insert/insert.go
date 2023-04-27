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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString("insert")
}

func Prepare(_ *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.state = Process
	if ap.IsRemote {
		s3Writers, err := colexec.AllocS3Writers(ap.InsertCtx.TableDef)
		if err != nil {
			return err
		}
		ap.ctr.s3Writers = s3Writers
	}
	return nil
}

func Call(idx int, proc *process.Process, arg any, _ bool, _ bool) (bool, error) {
	defer analyze(proc, idx)()
	ap := arg.(*Argument)
	if ap.ctr.state == End {
		proc.SetInputBatch(nil)
		return true, nil
	}
	s3Writers := ap.ctr.s3Writers
	bat := proc.InputBatch()
	if bat == nil {
		if ap.IsRemote {
			// handle the last Batch that batchSize less than DefaultBlockMaxRows
			// for more info, refer to the comments about reSizeBatch
			for _, s3Writer := range s3Writers {
				if err := s3Writer.WriteS3CacheBatch(proc); err != nil {
					ap.ctr.state = End
					return false, err
				}
			}
			if err := collectAndOutput(proc, s3Writers); err != nil {
				ap.ctr.state = End
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
	proc.SetInputBatch(&batch.Batch{})
	insertCtx := ap.InsertCtx
	//write origin table
	if ap.IsRemote {
		// write to s3.
		if err := s3Writers[0].WriteS3Batch(bat, proc); err != nil {
			ap.ctr.state = End
			return false, err
		}
	} else {
		// write origin table, bat will be deeply copied into txn's workspace.
		if err := insertCtx.Rels[0].Write(proc.Ctx, bat); err != nil {
			ap.ctr.state = End
			return false, err
		}
	}

	// write unique key table
	nameToPos, pkPos := getUniqueKeyInfo(insertCtx.TableDef)
	var uniqIndexs []engine.Relation
	if len(insertCtx.Rels) > 1 {
		uniqIndexs = insertCtx.Rels[1:]
	}
	err := colexec.WriteUniqueTable(
		s3Writers, proc, bat, insertCtx.TableDef,
		nameToPos, pkPos, uniqIndexs)
	if err != nil {
		return false, err
	}
	affectedRows := uint64(bat.Vecs[0].Length())
	atomic.AddUint64(&ap.Affected, affectedRows)
	return false, nil
}

func collectAndOutput(proc *process.Process, s3Writers []*colexec.S3Writer) (err error) {
	attrs := []string{catalog.BlockMeta_TableIdx_Insert, catalog.BlockMeta_MetaLoc}
	res := batch.NewWithSize(len(attrs))
	res.SetAttributes(attrs)
	res.Vecs[0] = vector.NewVec(types.T_int16.ToType())
	res.Vecs[1] = vector.NewVec(types.T_text.ToType())
	for _, w := range s3Writers {
		//deep copy.
		res, err = res.Append(proc.Ctx, proc.GetMPool(), w.GetMetaLocBat())
		if err != nil {
			return
		}
	}
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

func getUniqueKeyInfo(tableDef *pb.TableDef) (map[string]int, int) {
	nameToPos := make(map[string]int)
	pkPos := -1
	pos := 0
	hasCompositePKey := false
	if tableDef.Pkey != nil && tableDef.Pkey.CompPkeyCol != nil {
		hasCompositePKey = true
	}
	for j, col := range tableDef.Cols {
		// Check whether the composite primary key column is included
		if !hasCompositePKey && col.Name != catalog.Row_ID && col.Primary {
			pkPos = j
		}
		if col.Name != catalog.Row_ID {
			nameToPos[col.Name] = pos
			pos++
		}
	}
	// Check whether the composite primary key column is included
	if hasCompositePKey {
		pkPos = pos
	}
	return nameToPos, pkPos
}
