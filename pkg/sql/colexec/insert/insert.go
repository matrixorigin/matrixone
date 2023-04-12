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
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString("insert")
}

func Prepare(_ *process.Process, arg any) error {
	ap := arg.(*Argument)
	if ap.IsRemote {
		ap.s3Writer = colexec.NewS3Writer(ap.InsertCtx.TableDef)
	}
	return nil
}

func Call(idx int, proc *process.Process, arg any, _ bool, _ bool) (bool, error) {
	defer analyze(proc, idx)()

	insertArg := arg.(*Argument)
	s3Writer := insertArg.s3Writer
	bat := proc.InputBatch()
	if bat == nil {
		if insertArg.IsRemote {
			// handle the last Batch that batchSize less than DefaultBlockMaxRows
			// for more info, refer to the comments about reSizeBatch
			if err := s3Writer.WriteS3CacheBatch(proc); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	if bat.Length() == 0 {
		return false, nil
	}

	defer func() {
		bat.Clean(proc.Mp())
	}()

	insertCtx := insertArg.InsertCtx

	if insertArg.IsRemote {
		// write to s3
		err := s3Writer.WriteS3Batch(bat, proc, 0)
		if err != nil {
			return false, err
		}
	} else {
		// write origin table
		err := insertCtx.Source.Write(proc.Ctx, bat)
		if err != nil {
			return false, err
		}
	}

	// write unique key table
	nameToPos, pkPos := getUniqueKeyInfo(insertCtx.TableDef)
	err := colexec.WriteUniqueTable(s3Writer, proc, bat, insertCtx.TableDef, nameToPos, pkPos, insertCtx.UniqueSource)
	if err != nil {
		return false, err
	}

	affectedRows := uint64(bat.Vecs[0].Length())
	if insertArg.IsRemote {
		s3Writer.WriteEnd(proc)
	}
	atomic.AddUint64(&insertArg.Affected, affectedRows)
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

func getUniqueKeyInfo(tableDef *pb.TableDef) (map[string]int, int) {
	nameToPos := make(map[string]int)
	pkPos := -1
	pos := 0
	for j, col := range tableDef.Cols {
		// Check whether the composite primary key column is included
		if (tableDef.Pkey == nil || tableDef.Pkey.CompPkeyCol == nil) && col.Name != catalog.Row_ID && col.Primary {
			pkPos = j
		}
		if col.Name != catalog.Row_ID {
			nameToPos[col.Name] = pos
			pos++
		}
	}
	// Check whether the composite primary key column is included
	if tableDef.Pkey != nil && tableDef.Pkey.CompPkeyCol != nil {
		pkPos = pos
	}
	return nameToPos, pkPos
}
