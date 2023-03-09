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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString("insert")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	if ap.IsRemote {
		container := colexec.NewWriteS3Container(ap.InsertCtx.TableDef)
		ap.Container = container
	}
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	var err error
	var affectedRows uint64
	t1 := time.Now()
	insertArg := arg.(*Argument)
	bat := proc.Reg.InputBatch
	if bat == nil {
		if insertArg.IsRemote {
			// handle the last Batch that batchSize less than DefaultBlockMaxRows
			// for more info, refer to the comments about reSizeBatch
			err = insertArg.Container.WriteS3CacheBatch(proc)
			if err != nil {
				return false, err
			}
		}
		return true, nil
	}
	if bat.Length() == 0 {
		return false, nil
	}

	insertCtx := insertArg.InsertCtx

	var insertBat *batch.Batch
	defer func() {
		bat.Clean(proc.Mp())
		if insertBat != nil {
			insertBat.Clean(proc.Mp())
		}
		anal := proc.GetAnalyze(idx)
		anal.AddInsertTime(t1)
	}()

	insertRows := func() error {
		var affectedRow uint64

		affectedRow, err = colexec.InsertBatch(insertArg.Container, insertArg.Engine, proc, bat, insertCtx.Source,
			insertCtx.Ref, insertCtx.TableDef, insertCtx.ParentIdx, insertCtx.UniqueSource)
		if err != nil {
			return err
		}

		affectedRows = affectedRows + affectedRow
		return nil
	}

	if err := insertRows(); err != nil {
		return false, err
	}
	if insertArg.IsRemote {
		insertArg.Container.WriteEnd(proc)
	}
	atomic.AddUint64(&insertArg.Affected, affectedRows)
	return false, nil
}
