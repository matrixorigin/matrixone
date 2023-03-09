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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString("insert")
}

func Prepare(_ *process.Process, arg any) error {
	ap := arg.(*Argument)
	if ap.IsRemote {
		ap.Container = colexec.NewWriteS3Container(ap.InsertCtx.TableDef)
	}
	return nil
}

func Call(idx int, proc *process.Process, arg any, _ bool, _ bool) (bool, error) {
	defer analyze(proc, idx)()

	insertArg := arg.(*Argument)
	container := insertArg.Container
	bat := proc.Reg.InputBatch
	if bat == nil {
		if insertArg.IsRemote {
			// handle the last Batch that batchSize less than DefaultBlockMaxRows
			// for more info, refer to the comments about reSizeBatch
			if err := container.WriteS3CacheBatch(proc); err != nil {
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
	affectedRows, err := colexec.InsertBatch(container, proc, bat, insertCtx.Source,
		insertCtx.TableDef, insertCtx.UniqueSource)
	if err != nil {
		return false, err
	}
	if insertArg.IsRemote {
		container.WriteEnd(proc)
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
