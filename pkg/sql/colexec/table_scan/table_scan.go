// Copyright 2021-2023 Matrix Origin
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

package table_scan

import (
	"bytes"
	"time"

	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const argName = "table_scan"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": table_scan ")
}

func (arg *Argument) Prepare(proc *process.Process) (err error) {
	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	t := time.Now()
	anal := proc.GetAnalyze(arg.info.Idx, arg.info.ParallelIdx, arg.info.ParallelMajor)
	anal.Start()
	defer func() {
		anal.Stop()
		v2.TxnStatementScanDurationHistogram.Observe(time.Since(t).Seconds())
	}()

	result := vm.NewCallResult()
	//select {
	//case <-proc.Ctx.Done():
	//	result.Batch = nil
	//	result.Status = vm.ExecStop
	//	return result, proc.Ctx.Err()
	//default:
	//}
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	for {
		// read data from storage engine
		bat, err := arg.Reader.Read(proc.Ctx, arg.Attrs, nil, proc.Mp(), proc)
		if err != nil {
			result.Status = vm.ExecStop
			return result, err
		}

		if bat != nil {
			if bat.IsEmpty() {
				continue
			}

			bat.Cnt = 1
			anal.S3IOByte(bat)
			anal.Alloc(int64(bat.Size()))
		}

		if arg.buf != nil {
			proc.PutBatch(arg.buf)
		}
		arg.buf = bat
		bat = nil
		break
	}

	result.Batch = arg.buf
	return result, nil
}
