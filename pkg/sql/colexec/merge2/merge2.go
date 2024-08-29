// Copyright 2024 Matrix Origin
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

//package merge2
//
//import (
//	"bytes"
//	"github.com/matrixorigin/matrixone/pkg/container/batch"
//	"github.com/matrixorigin/matrixone/pkg/vm"
//	"github.com/matrixorigin/matrixone/pkg/vm/process"
//)
//
//func (merge *Merge) String(buf *bytes.Buffer) {
//	buf.WriteString(thisOperatorName)
//}
//
//func (merge *Merge) Prepare(_ *process.Process) error {
//	return nil
//}
//
//func (merge *Merge) Call(proc *process.Process) (vm.CallResult, error) {
//	if err, isCancel := vm.CancelCheck(proc); isCancel {
//		return vm.CancelResult, err
//	}
//
//	anal := proc.GetAnalyze(merge.GetIdx(), merge.GetParallelIdx(), merge.GetParallelMajor())
//	anal.Start()
//	defer anal.Stop()
//
//	var bat *batch.Batch
//	var err error
//	for {
//		bat, err = merge.dataEntry.ReceiveBatch()
//		if err != nil {
//			return vm.CancelResult, err
//		}
//		if bat == nil {
//			return vm.CancelResult, nil
//		}
//
//		anal.Input(bat, merge.GetIsFirst())
//		if merge.SinkScan && bat.Last() {
//			continue
//		}
//		break
//	}
//	anal.Output(bat, merge.GetIsLast())
//
//	res := vm.NewCallResult()
//	res.Batch = bat
//	return res, nil
//}
