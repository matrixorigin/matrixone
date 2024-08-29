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

//package connector2
//
//import (
//	"bytes"
//	"github.com/matrixorigin/matrixone/pkg/vm"
//	"github.com/matrixorigin/matrixone/pkg/vm/process"
//)
//
//func (dispatch *SimpleDispatch) String(buf *bytes.Buffer) {
//	buf.WriteString(thisOperatorName)
//}
//
//func (dispatch *SimpleDispatch) Prepare(_ *process.Process) error {
//	return nil
//}
//
//func (dispatch *SimpleDispatch) Call(proc *process.Process) (vm.CallResult, error) {
//	if err, isCancel := vm.CancelCheck(proc); isCancel {
//		return vm.CancelResult, err
//	}
//
//	result, err := dispatch.Children[0].Call(proc)
//	if err != nil {
//		return result, err
//	}
//
//	switch {
//	case result.Batch == nil:
//		result.Status = vm.ExecStop
//
//	case result.Batch.IsEmpty():
//
//	default:
//		pipelineDone, errSend := dispatch.next.SendBatch(proc.Ctx, 0, result.Batch, nil)
//		if errSend != nil {
//			return result, errSend
//		}
//		if pipelineDone {
//			result.Status = vm.ExecStop
//		}
//	}
//
//	return result, nil
//}
