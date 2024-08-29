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

package connector2
//
//import (
//	"github.com/matrixorigin/matrixone/pkg/common/reuse"
//	"github.com/matrixorigin/matrixone/pkg/sql/colexec/pipelineSpool"
//	"github.com/matrixorigin/matrixone/pkg/vm"
//	"github.com/matrixorigin/matrixone/pkg/vm/process"
//)
//
//var _ vm.Operator = &SimpleDispatch{}
//
//const (
//	thisOperatorName = "connector"
//)
//
//// SimpleDispatch is an operator used for communication between two local pipelines,
//// responsible for sending batch data to the next pipeline.
//type SimpleDispatch struct {
//	vm.OperatorBase
//
//	// next is the data receiver of this dispatch operator.
//	next pipelineSpool.SenderToLocalPipeline
//}
//
//func NewArgument(sender pipelineSpool.SenderToLocalPipeline) *SimpleDispatch {
//	d := reuse.Alloc[SimpleDispatch](nil)
//	d.next = sender
//	return d
//}
//
//func (dispatch *SimpleDispatch) Reset(_ *process.Process, _ bool, _ error) {
//	dispatch.next.Reset()
//}
//
//func (dispatch *SimpleDispatch) Free(_ *process.Process, _ bool, _ error) {
//	dispatch.next.Close()
//}
//
//// the following methods TypeName to implement the reuse.ReusableObject interface.
//func (dispatch SimpleDispatch) TypeName() string {
//	return thisOperatorName
//}
//
//// the following methods OpType, GetOperatorBase, Release to implement the vm.Operator interface.
//func (dispatch *SimpleDispatch) OpType() vm.OpType {
//	return vm.Connector
//}
//
//func (dispatch *SimpleDispatch) GetOperatorBase() *vm.OperatorBase {
//	return &dispatch.OperatorBase
//}
//
//func (dispatch *SimpleDispatch) Release() {
//	// do nothing.
//	if dispatch != nil {
//		reuse.Free[SimpleDispatch](dispatch, nil)
//	}
//}
//
//func init() {
//	reuse.CreatePool[SimpleDispatch](
//		func() *SimpleDispatch { return &SimpleDispatch{} },
//		func(a *SimpleDispatch) {
//			*a = SimpleDispatch{}
//		},
//		reuse.DefaultOptions[SimpleDispatch]().WithEnableChecker())
//}
