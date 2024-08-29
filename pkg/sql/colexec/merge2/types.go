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

package merge2
//
//import (
//	"github.com/matrixorigin/matrixone/pkg/common/reuse"
//	"github.com/matrixorigin/matrixone/pkg/sql/colexec/pipelineSpool"
//	"github.com/matrixorigin/matrixone/pkg/vm"
//	"github.com/matrixorigin/matrixone/pkg/vm/process"
//)
//
//const thisOperatorName = "merge"
//
//type Merge struct {
//	vm.OperatorBase
//	SinkScan bool
//
//	dataEntry pipelineSpool.ReceiverFromLocalPipeline
//}
//
//func NewArgument() *Merge {
//	return reuse.Alloc[Merge](nil)
//}
//
//func (merge *Merge) Reset(_ *process.Process, _ bool, _ error) {
//	merge.dataEntry.Reset()
//}
//
//func (merge *Merge) Free(_ *process.Process, _ bool, _ error) {
//	merge.dataEntry.Close()
//}
//
//// the following methods TypeName to implement the reuse.ReusableObject interface.
//func (merge Merge) TypeName() string {
//	return thisOperatorName
//}
//
//// the following methods OpType, GetOperatorBase, Release to implement the vm.Operator interface.
//func (merge *Merge) OpType() vm.OpType {
//	return vm.Merge
//}
//
//func (merge *Merge) GetOperatorBase() *vm.OperatorBase {
//	return &merge.OperatorBase
//}
//
//func (merge *Merge) Release() {
//	if merge != nil {
//		reuse.Free[Merge](merge, nil)
//	}
//}
//
//func init() {
//	reuse.CreatePool[Merge](
//		func() *Merge {
//			return &Merge{}
//		},
//		func(a *Merge) {
//			*a = Merge{}
//		},
//		reuse.DefaultOptions[Merge]().
//			WithEnableChecker(),
//	)
//}
//
//func (merge *Merge) WithSinkScan(sinkScan bool) *Merge {
//	merge.SinkScan = sinkScan
//	return merge
//}
//
//func (merge *Merge) SetDataEntry(dataEntry pipelineSpool.ReceiverFromLocalPipeline) {
//	merge.dataEntry = dataEntry
//}
