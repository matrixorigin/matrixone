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

package apply

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "apply"

func (apply *Apply) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	switch apply.ApplyType {
	case CROSS:
		buf.WriteString(": cross apply ")
	case OUTER:
		buf.WriteString(": outer apply ")
	}
}

func (apply *Apply) OpType() vm.OpType {
	return vm.Anti
}

func (apply *Apply) Prepare(proc *process.Process) (err error) {
	return nil
}

func (apply *Apply) Call(proc *process.Process) (vm.CallResult, error) {
	ctr := &apply.ctr
	result := vm.NewCallResult()
	ctr.batchRowCount = 0
	ctr.rbat = batch.NewWithSize(len(apply.Result))
	return result, moerr.NewInternalErrorNoCtx("apply call")
}
