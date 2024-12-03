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

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	txnClient "github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func allocateNewCompile(proc *process.Process) *Compile {
	runningCompile := reuse.Alloc[Compile](nil)
	runningCompile.proc = proc
	return runningCompile
}

func doCompileRelease(c *Compile) {
	if !c.isPrepare {
		reuse.Free[Compile](c, nil)
	}
}

func MarkQueryRunning(c *Compile, txn txnClient.TxnOperator) {
	c.proc.SetBaseProcessRunningStatus(true)
	if txn != nil {
		txn.EnterRunSql()
	}
}

func MarkQueryDone(c *Compile, txn txnClient.TxnOperator) {
	c.proc.SetBaseProcessRunningStatus(false)
	if txn != nil {
		txn.ExitRunSql()
	}
}
