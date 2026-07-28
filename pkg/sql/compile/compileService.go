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

func markQueryRunning(
	c *Compile,
	txn txnClient.TxnOperator,
	rejectionAware bool,
) error {
	c.proc.SetBaseProcessRunningStatus(true)
	if txn == nil {
		c.runSqlToken = 0
		return nil
	}
	_, cancel := process.GetQueryCtxFromProc(c.proc)
	sqlText := c.originSQL
	if sqlText == "" {
		sqlText = c.sql
	}
	var (
		token uint64
		err   error
	)
	if rejectionAware {
		token, err = txnClient.TryEnterRunSqlWithTokenAndSQL(txn, cancel, sqlText)
	} else {
		token = txn.EnterRunSqlWithTokenAndSQL(cancel, sqlText)
	}
	if err != nil {
		if cancel != nil {
			cancel()
		}
		c.runSqlToken = 0
		c.proc.SetBaseProcessRunningStatus(false)
		return err
	}
	c.runSqlToken = token
	return nil
}

// MarkQueryRunning preserves the original, non-rejecting API contract. Callers
// that need an admission error should use TryMarkQueryRunning.
func MarkQueryRunning(c *Compile, txn txnClient.TxnOperator) {
	_ = markQueryRunning(c, txn, false)
}

// TryMarkQueryRunning marks a query as running and reports SQL admission
// rejection without publishing partial running state.
func TryMarkQueryRunning(c *Compile, txn txnClient.TxnOperator) error {
	return markQueryRunning(c, txn, true)
}

func MarkQueryDone(c *Compile, txn txnClient.TxnOperator) {
	c.proc.SetBaseProcessRunningStatus(false)
	if txn == nil {
		c.runSqlToken = 0
		return
	}
	token := c.runSqlToken
	c.runSqlToken = 0
	txn.ExitRunSqlWithToken(token)
}
