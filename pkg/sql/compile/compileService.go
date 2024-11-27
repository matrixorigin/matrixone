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
<<<<<<< HEAD
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"

=======
>>>>>>> 12023e16cc66a531162ae2c41d49d12f98a84099
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	txnClient "github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

<<<<<<< HEAD
// todo: Move it to a CN level structure next day.
var compileService *ServiceOfCompile

func init() {
	compileService = InitCompileService()
}

func GetCompileService() *ServiceOfCompile {
	return compileService
}

// ServiceOfCompile is used to manage the lifecycle of Compile structures,
// including their creation and deletion.
//
// It also tracks the currently active complies within a single CN.
type ServiceOfCompile struct {
	sync.Mutex

	// ongoing compiles with additional information.
	aliveCompiles map[*Compile]compileAdditionalInformation
}

// compileAdditionalInformation holds additional information for one compile.
// to help control one compile.
type compileAdditionalInformation struct {
	// queryCancel is a method to cancel an ongoing query.
	queryCancel context.CancelFunc
	// queryDone is a waiter that checks if this query has been completed or not.
	queryDone queryDoneWaiter
}

// kill one query and block until it was completed.
func (info *compileAdditionalInformation) kill() {
	info.queryCancel()
	info.queryDone.checkCompleted()
}

type queryDoneWaiter chan bool

func newQueryDoneWaiter() queryDoneWaiter {
	return make(chan bool, 1)
}

func (waiter queryDoneWaiter) noticeQueryCompleted() {
	waiter <- true
}

func (waiter queryDoneWaiter) checkCompleted() {
	<-waiter
	waiter <- true
}

func (waiter queryDoneWaiter) clear() {
	for len(waiter) > 0 {
		<-waiter
	}
}

func InitCompileService() *ServiceOfCompile {
	srv := &ServiceOfCompile{
		aliveCompiles: make(map[*Compile]compileAdditionalInformation, 1024),
	}
	return srv
}

// todo: maybe we can do the record action while allocating a new compile structure next day.
=======
>>>>>>> 12023e16cc66a531162ae2c41d49d12f98a84099
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
