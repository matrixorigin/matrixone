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
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	txnClient "github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// todo: Move it to a CN level structure next day.
var compileService *ServiceOfCompile

func init() {
	compileService = InitCompileService()
	txnClient.SetRunningPipelineManagement(compileService)
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
func allocateNewCompile(proc *process.Process) *Compile {
	runningCompile := reuse.Alloc[Compile](nil)
	runningCompile.proc = proc
	return runningCompile
}

func releaseCompile(c *Compile) {
	if !c.isPrepare {
		reuse.Free[Compile](c, nil)
	}
}

// recordRunningCompile record a running query to the compileService.
func (srv *ServiceOfCompile) recordRunningCompile(runningCompile *Compile, txn txnClient.TxnOperator) {
	if runningCompile.queryStatus == nil {
		runningCompile.queryStatus = newQueryDoneWaiter()
	} else {
		runningCompile.queryStatus.clear()
	}

	runningCompile.proc.SetBaseProcessRunningStatus(true)
	_, queryCancel := process.GetQueryCtxFromProc(runningCompile.proc)

	srv.Lock()
	srv.aliveCompiles[runningCompile] = compileAdditionalInformation{
		queryCancel: queryCancel,
		queryDone:   runningCompile.queryStatus,
	}

	if txn != nil {
		txn.EnterRunSql()
	}
	srv.Unlock()
}

// thisQueryStillRunning return nil if this query is still in running.
// return error once query was canceled or txn was done.
func thisQueryStillRunning(proc *process.Process, txn txnClient.TxnOperator) error {
	if err := proc.GetQueryContextError(); err != nil {
		return err
	}
	if txn != nil && txn.Status() != pbtxn.TxnStatus_Active {
		return moerr.NewInternalError(proc.Ctx, "transaction is not active.")
	}
	return nil
}

// removeRunningCompile remove a running query from the compileService.
func (srv *ServiceOfCompile) removeRunningCompile(c *Compile, txn txnClient.TxnOperator) {
	if txn != nil {
		txn.ExitRunSql()
	}
	c.queryStatus.noticeQueryCompleted()
	c.proc.SetBaseProcessRunningStatus(false)

	srv.Lock()
	delete(srv.aliveCompiles, c)
	srv.Unlock()

	c.queryStatus.clear()
}

func (srv *ServiceOfCompile) aliveCompile() int {
	srv.Lock()
	l := len(srv.aliveCompiles)
	srv.Unlock()
	return l
}

func (srv *ServiceOfCompile) PauseService() {
	srv.Lock()
}

func (srv *ServiceOfCompile) ResumeService() {
	srv.Unlock()
}

// KillAllQueriesWithError cancel all the running queries' context to terminate all the running queries.
//
// this function will block until all record compiles were done.
func (srv *ServiceOfCompile) KillAllQueriesWithError() {
	logutil.Infof("compile service starts to kill all running queries.")
	start := time.Now()
	defer func() {
		logutil.Infof("compile service has killed all running queries, time cost: %.2f s", time.Since(start).Seconds())
	}()

	for _, v := range srv.aliveCompiles {
		v.kill()
	}
}
