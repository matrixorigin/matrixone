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
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	txnClient "github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"time"
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
	// lch is lock for the service.
	// we use channel but not mutex to prevent users' cannot stop his query when the service is paused.
	lch chan struct{}

	// ongoing compiles with additional information.
	aliveCompiles map[*Compile]compileAdditionalInformation
}

// compileAdditionalInformation holds additional information for one compile.
// to help control one compile.
type compileAdditionalInformation struct {
	// mustReturnError holds an error that must be returned if set.
	mustReturnError error

	// queryCancel is a method to cancel an ongoing query.
	queryCancel context.CancelFunc
	// queryDone is a waiter that checks if this query has been completed or not.
	queryDone queryDoneWaiter
}

// kill one query and block until it was completed.
func (info *compileAdditionalInformation) kill(errResult error) {
	info.queryCancel()
	info.queryDone.checkCompleted()
	info.mustReturnError = errResult
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
		lch:           make(chan struct{}, 1),
		aliveCompiles: make(map[*Compile]compileAdditionalInformation, 1024),
	}
	srv.lch <- struct{}{}
	return srv
}

func (srv *ServiceOfCompile) getCompile(proc *process.Process) *Compile {
	runningCompile := reuse.Alloc[Compile](nil)
	// runningCompile.AllocMsg = time.Now().String() + " : " + string(debug.Stack())
	runningCompile.proc = proc
	return runningCompile
}

func (srv *ServiceOfCompile) recordRunningCompile(runningCompile *Compile) error {
	if runningCompile.queryStatus == nil {
		runningCompile.queryStatus = newQueryDoneWaiter()
	} else {
		runningCompile.queryStatus.clear()
	}

	queryCtx, queryCancel := process.GetQueryCtxFromProc(runningCompile.proc)

	select {
	case <-srv.lch:
		runningCompile.proc.SetBaseProcessRunningStatus(true)
		srv.aliveCompiles[runningCompile] = compileAdditionalInformation{
			mustReturnError: nil,
			queryCancel:     queryCancel,
			queryDone:       runningCompile.queryStatus,
		}
		srv.lch <- struct{}{}
		return nil

	case <-queryCtx.Done():
		return queryCtx.Err()
	}
}

func (srv *ServiceOfCompile) removeRunningCompile(c *Compile) (mustReturnError bool, err error) {
	c.queryStatus.noticeQueryCompleted()
	c.proc.SetBaseProcessRunningStatus(false)

	<-srv.lch
	if item, ok := srv.aliveCompiles[c]; ok {
		err = item.mustReturnError
	}
	delete(srv.aliveCompiles, c)
	c.queryStatus.clear()
	srv.lch <- struct{}{}

	return err != nil, err
}

func (srv *ServiceOfCompile) putCompile(c *Compile) {
	if !c.isPrepare {
		// c.FreeMsg = time.Now().String() + " : " + string(debug.Stack())
		reuse.Free[Compile](c, nil)
	}
}

func (srv *ServiceOfCompile) aliveCompile() int {
	<-srv.lch
	l := len(srv.aliveCompiles)
	srv.lch <- struct{}{}
	return l
}

func (srv *ServiceOfCompile) PauseService() {
	<-srv.lch
}

func (srv *ServiceOfCompile) ResumeService() {
	srv.lch <- struct{}{}
}

func (srv *ServiceOfCompile) KillAllQueriesWithError(err error) {
	logutil.Infof("compile service starts to kill all running queries.")
	start := time.Now()
	defer func() {
		logutil.Infof("compile service has killed all running queries, time cost: %.2f s", time.Since(start).Seconds())
	}()

	for _, v := range srv.aliveCompiles {
		v.kill(err)
	}
}
