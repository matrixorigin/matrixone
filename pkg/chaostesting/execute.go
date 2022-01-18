// Copyright 2021 Matrix Origin
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

package fz

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/reusee/dscope"
	"github.com/reusee/e4"
)

type (
	Nodes = []Node
	Do    func(threadID int64, action Action) error
)

func (_ Def) ExecuteFuncs() (
	nodes Nodes,
	do Do,
) {
	panic(fmt.Errorf("fixme: provide %T %T", nodes, do))
}

type NextThreadID func() int64

func (_ Def) NextThreadID() NextThreadID {
	var n int64
	return func() int64 {
		return atomic.AddInt64(&n, 1)
	}
}

type ExecuteTimeout time.Duration

func (_ Def) ExecuteTimeout() ExecuteTimeout {
	return ExecuteTimeout(time.Minute * 10)
}

type Execute func() error

func (_ Def) Execute(
	nodes Nodes,
	numNodes NumNodes,
	mainAction MainAction,
	ops Operators,
	doAction doAction,
	scope dscope.Scope,
	nextThreadID NextThreadID,
	closeNode CloseNode,
	processReports processReports,
	logger Logger,
	timeout ExecuteTimeout,
) Execute {

	return func() (err error) {
		done := make(chan struct{})
		go func() {
			defer func() {
				close(done)
			}()

			defer he(&err, e4.Do(func() {
				for i := range nodes {
					ce(closeNode(NodeID(i)))
				}
			}))

			// clean-ups
			defer func() {
				ce(ops.parallelDo(scope, func(op Operator) func() {
					return op.Finally
				}))
				logger.Info("Operator.Finally done")
			}()

			// action

			ce(ops.parallelDo(scope, func(op Operator) func() {
				return op.BeforeDo
			}))
			logger.Info("Operator.BeforeDo done")

			ce(doAction(nextThreadID(), mainAction.Action))

			ce(ops.parallelDo(scope, func(op Operator) func() {
				return op.AfterDo
			}))
			logger.Info("Operator.AfterDo done")

			// close nodes

			ce(ops.parallelDo(scope, func(op Operator) func() {
				return op.BeforeClose
			}))
			logger.Info("Operator.BeforeClose done")

			for i := range nodes {
				ce(closeNode(NodeID(i)))
			}

			ce(ops.parallelDo(scope, func(op Operator) func() {
				return op.AfterClose
			}))
			logger.Info("Operator.AfterClose done")

			// report
			ce(processReports())

		}()

		select {
		case <-done:
		case <-time.After(time.Duration(timeout)):
			return fmt.Errorf("execute timeout")
		}

		return
	}
}

type doAction func(threadID int64, action Action) error

func (_ Def) DoAction(
	do Do,
	nextThreadID NextThreadID,
) (
	doAction doAction,
) {

	doAction = func(threadID int64, action Action) error {
		switch action := action.(type) {

		case SequentialAction:
			// sequential action
			for _, action := range action.Actions {
				if err := doAction(threadID, action); err != nil {
					return we(err)
				}
			}

		case ParallelAction:
			// parallel action
			wg := new(sync.WaitGroup)
			errCh := make(chan error, 1)
			for _, action := range action.Actions {
				select {
				case err := <-errCh:
					return we(err)
				default:
				}
				action := action
				wg.Add(1)
				go func() {
					defer wg.Done()
					err := doAction(nextThreadID(), action)
					if err != nil {
						select {
						case errCh <- err:
						default:
						}
					}
				}()
			}
			wg.Wait()
			select {
			case err := <-errCh:
				return we(err)
			default:
			}

		default:
			// send to target
			if err := do(threadID, action); err != nil {
				return we(err)
			}

		}

		return nil
	}

	return
}
