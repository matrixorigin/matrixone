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
	"io"
	"sync"
	"sync/atomic"

	"github.com/reusee/dscope"
)

type (
	StartNode func(id NodeID) (Node, error)
	Do        func(threadID int64, action Action) error
)

func (_ Def) ExecuteFuncs() (
	start StartNode,
	do Do,
) {
	panic(fmt.Errorf("fixme: provide %T %T", start, do))
}

type NextThreadID func() int64

func (_ Def) NextThreadID() NextThreadID {
	var n int64
	return func() int64 {
		return atomic.AddInt64(&n, 1)
	}
}

type Execute func() error

func (_ Def) Execute(
	start StartNode,
	numNodes NumNodes,
	mainAction MainAction,
	ops Operators,
	doAction doAction,
	scope dscope.Scope,
	getReports GetReports,
	nextThreadID NextThreadID,
) Execute {
	return func() (err error) {
		defer he(&err)

		defer func() {
			ce(ops.parallelDo(scope, func(op Operator) any {
				return op.Finally
			}))
		}()

		ce(ops.parallelDo(scope, func(op Operator) any {
			return op.BeforeStart
		}))

		var nodes []Node
		for i := NumNodes(0); i < numNodes; i++ {
			node, err := start(NodeID(i))
			ce(err)
			nodes = append(nodes, node)
		}

		ce(ops.parallelDo(scope, func(op Operator) any {
			return op.BeforeDo
		}))

		ce(doAction(nextThreadID(), mainAction.Action))

		ce(ops.parallelDo(scope, func(op Operator) any {
			return op.AfterDo
		}))

		for _, node := range nodes {
			if closer, ok := node.(io.Closer); ok {
				ce(closer.Close())
			}
		}

		ce(ops.parallelDo(scope, func(op Operator) any {
			return op.AfterStop
		}))

		reports := getReports()
		for _, report := range reports {
			pt("%s\n", report)
		}

		ce(ops.parallelDo(scope, func(op Operator) any {
			return op.AfterReport
		}))

		if len(reports) > 0 {
			return fmt.Errorf("failure reported")
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
					return err
				}
			}

		case ParallelAction:
			// parallel action
			wg := new(sync.WaitGroup)
			errCh := make(chan error, 1)
			for _, action := range action.Actions {
				select {
				case err := <-errCh:
					return err
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
				return err
			default:
			}

		default:
			// send to target
			if err := do(threadID, action); err != nil {
				return err
			}

		}

		return nil
	}

	return
}
