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
	"sync"

	"github.com/reusee/dscope"
)

type (
	Start func() error
	Stop  func() error
	Do    func(action Action) error
)

func (_ Def) DumbExecuteFuncs() (
	_ Start,
	_ Stop,
	_ Do,
) {
	panic("fixme: provide Start, Stop, Do")
}

type Execute func() error

func (_ Def) Execute(
	start Start,
	stop Stop,
	do Do,
	mainAction MainAction,
	ops Operators,
	doAction doAction,
	scope dscope.Scope,
) Execute {
	return func() (err error) {
		defer he(&err)

		defer func() {
			for _, op := range ops {
				if op.Finally != nil {
					scope.Call(op.Finally)
				}
			}
		}()

		for _, op := range ops {
			if op.BeforeStart != nil {
				scope.Call(op.BeforeStart)
			}
		}

		if start == nil {
			panic("Start not provided")
		}
		ce(start())

		for _, op := range ops {
			if op.BeforeDo != nil {
				scope.Call(op.BeforeDo)
			}
		}

		if do == nil {
			panic("Do not provided")
		}
		ce(doAction(mainAction.Action))

		for _, op := range ops {
			if op.AfterDo != nil {
				scope.Call(op.AfterDo)
			}
		}

		if stop == nil {
			panic("Stop not provided")
		}
		ce(stop())

		for _, op := range ops {
			if op.AfterStop != nil {
				scope.Call(op.AfterStop)
			}
		}

		return
	}
}

type doAction func(action Action) error

func (_ Def) DoAction(
	do Do,
) (
	doAction doAction,
) {

	doAction = func(action Action) error {
		switch action := action.(type) {

		case SequentialAction:
			// sequential action
			for _, action := range action.Actions {
				if err := doAction(action); err != nil {
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
					err := doAction(action)
					if err != nil {
						select {
						case errCh <- err:
						default:
						}
					}
				}()
			}
			wg.Wait()

		default:
			// send to target
			if err := do(action); err != nil {
				return err
			}

		}
		return nil
	}

	return
}
