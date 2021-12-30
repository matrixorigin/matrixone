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

type Operator struct {
	BeforeStart any
	BeforeDo    any
	AfterDo     any
	AfterStop   any
	AfterReport any
	Finally     any
}

type Operators []Operator

var _ dscope.Reducer = Operators{}

func (c Operators) IsReducer() {}

func (o Operators) parallelDo(scope dscope.Scope, getFn func(op Operator) any) error {
	wg := new(sync.WaitGroup)
	errCh := make(chan error, 1)
	for _, op := range o {
		fn := getFn(op)
		if fn != nil {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var err error
				defer func() {
					if err != nil {
						select {
						case errCh <- err:
						default:
						}
					}
				}()
				defer he(&err)
				scope.Call(fn)
			}()
		}
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
	}
	return nil
}
