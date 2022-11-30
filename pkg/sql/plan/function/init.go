// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// init function fills the functionRegister with
// aggregates,	see initAggregateFunction
// builtins,	see initBuiltIns
// operators,	see initOperators
func init() {
	initRelatedStructure()

	initOperators()
	initBuiltIns()
	initAggregateFunction()

	initTypeCheckRelated()
}

var registerMutex sync.RWMutex

func initRelatedStructure() {
	functionRegister = make([]Functions, FUNCTION_END_NUMBER)
}

// appendFunction is a method only used at init-functions to add a new function into supported-function list.
// Ensure that no duplicate functions will be added.
func appendFunction(fid int, newFunctions Functions) error {
	functionRegister[fid].TypeCheckFn = newFunctions.TypeCheckFn
	functionRegister[fid].Id = newFunctions.Id
	registerMutex.Lock()
	defer registerMutex.Unlock()
	for _, newFunction := range newFunctions.Overloads {
		newFunction.flag = newFunctions.Flag
		newFunction.layout = newFunctions.Layout

		requiredIndex := len(functionRegister[fid].Overloads)
		if int(newFunction.Index) != requiredIndex {
			return moerr.NewInternalErrorNoCtx("function (fid = %d, index = %d)'s index should be %d", fid, newFunction.Index, requiredIndex)
		}
		functionRegister[fid].Overloads = append(functionRegister[fid].Overloads, newFunction)
	}
	return nil
}
