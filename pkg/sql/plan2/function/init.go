package function

import (
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
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
		requiredIndex := len(functionRegister[fid].Overloads)
		if int(newFunction.Index) != requiredIndex {
			return errors.New(errno.InvalidFunctionDefinition, fmt.Sprintf("function (fid = %d, index = %d)'s index should be %d", fid, newFunction.Index, requiredIndex))
		}
		functionRegister[fid].Overloads = append(functionRegister[fid].Overloads, newFunction)
	}
	return nil
}
