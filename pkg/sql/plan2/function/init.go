package function

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"reflect"
)

func init() {
	initOperators()
	initBuiltIns()
	initAggregateFunction()
}

// appendFunction is a method only used at init-functions to add a new function into supported-function list.
// Ensure that no duplicate functions will be added.
func appendFunction(name string, newFunction Function) error {
	if newFunction.ID == undefined {
		return errors.New(errno.UndefinedObject, fmt.Sprintf("ID of function %s is undefined"))
	}

	if fs, ok := functionRegister[name]; ok {
		for _, f := range fs {
			if reflect.DeepEqual(f, newFunction) {
				return errors.New(errno.DuplicateFunction, fmt.Sprintf("duplicate function %s(%v)", name, f.Args))
			}
		}
	}

	functionRegister[name] = append(functionRegister[name], newFunction)
	return nil
}
