package function

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"reflect"
	"sync"
)

func init() {
	initOperators()
	initBuiltIns()
	initAggregateFunction()
}

var registerMutex sync.RWMutex

// appendFunction is a method only used at init-functions to add a new function into supported-function list.
// Ensure that no duplicate functions will be added.
func appendFunction(name string, newFunction Function) error {
	if err := completenessCheck(newFunction, name); err != nil {
		return err
	}

	if fs, ok := functionRegister[name]; ok {
		requiredIndex := len(fs)
		if newFunction.Index != requiredIndex {
			return errors.New(errno.InvalidFunctionDefinition, fmt.Sprintf("function %s(%v)'s index number is duplicate", name, newFunction.Args))
		}

		for _, f := range fs {
			if functionsEqual(f, newFunction) {
				return errors.New(errno.DuplicateFunction, fmt.Sprintf("conflict happens, duplicate function %s(%v)", name, f.Args))
			}
		}
	}

	registerMutex.Lock()
	functionRegister[name] = append(functionRegister[name], newFunction)
	registerMutex.Unlock()
	return nil
}

func completenessCheck(f Function, name string) error {
	if f.Fn == nil && !f.IsAggregate() {
		return errors.New(errno.InvalidFunctionDefinition, fmt.Sprintf("function '%s' missing its's Fn", name))
	}
	if f.TypeCheckFn == nil {
		return errors.New(errno.InvalidFunctionDefinition, fmt.Sprintf("function '%s' missing its's type check function", name))
	}
	return nil
}

func functionsEqual(f1 Function, f2 Function) bool {
	if reflect.DeepEqual(f1.Args, f2.Args) {
		tc1 := reflect.ValueOf(f1.TypeCheckFn)
		tc2 := reflect.ValueOf(f2.TypeCheckFn)

		if tc1.Pointer() == tc2.Pointer() {
			return true
		}
		return false
	}
	return false
}
