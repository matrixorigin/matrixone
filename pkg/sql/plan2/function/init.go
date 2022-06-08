package function

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"

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

	initLevelUpRules()
}

var registerMutex sync.RWMutex

func initRelatedStructure() {
	functionRegister = make([][]Function, FUNCTION_END_NUMBER)
}

func initLevelUpRules() {
	base := types.T_tuple + 10
	levelUp = make([][]int, base)
	for i := range levelUp {
		levelUp[i] = make([]int, base)
		for j := range levelUp[i] {
			levelUp[i][j] = upFailed
		}
	}
	// convert map levelUpRules to be a group
	for i := range levelUp {
		levelUp[i][i] = 0
	}
	for k, v := range levelUpRules {
		for i, t := range v {
			levelUp[k][t] = i + 1
		}
	}
}

// appendFunction is a method only used at init-functions to add a new function into supported-function list.
// Ensure that no duplicate functions will be added.
func appendFunction(fid int, newFunction Function) error {
	if err := completenessCheck(fid, newFunction); err != nil {
		return err
	}

	fs := functionRegister[fid]

	requiredIndex := len(fs)
	if int(newFunction.Index) != requiredIndex {
		return errors.New(errno.InvalidFunctionDefinition, fmt.Sprintf("function (fid = %d, index = %d)'s index should be %d", fid, newFunction.Index, requiredIndex))
	}

	for _, f := range fs {
		if functionsEqual(f, newFunction) {
			return errors.New(errno.DuplicateFunction, fmt.Sprintf("conflict happens, duplicate function (fid = %d, index = %d)", fid, newFunction.Index))
		}
	}

	registerMutex.Lock()
	functionRegister[fid] = append(functionRegister[fid], newFunction)
	registerMutex.Unlock()
	return nil
}

func completenessCheck(id int, f Function) error {
	if id < 0 || id >= FUNCTION_END_NUMBER {
		return errors.New(errno.InvalidFunctionDefinition, fmt.Sprintf("illegal function id %d", id))
	}
	return nil // just jump it now.
	// if f.Fn == nil && !f.IsAggregate() {
	//	return errors.New(errno.InvalidFunctionDefinition, fmt.Sprintf("function %d missing its's Fn", id))
	//}
	// if f.TypeCheckFn == nil {
	//	return errors.New(errno.InvalidFunctionDefinition, fmt.Sprintf("function %d missing its's type check function", id))
	//}
	// return nil
}

func functionsEqual(f1 Function, f2 Function) bool {
	if reflect.DeepEqual(f1.Args, f2.Args) && reflect.DeepEqual(f1.ReturnTyp, f2.ReturnTyp) {
		tc1 := reflect.ValueOf(f1.TypeCheckFn)
		tc2 := reflect.ValueOf(f2.TypeCheckFn)

		return tc1.Pointer() == tc2.Pointer()
	}
	return false
}
