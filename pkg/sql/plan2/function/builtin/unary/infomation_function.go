// Copyright 2022 Matrix Origin
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

package unary

import (
	"errors"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	errorParameterIsInvalid = errors.New("the parameter is invalid")
)

/*
These functions get information from the session and system status.
*/

//
func adapter(vectors []*vector.Vector,
	proc *process.Process,
	resultType types.Type,
	parameterCount int,
	evaluateMemoryCapacityForJobFunc func(proc *process.Process, params ...interface{}) (int, error),
	jobFunc func(proc *process.Process, params ...interface{}) (interface{}, error),
) (*vector.Vector, error) {
	if parameterCount == 0 {
		//step 1: evaluate the capacity for result vector
		capacity, err := evaluateMemoryCapacityForJobFunc(proc)
		if err != nil {
			return nil, err
		}
		//step 2: allocate the memory for the result
		var resultSpace interface{}
		switch resultType.Oid {
		case types.T_varchar, types.T_char:
			resultSpace = &types.Bytes{
				Data:    make([]byte, capacity),
				Offsets: make([]uint32, 1),
				Lengths: make([]uint32, 1),
			}
		case types.T_uint64:
			resultSpace = make([]uint64, 1)
		}
		//step 3: evaluate the function and get the result
		result, err := jobFunc(proc, resultSpace)
		if err != nil {
			return nil, err
		}
		//step 4: fill the result vector
		resultVector := vector.NewConst(resultType)
		if result == nil {
			nulls.Add(resultVector.Nsp, 0)
		} else {
			vector.SetCol(resultVector, result)
		}
		return resultVector, nil
	}
	return nil, errorParameterIsInvalid
}

func fillString(str string, dst *types.Bytes) error {
	if dst == nil ||
		dst.Data == nil ||
		dst.Offsets == nil ||
		dst.Lengths == nil ||
		len(dst.Offsets) != len(dst.Lengths) {
		return errorParameterIsInvalid
	}
	l := len(dst.Data)
	copy(dst.Data, []byte(str)[:l])
	dst.Offsets[0] = 0
	dst.Lengths[0] = uint32(l)
	return nil
}

func evaluateMemoryCapacityForDatabase(proc *process.Process, params ...interface{}) (int, error) {
	return len(proc.SessionInfo.GetDatabase()), nil
}

func doDatabase(proc *process.Process, params ...interface{}) (interface{}, error) {
	result := params[0].(*types.Bytes)
	dbName := proc.SessionInfo.GetDatabase()
	if len(dbName) == 0 {
		return nil, nil
	}
	err := fillString(dbName, result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Database returns the default (current) database name
func Database(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return adapter(vectors, proc, types.T_varchar.ToType(),
		0,
		evaluateMemoryCapacityForDatabase,
		doDatabase)
}

func evaluateMemoryCapacityForUser(proc *process.Process, params ...interface{}) (int, error) {
	return len(proc.SessionInfo.GetUserHost()), nil
}

func doUser(proc *process.Process, params ...interface{}) (interface{}, error) {
	result := params[0].(*types.Bytes)
	err := fillString(proc.SessionInfo.GetUserHost(), result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// User returns the user name and host name provided by the client
func User(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return adapter(vectors, proc, types.T_varchar.ToType(),
		0,
		evaluateMemoryCapacityForUser,
		doUser)
}

func evaluateMemoryCapacityForConnectionID(proc *process.Process, params ...interface{}) (int, error) {
	return 8, nil
}

func doConnectionID(proc *process.Process, params ...interface{}) (interface{}, error) {
	result := params[0].([]uint64)
	result[0] = proc.SessionInfo.ConnectionID
	return result, nil
}

// ConnectionID returns the connection ID (thread ID) for the connection
func ConnectionID(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return adapter(vectors, proc, types.T_uint64.ToType(),
		0,
		evaluateMemoryCapacityForConnectionID,
		doConnectionID)
}

func evaluateMemoryCapacityForCharset(proc *process.Process, params ...interface{}) (int, error) {
	return len(proc.SessionInfo.GetCharset()), nil
}

func doCharset(proc *process.Process, params ...interface{}) (interface{}, error) {
	result := params[0].(*types.Bytes)
	err := fillString(proc.SessionInfo.GetCharset(), result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Charset returns the character set of the argument
func Charset(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	//TODO: even the charset has one parameter but
	//the result is utf8 always. So, we do not process the parameter
	return adapter(vectors, proc, types.T_varchar.ToType(),
		0,
		evaluateMemoryCapacityForCharset,
		doCharset)
}

func evaluateMemoryCapacityForCurrentRole(proc *process.Process, params ...interface{}) (int, error) {
	return len(proc.SessionInfo.GetRole()), nil
}

func doCurrentRole(proc *process.Process, params ...interface{}) (interface{}, error) {
	result := params[0].(*types.Bytes)
	role := proc.SessionInfo.GetRole()
	if len(role) == 0 {
		return nil, nil
	}
	err := fillString(role, result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Current_Role returns the current active roles
func CurrentRole(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return adapter(vectors, proc, types.T_varchar.ToType(),
		0,
		evaluateMemoryCapacityForCurrentRole,
		doCurrentRole)
}

func evaluateMemoryCapacityForFoundRows(proc *process.Process, params ...interface{}) (int, error) {
	return 8, nil
}

func doFoundRows(proc *process.Process, params ...interface{}) (interface{}, error) {
	result := params[0].([]uint64)
	result[0] = 0
	return result, nil
}

// For a SELECT with a LIMIT clause, the number of rows that would be returned were there no LIMIT clause
func FoundRows(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return adapter(vectors, proc, types.T_uint64.ToType(),
		0,
		evaluateMemoryCapacityForFoundRows,
		doFoundRows)
}

// ICU library version
func ICULIBVersion(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return adapter(vectors, proc, types.T_varchar.ToType(), 0,
		func(proc *process.Process, params ...interface{}) (int, error) {
			return 0, nil
		},
		func(proc *process.Process, params ...interface{}) (interface{}, error) {
			result := params[0].(*types.Bytes)
			err := fillString("", result)
			if err != nil {
				return nil, err
			}
			return result, nil
		},
	)
}

// Value of the AUTOINCREMENT column for the last INSERT
func LastInsertID(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return adapter(vectors, proc, types.T_uint64.ToType(), 0,
		func(proc *process.Process, params ...interface{}) (int, error) {
			return 0, nil
		},
		func(proc *process.Process, params ...interface{}) (interface{}, error) {
			result := params[0].([]uint64)
			result[0] = 0
			return result, nil
		},
	)
}

//RolesGraphml returns a GraphML document representing memory role subgraphs
func RolesGraphml(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return adapter(vectors, proc, types.T_varchar.ToType(), 0,
		func(proc *process.Process, params ...interface{}) (int, error) {
			return 0, nil
		},
		func(proc *process.Process, params ...interface{}) (interface{}, error) {
			result := params[0].(*types.Bytes)
			err := fillString("", result)
			if err != nil {
				return nil, err
			}
			return result, nil
		},
	)
}

//RowCount returns The number of rows updated
func RowCount(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return adapter(vectors, proc, types.T_uint64.ToType(), 0,
		func(proc *process.Process, params ...interface{}) (int, error) {
			return 0, nil
		},
		func(proc *process.Process, params ...interface{}) (interface{}, error) {
			result := params[0].([]uint64)
			result[0] = 0
			return result, nil
		},
	)
}

//	Return a string that indicates the MySQL server version
func Version(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return adapter(vectors, proc, types.T_varchar.ToType(), 0,
		func(proc *process.Process, params ...interface{}) (int, error) {
			return len(proc.SessionInfo.GetVersion()), nil
		},
		func(proc *process.Process, params ...interface{}) (interface{}, error) {
			result := params[0].(*types.Bytes)
			err := fillString(proc.SessionInfo.GetVersion(), result)
			if err != nil {
				return nil, err
			}
			return result, nil
		},
	)
}

//Collation	returns the collation of the string argument
func Collation(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return adapter(vectors, proc, types.T_varchar.ToType(), 0,
		func(proc *process.Process, params ...interface{}) (int, error) {
			return len(proc.SessionInfo.GetCollation()), nil
		},
		func(proc *process.Process, params ...interface{}) (interface{}, error) {
			result := params[0].(*types.Bytes)
			err := fillString(proc.SessionInfo.GetCollation(), result)
			if err != nil {
				return nil, err
			}
			return result, nil
		},
	)
}
