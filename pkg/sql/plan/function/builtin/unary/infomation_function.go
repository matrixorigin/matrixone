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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

/*
These functions get information from the session and system status.
*/

func adapter(vectors []*vector.Vector,
	proc *process.Process,
	resultType types.Type,
	parameterCount int,
	evaluateMemoryCapacityForJobFunc func(proc *process.Process, params ...interface{}) (int, error),
	jobFunc func(proc *process.Process, params ...interface{}) (interface{}, error),
) (*vector.Vector, error) {
	if parameterCount == 0 {
		//XXX step 1 is not needed now.
		//step 1: evaluate the capacity for result vector

		//step 2: allocate the memory for the result
		var svals []string
		var uvals []uint64
		var resultSpace interface{}
		if resultType.IsString() {
			svals = make([]string, 1)
			resultSpace = svals
		} else {
			uvals = make([]uint64, 1)
			resultSpace = uvals
		}
		//step 3: evaluate the function and get the result
		result, err := jobFunc(proc, resultSpace)
		if err != nil {
			return nil, err
		}
		//step 4: fill the result vector
		if result == nil {
			return vector.NewConstNull(resultType, 1), nil
		} else if resultType.IsString() {
			return vector.NewConstString(resultType, 1, svals[0], proc.Mp()), nil
		} else {
			return vector.NewConstFixed(resultType, 1, uvals[0], proc.Mp()), nil
		}
	}
	return nil, moerr.NewInternalError(proc.Ctx, "the parameter is invalid")
}

func evaluateMemoryCapacityForDatabase(proc *process.Process, params ...interface{}) (int, error) {
	return len(proc.SessionInfo.GetDatabase()), nil
}

func doDatabase(proc *process.Process, params ...interface{}) (interface{}, error) {
	result := params[0].([]string)
	result[0] = proc.SessionInfo.GetDatabase()
	if len(result[0]) == 0 {
		return nil, nil
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
	result := params[0].([]string)
	result[0] = proc.SessionInfo.GetUserHost()
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
	result := params[0].([]string)
	result[0] = proc.SessionInfo.GetCharset()
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
	result := params[0].([]string)
	role := proc.SessionInfo.GetRole()
	if len(role) == 0 {
		return nil, nil
	}
	result[0] = role
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
			result := params[0].([]string)
			result[0] = ""
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
			result[0] = proc.SessionInfo.LastInsertID
			return result, nil
		},
	)
}

func LastQueryIDWithoutParam(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return adapter(vectors, proc, types.T_varchar.ToType(), 0,
		func(proc *process.Process, params ...interface{}) (int, error) {
			return 0, nil
		},
		func(proc *process.Process, params ...interface{}) (interface{}, error) {
			cnt := int64(len(proc.SessionInfo.QueryId))
			if cnt == 0 {
				return nil, nil
			}
			result := params[0].([]string)
			// LAST_QUERY_ID(-1) returns the most recently-executed query (equivalent to LAST_QUERY_ID()).
			idx, err := makeQueryIdIdx(-1, cnt, proc)
			if err != nil {
				return nil, err
			}
			result[0] = proc.SessionInfo.QueryId[idx]
			return result, nil
		},
	)
}

func LastQueryID(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return adapter(vectors, proc, types.T_varchar.ToType(), 0,
		func(proc *process.Process, params ...interface{}) (int, error) {
			return 0, nil
		},
		func(proc *process.Process, params ...interface{}) (interface{}, error) {
			cnt := int64(len(proc.SessionInfo.QueryId))
			if cnt == 0 {
				return nil, nil
			}
			result := params[0].([]string)
			loc := vector.MustTCols[int64](vectors[0])[0]
			idx, err := makeQueryIdIdx(loc, cnt, proc)
			if err != nil {
				return nil, err
			}
			result[0] = proc.SessionInfo.QueryId[idx]
			return result, nil
		},
	)
}

func makeQueryIdIdx(loc, cnt int64, proc *process.Process) (int, error) {
	// https://docs.snowflake.com/en/sql-reference/functions/last_query_id.html
	var idx int
	if loc < 0 {
		if loc < -cnt {
			return 0, moerr.NewInvalidInput(proc.Ctx, "index out of range: %d", loc)
		}
		idx = int(loc + cnt)
	} else {
		if loc > cnt {
			return 0, moerr.NewInvalidInput(proc.Ctx, "index out of range: %d", loc)
		}
		idx = int(loc)
	}
	return idx, nil
}

// RolesGraphml returns a GraphML document representing memory role subgraphs
func RolesGraphml(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return adapter(vectors, proc, types.T_varchar.ToType(), 0,
		func(proc *process.Process, params ...interface{}) (int, error) {
			return 0, nil
		},
		func(proc *process.Process, params ...interface{}) (interface{}, error) {
			result := params[0].([]string)
			result[0] = ""
			return result, nil
		},
	)
}

// RowCount returns The number of rows updated
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

// Return a string that indicates the MySQL server version
func Version(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return adapter(vectors, proc, types.T_varchar.ToType(), 0,
		func(proc *process.Process, params ...interface{}) (int, error) {
			return len(proc.SessionInfo.GetVersion()), nil
		},
		func(proc *process.Process, params ...interface{}) (interface{}, error) {
			result := params[0].([]string)
			result[0] = proc.SessionInfo.GetVersion()
			return result, nil
		},
	)
}

// Collation	returns the collation of the string argument
func Collation(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return adapter(vectors, proc, types.T_varchar.ToType(), 0,
		func(proc *process.Process, params ...interface{}) (int, error) {
			return len(proc.SessionInfo.GetCollation()), nil
		},
		func(proc *process.Process, params ...interface{}) (interface{}, error) {
			result := params[0].([]string)
			result[0] = proc.SessionInfo.GetCollation()
			return result, nil
		},
	)
}
