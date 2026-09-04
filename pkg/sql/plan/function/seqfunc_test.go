// Copyright 2026 Matrix Origin
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

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestSequenceDatabase(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	database, null := sequenceDatabase("session_db", nil, 0)
	require.Equal(t, "session_db", database)
	require.False(t, null)

	storedDatabase, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("view_db"), 1, proc.Mp())
	require.NoError(t, err)
	defer storedDatabase.Free(proc.Mp())
	database, null = sequenceDatabase("session_db", vector.GenerateFunctionStrParameter(storedDatabase), 0)
	require.Equal(t, "view_db", database)
	require.False(t, null)

	nullDatabase := vector.NewConstNull(types.T_varchar.ToType(), 1, proc.Mp())
	defer nullDatabase.Free(proc.Mp())
	database, null = sequenceDatabase("session_db", vector.GenerateFunctionStrParameter(nullDatabase), 0)
	require.Empty(t, database)
	require.True(t, null)
}

func TestSequenceHiddenOverloadExecutors(t *testing.T) {
	for _, test := range []struct {
		name       string
		functionID int
		overloadID int
		args       []types.T
	}{
		{name: "nextval", functionID: NEXTVAL, overloadID: 1, args: []types.T{types.T_varchar, types.T_varchar}},
		{name: "setval", functionID: SETVAL, overloadID: 2, args: []types.T{types.T_varchar, types.T_varchar, types.T_bool, types.T_varchar}},
		{name: "currval", functionID: CURRVAL, overloadID: 1, args: []types.T{types.T_varchar, types.T_varchar}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fn := allSupportedFunctions[test.functionID]
			require.Equal(t, test.functionID, fn.functionId)
			require.Greater(t, len(fn.Overloads), test.overloadID)
			overload := fn.Overloads[test.overloadID]
			require.Equal(t, test.args, overload.args)
			require.NotNil(t, overload.newOp())
		})
	}
}
