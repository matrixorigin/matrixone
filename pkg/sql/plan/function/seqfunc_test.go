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
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
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

func TestCurrvalResolvesDatabaseOncePerBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	txn := mock_frontend.NewMockTxnOperator(ctrl)

	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.InitSeq()
	proc.Base.TxnOperator = txn
	proc.Base.SessionInfo.Database = "session_db"
	proc.Base.SessionInfo.SeqCurValues[1] = "42"
	proc.Ctx = context.WithValue(proc.Ctx, defines.EngineKey{}, eng)

	eng.EXPECT().Database(gomock.Any(), "session_db", txn).Return(db, nil).Times(1)
	db.EXPECT().Relation(gomock.Any(), "seq", nil).Return(rel, nil).Times(4)
	rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).Times(4)

	input, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("seq"), 4, proc.Mp())
	require.NoError(t, err)
	defer input.Free(proc.Mp())
	result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())
	defer result.Free()

	require.NoError(t, Currval([]*vector.Vector{input}, result, proc, 4, nil))
	for i := 0; i < 4; i++ {
		require.Equal(t, []byte("42"), result.GetResultVector().GetBytesAt(i))
	}
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
