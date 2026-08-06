// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestSelectIntoUserVariablesCapturesAndAssignsOneRow(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	ses := &Session{userDefinedVars: make(map[string]*UserDefinedVar)}
	collector := newSelectIntoUserVariables([]*tree.VarExpr{{Name: "out"}})

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(5), false, mp))
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	require.NoError(t, collector.capture(ctx, ses, bat))
	require.NoError(t, collector.apply(ctx, ses, "select abs(-5) into @out"))
	variable, err := ses.GetUserDefinedVar("OUT")
	require.NoError(t, err)
	require.Equal(t, int64(5), variable.Value)
	require.Equal(t, "select abs(-5) into @out", variable.Sql)
}

func TestSelectIntoUserVariablesZeroOrManyRowsDoNotAssign(t *testing.T) {
	ctx := context.Background()
	ses := &Session{userDefinedVars: make(map[string]*UserDefinedVar)}
	require.NoError(t, ses.SetUserDefinedVar("out", "old", "set @out = 'old'"))

	zeroRows := newSelectIntoUserVariables([]*tree.VarExpr{{Name: "out"}})
	require.NoError(t, zeroRows.apply(ctx, ses, "select value into @out from empty_table"))
	variable, err := ses.GetUserDefinedVar("out")
	require.NoError(t, err)
	require.Equal(t, "old", variable.Value)

	manyRows := newSelectIntoUserVariables([]*tree.VarExpr{{Name: "out"}})
	manyRows.row = []any{"new"}
	manyRows.rowCount = 2
	err = manyRows.apply(ctx, ses, "select value into @out from two_rows")
	require.ErrorContains(t, err, "Result consisted of more than one row")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTooManyRows))
	variable, err = ses.GetUserDefinedVar("out")
	require.NoError(t, err)
	require.Equal(t, "old", variable.Value)
}
