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

package compile

import (
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestInternalStatementProfile(t *testing.T) {
	proc := testutil.NewProcess(t)
	parent := &process.StmtProfile{}
	proc.SetStmtProfile(parent)
	parent.SetStatementRuntimeProfile("Create Table", tree.QueryTypeDDL, false)
	for _, tc := range []struct {
		sql, statement, query string
		ignore                bool
	}{
		{"insert into t select 10/0", "Insert", tree.QueryTypeDML, false},
		{"insert ignore into t select 10/0", "Insert", tree.QueryTypeDML, true},
		{"insert into t values(1) on duplicate key update a=1", "Insert", tree.QueryTypeDML, false},
		{"replace into t select 10/0", "Replace", tree.QueryTypeDML, false},
		{"update t set a=10/0", "Update", tree.QueryTypeDML, false},
		{"update ignore t set a=10/0", "Update", tree.QueryTypeDML, true},
		{"load data infile 'x' ignore into table t", "Load", tree.QueryTypeDML, true},
		{"load data infile 'x' into table t ignore 1 lines", "Load", tree.QueryTypeDML, false},
		{"select 10/0", "Select", tree.QueryTypeDQL, false},
		{"create table t as select 10/0", "Create Table", tree.QueryTypeDDL, false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := mysql.ParseOne(t.Context(), tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			atomic.StoreInt32(&proc.Base.DivByZeroErrorMode, 1)
			initInternalStatementProfile(proc, stmt)
			st, qt, ignore := proc.GetStmtProfile().GetStatementRuntimeProfile()
			require.Equal(t, tc.statement, st)
			require.Equal(t, tc.query, qt)
			require.Equal(t, tc.ignore, ignore)
			require.Equal(t, int32(-1), atomic.LoadInt32(&proc.Base.DivByZeroErrorMode))
			require.NotSame(t, parent, proc.GetStmtProfile())
			st, qt, ignore = parent.GetStatementRuntimeProfile()
			require.Equal(t, "Create Table", st)
			require.Equal(t, tree.QueryTypeDDL, qt)
			require.False(t, ignore)
		})
	}
}
