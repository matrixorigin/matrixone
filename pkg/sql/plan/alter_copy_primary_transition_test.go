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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestAlterCopyPrimaryKeyTransitions(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name: "rename after dropping primary key",
			sql:  "ALTER TABLE t1 ALGORITHM=COPY, DROP PRIMARY KEY, RENAME COLUMN a TO x, ADD PRIMARY KEY(x)",
		},
		{
			name: "change after dropping primary key",
			sql:  "ALTER TABLE t1 ALGORITHM=COPY, DROP PRIMARY KEY, CHANGE COLUMN a x BIGINT, ADD PRIMARY KEY(x)",
		},
		{
			name:    "drop primary key twice",
			sql:     "ALTER TABLE t1 ALGORITHM=COPY, DROP PRIMARY KEY, DROP PRIMARY KEY",
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got *Plan
			var err error
			require.NotPanics(t, func() { got, err = buildSingleStmt(NewMockOptimizer(false), t, tc.sql) })
			if tc.wantErr {
				require.Error(t, err)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrCantDropFieldOrKey))
				return
			}
			require.NoError(t, err)
			require.Equal(t, []string{"x"}, got.GetDdl().GetAlterTable().CopyTableDef.Pkey.Names)
		})
	}
}

func TestAlterCopyChangeTracksGeneratedDependencies(t *testing.T) {
	for _, tc := range []struct {
		sql                  string
		wantErr              bool
		preserveGeneratedIdx bool
	}{
		{"ALTER TABLE dept ALGORITHM=COPY, CHANGE COLUMN loc loc INT", false, false},
		{"ALTER TABLE dept ALGORITHM=COPY, CHANGE COLUMN loc renamed VARCHAR(50)", true, false},
		{"ALTER TABLE dept ALGORITHM=COPY, ADD COLUMN extra INT", false, true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			setMockGeneratedColumn(t, mock, "dept", "dname", "loc")
			table := mock.ctxt.tables["dept"]
			FindColumn(table.Cols, "dname").GeneratedCol.OriginString = "loc"
			table.Indexes = []*plan.IndexDef{
				{IndexName: "gen_idx", IndexTableName: "gen_hidden", Parts: []string{"dname"}},
				{IndexName: "other_idx", IndexTableName: "other_hidden", Parts: []string{"deptno"}},
			}
			built, err := buildSingleStmt(mock, t, tc.sql)
			if tc.wantErr {
				require.ErrorContains(t, err, "generated column 'dname' depends on it")
			} else {
				require.NoError(t, err)
				options := built.GetDdl().GetAlterTable().Options
				require.Equal(t, tc.preserveGeneratedIdx, options.SkipIndexesCopy["gen_idx"])
				require.True(t, options.SkipIndexesCopy["other_idx"])
			}
		})
	}
}
