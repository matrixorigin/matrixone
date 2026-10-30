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

package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
)

// A literal DEFAULT that cannot be converted to the column type is
// ER_INVALID_DEFAULT (1067), like MySQL, instead of the cast's own error
// (20203 invalid argument, 1690 out of range).
func TestLiteralDefaultCastErrorIsInvalidDefault(t *testing.T) {
	build := func(sql string) error {
		ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
		stmt, err := mysql.ParseOne(t.Context(), sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		_, err = BuildPlan(ctx, stmt, false)
		return err
	}
	for _, def := range []string{
		"c int default 'abc'",
		"c int default '12x'",
		"c int default 99999999999",
		"c tinyint default 300",
		"c tinyint unsigned default -1",
		"c smallint default -40000",
		"c bigint default 'x'",
		"c date default 'abc'",
		"c datetime default 'not a time'",
		"c decimal(5,2) default 'abc'",
	} {
		t.Run("reject "+def, func(t *testing.T) {
			err := build("create table dd(id int, " + def + ")")
			require.Error(t, err, def)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault), "%s: %v", def, err)
			require.Contains(t, err.Error(), "Invalid default value for 'c'")
		})
	}
	for _, def := range []string{
		"c int default '12'",
		"c int default -5",
		"c tinyint default 127",
		"c date default '2026-10-30'",
		"c decimal(5,2) default '1.5'",
		"c int default null",
	} {
		t.Run("accept "+def, func(t *testing.T) {
			require.NoError(t, build("create table dd(id int, "+def+")"), def)
		})
	}
}

// ALTER TABLE ... ALTER COLUMN ... SET DEFAULT goes through the same default
// builder and used to return 20203 for an INT column.
func TestAlterColumnSetLiteralDefaultCastErrorIsInvalidDefault(t *testing.T) {
	for _, def := range []string{"'abc'", "'12x'", "99999999999999999999999"} {
		t.Run(def, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			origin := makeAlterCoverageTableDef()
			copyTable := DeepCopyTableDef(origin, true)
			alterCtx := initAlterTableContext(origin, copyTable, origin.DbName)
			alterPlan := &planpb.AlterTable{
				Database:     origin.DbName,
				TableDef:     origin,
				CopyTableDef: copyTable,
			}
			spec := mustParseAlterColumnClause(t, mock.CurrentContext(),
				"alter table t1 alter column id set default "+def)
			_, err := AlterColumn(mock.CurrentContext(), alterPlan, spec, alterCtx)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault), "%s: %v", def, err)
		})
	}
}
