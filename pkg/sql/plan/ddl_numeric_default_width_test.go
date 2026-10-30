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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
)

// A numeric DEFAULT whose text does not fit a CHAR/VARCHAR column is
// ER_INVALID_DEFAULT (1067), like an over-long string literal already is.
// It used to be accepted and silently truncated ('123' for VARCHAR(3)
// DEFAULT 12345).
func TestCreateTableRejectsTooWideNumericStringDefault(t *testing.T) {
	build := func(sql string) error {
		ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
		stmt, err := mysql.ParseOne(t.Context(), sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		_, err = BuildPlan(ctx, stmt, false)
		return err
	}
	for _, def := range []string{
		"v varchar(3) default 12345",
		"v char(2) default 123",
		"v varchar(3) default 1.25",
		"v varchar(3) default 'abcd'",
		"v varchar(2) default -12",
		"v varchar(3) default 1e10",
	} {
		t.Run("reject "+def, func(t *testing.T) {
			err := build("create table dd(id int, " + def + ")")
			require.Error(t, err, def)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault), "%s: %v", def, err)
		})
	}
	for _, def := range []string{
		"v varchar(3) default 123",
		"v varchar(4) default 1.25",
		"v varchar(3) default 'abc'",
		"v varchar(3) default null",
		"v char(5) default 12345",
		"v varchar(3) default -12",
		"v char(1) default 0",
		"v varchar(3) default (12345)",
	} {
		t.Run("accept "+def, func(t *testing.T) {
			require.NoError(t, build("create table dd(id int, "+def+")"), def)
		})
	}
}
