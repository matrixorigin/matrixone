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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

// Issue #31688: a snapshot-qualified table is a read-only historical version;
// using it as a write or DDL target used to modify the live table silently.
func TestRejectSnapshotQualifiedWriteTarget(t *testing.T) {
	ctx := NewMockCompilerContext(false, nil)
	check := func(sql string) error {
		stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, sql, 1)
		require.NoError(t, err, sql)
		return rejectSnapshotQualifiedWriteTarget(ctx, stmts[0])
	}
	for _, sql := range []string{
		"insert into t{snapshot='s'} values (1)",
		"replace into t{snapshot='s'} values (1)",
		"update t{snapshot='s'} set v = 1 where id = 1",
		"update t{snapshot='s'} as x join o on o.id = x.id set x.v = 1",
		"update o join t{snapshot='s'} on o.id = t.id set t.v = 1",
		"delete from t{snapshot='s'} where id = 1",
		"delete x from t{snapshot='s'} as x join o on o.id = x.id",
		"alter table t{snapshot='s'} add column c int",
		"truncate table t{snapshot='s'}",
		"drop table t{snapshot='s'}",
		"drop table o, t{snapshot='s'}",
		"insert into t{timestamp='2024-01-01 00:00:00'} values (1)",
	} {
		err := check(sql)
		require.Error(t, err, sql)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%s: %v", sql, err)
	}
	for _, sql := range []string{
		"insert into t select * from t{snapshot='s'}",
		"update o join t{snapshot='s'} as x on o.id = x.id set o.v = x.v",
		"delete o from o join t{snapshot='s'} as x on o.id = x.id",
		"select * from t{snapshot='s'}",
		"insert into t values (1)",
		"drop table t",
	} {
		require.NoError(t, check(sql), sql)
	}
}
