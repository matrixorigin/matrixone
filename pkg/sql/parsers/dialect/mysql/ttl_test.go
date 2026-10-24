// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTTLSyntaxAndLegacyIdentifiers(t *testing.T) {
	for _, sql := range []string{
		"create table t(ts datetime) ttl ts + interval 1 day",
		"create table t(ts datetime) ttl ts + interval 1 day delete",
		"alter table t modify ttl ts + interval 1 day delete",
		"alter table t ttl ts + interval 1 hour",
		"alter table t remove ttl",
		"create table ttl(ttl int)",
		"select ttl from ttl",
		"alter table t modify ttl int",
		"alter table t modify column ttl int",
		"alter table t modify `ttl` int",
		"alter table t add column ttl int",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			formatted := tree.String(stmt, dialect.MYSQL)
			again, err := ParseOne(context.Background(), formatted, 1)
			require.NoError(t, err, formatted)
			again.Free()
		})
	}
	for _, sql := range []string{
		"create table t(ts datetime) ttl ts + interval -1 day",
		"create table t(ts datetime) ttl ts + interval 1.5 day",
		"create table t(ts datetime) ttl now() + interval 1 day",
		"create table t(ts datetime) ttl ts + interval 1 day delete where id=1",
		"create table t(ts datetime) ttl ts + interval 1 day to disk 'cold'",
		"create table t(ts datetime) ttl ts + interval 1 day group by ts",
		"create table t(ts datetime) ttl ts + interval 18446744073709551616 day",
	} {
		stmt, err := ParseOne(context.Background(), sql, 1)
		if stmt != nil {
			stmt.Free()
		}
		require.Error(t, err, sql)
	}
}
