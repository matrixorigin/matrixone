// Copyright 2021 - 2024 Matrix Origin
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

package dml

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestBlockScaledODKUNoOpPreservesImplicitTimestamp(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		db := openRetestSQLDB(t, c)
		defer db.Close()
		execSQLDB(t, ctx, db, "create database vecblock_odku")
		defer cleanupTestDatabases(t, db, "vecblock_odku")
		execSQLDB(t, ctx, db, "use vecblock_odku")
		for _, typ := range []string{"vecf8(1)", "vecf4(1)"} {
			t.Run(typ, func(t *testing.T) {
				execSQLDB(t, ctx, db, "drop table if exists t")
				execSQLDB(t, ctx, db, "create table t(id int primary key,v "+typ+",ts datetime default '2000-01-01 00:00:00' on update current_timestamp)")
				execSQLDB(t, ctx, db, `insert into t(id,v) values(1,cast('{"g":1,"s":[1],"v":[1]}' as `+typ+"))")
				before := queryStringRows(t, ctx, db, "select hex(vecblock_binary(v)),ts='2000-01-01 00:00:00' from t")
				result, err := db.ExecContext(ctx, `insert into t(id,v) values(1,cast('{"g":1,"s":[2],"v":[0.5]}' as `+typ+")) on duplicate key update v=values(v)")
				require.NoError(t, err)
				affected, err := result.RowsAffected()
				require.NoError(t, err)
				require.Zero(t, affected)
				require.Equal(t, before, queryStringRows(t, ctx, db, "select hex(vecblock_binary(v)),ts='2000-01-01 00:00:00' from t"))
				result, err = db.ExecContext(ctx, `insert into t(id,v) values(1,cast('{"g":1,"s":[1],"v":[2]}' as `+typ+")) on duplicate key update v=values(v)")
				require.NoError(t, err)
				affected, err = result.RowsAffected()
				require.NoError(t, err)
				require.Equal(t, int64(2), affected)
				require.Equal(t, [][]string{{"1", "1"}}, queryStringRows(t, ctx, db, "select v=cast('[2]' as "+typ+"),ts<>'2000-01-01 00:00:00' from t"))
			})
		}
	})
}
