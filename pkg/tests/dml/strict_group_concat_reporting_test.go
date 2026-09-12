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

package dml

import (
	"context"
	"errors"
	mysql "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

func TestStrictGroupConcatOldWorkerCannotCommit(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		peer, err := c.GetCNService(1)
		require.NoError(t, err)
		inventory := clusterservice.GetMOCluster(cn.ServiceID())
		refresher := inventory.(clusterservice.AuthoritativeRefresher)
		require.Eventually(t, func() bool {
			if refresher.Refresh(ctx) != nil {
				return false
			}
			count := 0
			inventory.GetCNService(clusterservice.NewSelector(), func(metadata.CNService) bool { count++; return true })
			return count == 2
		}, 30*time.Second, 100*time.Millisecond)
		db := openRetestSQLDB(t, c)
		defer db.Close()
		name := strings.ToLower(testutils.GetDatabaseName(t))
		defer cleanupTestDatabases(t, db, name)
		execSQLDB(t, ctx, db, "create database "+name)
		execSQLDB(t, ctx, db, "use "+name)
		execSQLDB(t, ctx, db, "create table src(id int primary key,v varchar(20))")
		execSQLDB(t, ctx, db, "insert into src values(1,'aa'),(2,'bbb')")
		execSQLDB(t, ctx, db, "create table dst(gc varchar(20))")
		execSQLDB(t, ctx, db, "select mo_ctl('dn','flush','"+name+".src')")
		execSQLDB(t, ctx, db, "set session sql_mode='STRICT_TRANS_TABLES'")
		execSQLDB(t, ctx, db, "set session group_concat_max_len=4")
		rt := moruntime.ServiceRuntime(peer.ServiceID())
		oldVersion, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion66)
		defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		oldForce := plan.GetForceScanOnMultiCN()
		plan.SetForceScanOnMultiCN(true)
		defer plan.SetForceScanOnMultiCN(oldForce)
		_, err = db.ExecContext(ctx, "insert into dst select group_concat(v order by id separator '|') from src")
		var sqlErr *mysql.MySQLError
		require.True(t, errors.As(err, &sqlErr), "%v", err)
		require.Equal(t, uint16(1260), sqlErr.Number)
		var count int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from dst").Scan(&count))
		require.Zero(t, count, "a strict write cannot commit a truncated aggregate")
		_, err = db.ExecContext(ctx, "create table rejected_ctas as select group_concat(v order by id separator '|') as gc from src")
		sqlErr = nil
		require.True(t, errors.As(err, &sqlErr), "%v", err)
		require.Equal(t, uint16(1260), sqlErr.Number)
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from information_schema.tables where table_schema=? and table_name='rejected_ctas'", name).Scan(&count))
		require.Zero(t, count, "CTAS internal SQL must inherit the reporting requirement and roll back")
		execSQLDB(t, ctx, db, "insert ignore into dst select group_concat(v order by id separator '|') from src")
		var value string
		require.NoError(t, db.QueryRowContext(ctx, "select gc from dst").Scan(&value))
		require.Equal(t, "aa|b", value, "IGNORE must retain warning-only behavior")
	})
}
