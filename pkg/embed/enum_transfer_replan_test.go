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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// The internal executor rebuilds its plan after the first bind has qualified
// source column names. Both executions must preserve the physical ENUM value.
func TestAlterEnumTransferReplan(t *testing.T) {
	RunBaseClusterTests(t, func(cluster Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(defines.AttachAccountId(context.Background(), 0), 90*time.Second)
		defer cancel()
		_, err = db.ExecContext(ctx, "create database enum_transfer_replan")
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, _ = db.ExecContext(cleanupCtx, "drop database if exists enum_transfer_replan")
		}()
		for _, statement := range []string{
			"create table enum_transfer_replan.src(id int primary key, e enum('A','a'))",
			"insert into enum_transfer_replan.src values (1,2)",
			"create table enum_transfer_replan.dst(id int primary key, e enum('A','a') collate utf8mb4_general_ci)",
		} {
			_, err = db.ExecContext(ctx, statement)
			require.NoErrorf(t, err, "%s", statement)
		}
		service := cn.RawService().(cnservice.Service)
		copySQL := "INSERT INTO `enum_transfer_replan`.`dst` (`id`,`e`) SELECT `id`,`e` FROM `enum_transfer_replan`.`src`"
		err = service.GetSQLExecutor().ExecTxn(ctx, func(tx executor.TxnExecutor) error {
			database, err := service.GetEngine().Database(ctx, "enum_transfer_replan", tx.Txn())
			if err != nil {
				return err
			}
			source, err := database.Relation(ctx, "src", nil)
			if err != nil {
				return err
			}
			target, err := database.Relation(ctx, "dst", nil)
			if err != nil {
				return err
			}
			spec := &executor.AlterEnumTransferSpec{
				AccountID: 0, Database: "enum_transfer_replan", SQL: copySQL,
				SourceID: source.GetTableID(ctx), TargetID: target.GetTableID(ctx),
				Source: source.CopyTableDef(ctx), Target: target.CopyTableDef(ctx),
			}
			result, err := tx.Exec(copySQL, executor.StatementOption{}.WithAlterEnumTransfer(spec))
			if err == nil {
				result.Close()
			}
			return err
		}, executor.Options{}.WithDatabase("enum_transfer_replan").WithForceRebuildPlan())
		require.NoError(t, err)
		var ordinal int
		var value string
		require.NoError(t, db.QueryRowContext(ctx, "select e+0,hex(cast(e as char)) from enum_transfer_replan.dst where id=1").Scan(&ordinal, &value))
		require.Equal(t, 2, ordinal)
		require.Equal(t, "61", value)
	})
}
