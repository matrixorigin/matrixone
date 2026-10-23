//go:build mo_m0

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

package sqlintegration

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestM0BirthFalseFallbackSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		closeAdmission := admitM0BirthSQL(t, cn)
		defer closeAdmission()
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db := openM0SQL(t, cn)
		defer db.Close()
		const schema = "m0_birth_fallback"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		m0Exec(t, ctx, db, "create database "+schema)
		for i, definition := range []string{
			"flag boolean default true",
			"flag boolean default (1=0)",
			"flag boolean default 'false'",
			"flag int default 0",
			"flag boolean not null default false",
			"flag boolean default false comment 'retained'",
			"flag boolean default false after c_id",
		} {
			name := fmt.Sprintf("customer_%d", i)
			table := schema + "." + name
			m0Exec(t, ctx, db, "create table "+table+m0RollbackCustomerSchema)
			m0Exec(t, ctx, db, "insert into "+table+" values(1,1,1,50000,100,10)")
			beforeID, _ := m0TableGeneration(t, ctx, db, schema, name)
			_, err = db.ExecContext(ctx, "alter table "+table+" add column "+definition+", algorithm=inplace")
			require.Error(t, err, definition)
			afterRejected, _ := m0TableGeneration(t, ctx, db, schema, name)
			require.Equal(t, beforeID, afterRejected)
			m0Exec(t, ctx, db, "alter table "+table+" add column "+definition)
			afterID, version := m0TableGeneration(t, ctx, db, schema, name)
			require.NotEqual(t, beforeID, afterID, definition)
			require.Zero(t, version)
			require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from "+table))
		}
		for _, indexSQL := range []string{
			"create index ix on %s(c_balance)",
			"alter table %s add index ix(c_balance)",
			"create unique index ix on %s(c_balance)",
		} {
			name := "index_exit"
			table := schema + "." + name
			m0Exec(t, ctx, db, "create table "+table+m0RollbackCustomerSchema)
			m0Exec(t, ctx, db, "insert into "+table+" values(1,1,1,50000,100,10),(2,1,1,50000,100,20)")
			m0Exec(t, ctx, db, "alter table "+table+" add column flag boolean default false")
			m0Exec(t, ctx, db, "insert into "+table+" values(3,1,1,50000,100,30,NULL)")
			beforeID, beforeVersion := m0TableGeneration(t, ctx, db, schema, name)
			require.Equal(t, uint32(1), beforeVersion)
			// A failed UNIQUE build must leave the source and its birth state usable.
			_, err = db.ExecContext(ctx, "create unique index duplicate_flags on "+table+"(flag)")
			require.ErrorContains(t, err, "Duplicate entry 'false' for key 'flag'")
			afterFailure, version := m0TableGeneration(t, ctx, db, schema, name)
			require.Equal(t, beforeID, afterFailure)
			require.Equal(t, beforeVersion, version)
			require.Equal(t, 2, m0Count(t, ctx, db, "select count(*) from "+table+" where flag=false"))
			m0Exec(t, ctx, db, fmt.Sprintf(indexSQL, table))
			afterID, version := m0TableGeneration(t, ctx, db, schema, name)
			require.NotEqual(t, beforeID, afterID)
			require.Zero(t, version)
			require.Equal(t, 2, m0Count(t, ctx, db, "select count(*) from "+table+" where flag=false"))
			require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from "+table+" where flag is null"))
			require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from "+table+" where c_balance=20"))
			m0Exec(t, ctx, db, "insert into "+table+"(c_id,c_d_id,c_w_id,c_balance) values(4,1,1,40)")
			require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from "+table+" where c_id=4 and flag=false"))
			m0Exec(t, ctx, db, "drop table "+table)
		}
	})
}

func TestM0BirthFalseTransactionFallback(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		closeAdmission := admitM0BirthSQL(t, cn)
		defer closeAdmission()
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		ctx = defines.AttachAccountId(ctx, 0)
		db, table := m0PrepareRollbackTable(t, ctx, cn, "m0_birth_transaction")
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists m0_birth_transaction")
		m0Exec(t, ctx, db, "alter table "+table+" add column committed_flag boolean default false")
		beforeID, beforeVersion := m0TableGeneration(t, ctx, db, "m0_birth_transaction", "customer")
		service := cn.RawService().(cnservice.Service)
		op, err := service.GetTxnClient().New(ctx, timestamp.Timestamp{}, client.WithBeginAutoCommit(true, false),
			client.WithTxnMode(txn.TxnMode_Pessimistic), client.WithTxnIsolation(txn.TxnIsolation_RC))
		require.NoError(t, err)
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = op.Rollback(cleanup)
		}()
		require.NoError(t, service.GetEngine().New(ctx, op))
		exec := func(sql string) {
			result, err := service.GetSQLExecutor().Exec(ctx, sql, executor.Options{}.WithTxn(op))
			require.NoError(t, err, sql)
			result.Close()
		}
		exec("insert into " + table + "(c_id,c_d_id,c_w_id,committed_flag) values(4,4,4,NULL)")
		exec("alter table " + table + " add column transaction_flag boolean default false")
		database, err := service.GetEngine().Database(ctx, "m0_birth_transaction", op)
		require.NoError(t, err)
		relation, err := database.Relation(ctx, "customer", nil)
		require.NoError(t, err)
		after := relation.CopyTableDef(ctx)
		require.NotEqual(t, beforeID, after.TblId)
		require.Zero(t, after.Version)
		require.Nil(t, after.AppendMissingState)
		require.NoError(t, op.Rollback(ctx))
		fresh := openM0SQL(t, cn)
		defer fresh.Close()
		afterID, afterVersion := m0TableGeneration(t, ctx, fresh, "m0_birth_transaction", "customer")
		require.Equal(t, beforeID, afterID)
		require.Equal(t, beforeVersion, afterVersion)
		require.Equal(t, 3, m0Count(t, ctx, fresh, "select count(*) from "+table))
		require.Equal(t, 3, m0Count(t, ctx, fresh, "select count(*) from "+table+" where committed_flag=false"))
		rows, err := fresh.QueryContext(ctx, "select transaction_flag from "+table)
		if rows != nil {
			rows.Close()
		}
		require.Error(t, err)
	})
}
