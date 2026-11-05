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
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// The first relation is opened before ADD; RC's next statement must not use
// the old relation's schema or range interpretation after the ADD commits.
func TestM0RangesRCRefreshAndReopenSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openM0SQL(t, cn)
		db.SetMaxIdleConns(0) // Reopen below uses a new physical connection.
		defer db.Close()
		const schema = "m0_ranges_rc_binding"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		m0Exec(t, ctx, db, "create database "+schema)
		m0Exec(t, ctx, db, "create table "+schema+".t (c_id int not null, b int not null, primary key(c_id,b))")
		m0Exec(t, ctx, db, "insert into "+schema+".t values (1,1),(2,2)")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+schema+".t')")
		before := m0ObjectFingerprints(t, ctx, db, schema+".t")
		require.NotEmpty(t, before)
		id, version := m0TableGeneration(t, ctx, db, schema, "t")
		require.Zero(t, version)

		reader, err := db.Conn(ctx)
		require.NoError(t, err)
		readerOpen := true
		defer func() {
			if readerOpen {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cleanupCancel()
				_, _ = reader.ExecContext(cleanupCtx, "rollback")
				_ = reader.Close()
			}
		}()
		_, err = reader.ExecContext(ctx, "set session transaction_isolation='READ-COMMITTED'")
		require.NoError(t, err)
		_, err = reader.ExecContext(ctx, "begin")
		require.NoError(t, err)
		var count int
		require.NoError(t, reader.QueryRowContext(ctx, "select count(*) from "+schema+".t where c_id > 0").Scan(&count))
		require.Equal(t, 2, count)

		m0Exec(t, ctx, db, "alter table "+schema+".t add column c int")
		require.Equal(t, before, m0ObjectFingerprints(t, ctx, db, schema+".t"))
		afterID, afterVersion := m0TableGeneration(t, ctx, db, schema, "t")
		require.Equal(t, id, afterID)
		require.Equal(t, uint32(1), afterVersion)
		// Remote scopes serialize the opened table definition. Compare its
		// decoded form with a fresh relation in another transaction.
		svc := cn.RawService().(cnservice.Service)
		catalogCtx := defines.AttachAccountId(ctx, 0)
		loadDefinition := func() *plan.TableDef {
			var result *plan.TableDef
			err := svc.GetSQLExecutor().ExecTxn(catalogCtx, func(op executor.TxnExecutor) error {
				database, err := svc.GetEngine().Database(catalogCtx, schema, op.Txn())
				if err != nil {
					return err
				}
				relation, err := database.Relation(catalogCtx, "t", nil)
				if err != nil {
					return err
				}
				result = proto.Clone(relation.GetTableDef(catalogCtx)).(*plan.TableDef)
				return nil
			}, executor.Options{}.WithWaitCommittedLogApplied())
			require.NoError(t, err)
			require.NotNil(t, result)
			return result
		}
		senderDef := loadDefinition()
		require.Equal(t, id, senderDef.TblId)
		require.Equal(t, afterVersion, senderDef.Version)
		wireDef, err := proto.Marshal(senderDef)
		require.NoError(t, err)
		var decodedDef plan.TableDef
		require.NoError(t, proto.Unmarshal(wireDef, &decodedDef))
		require.True(t, proto.Equal(&decodedDef, loadDefinition()), "reopened definition differs after wire roundtrip")
		require.NoError(t, reader.QueryRowContext(ctx, "select count(*) from "+schema+".t where c is null").Scan(&count))
		require.Equal(t, 2, count)
		require.NoError(t, reader.QueryRowContext(ctx, "select count(*) from "+schema+".t where c is not null").Scan(&count))
		require.Zero(t, count)
		// A client retry starts a new transaction after rollback. This is not
		// an injected engine-level statement retry.
		_, err = reader.ExecContext(ctx, "rollback")
		require.NoError(t, err)
		_, err = reader.ExecContext(ctx, "begin")
		require.NoError(t, err)
		require.NoError(t, reader.QueryRowContext(ctx, "select count(*) from "+schema+".t where c is null").Scan(&count))
		require.Equal(t, 2, count)
		_, err = reader.ExecContext(ctx, "commit")
		require.NoError(t, err)
		require.NoError(t, reader.Close())
		readerOpen = false

		reopened, err := db.Conn(ctx)
		require.NoError(t, err)
		defer reopened.Close()
		require.NoError(t, reopened.QueryRowContext(ctx, "select count(*) from "+schema+".t where c is null").Scan(&count))
		require.Equal(t, 2, count)
	})
}
