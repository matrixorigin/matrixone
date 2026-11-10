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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/stretchr/testify/require"
)

// The test-only switch qualifies EXPECT through real SQL and the existing
// client-owned retirement composition, not the complete adapter protocol.
func TestNativeRebaseSnapshotDependencyPublication(t *testing.T) {
	runRebaseHistoryIntegration(t, func(c embed.Cluster) {
		restore := frontend.SetRebaseSourceExpectationForTest(true)
		defer restore()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		sessions, ok := cn.RawService().(interface {
			SessionMgr() *queryservice.SessionManager
		})
		require.True(t, ok)
		findSession := func(connectionID uint32) *frontend.Session {
			for _, candidate := range sessions.SessionMgr().GetAllSessions() {
				if session, ok := candidate.(*frontend.Session); ok && session.GetConnectionID() == connectionID {
					return session
				}
			}
			return nil
		}
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		for _, scenario := range []string{"control", "catalog_after_snapshot", "before_dependency", "after_replay", "publication_first", "retirement_before_write"} {
			if !t.Run(scenario, func(t *testing.T) {
				prefix := "nr_dep_" + scenario + "_"
				upstream, source, target, meta := prefix+"u", prefix+"f", prefix+"next", prefix+"meta"
				defer cleanupSQLIntegration(t, cn,
					"drop snapshot if exists "+prefix+"uh", "drop snapshot if exists "+prefix+"fh",
					"drop snapshot if exists "+prefix+"bf", "drop snapshot if exists "+prefix+"bu",
					"drop database if exists "+target, "drop database if exists "+source,
					"drop database if exists "+upstream, "drop database if exists "+meta)
				f := rebaseSQLFixture{t, ctx, db, prefix}
				f.exec("create database " + upstream)
				f.exec("create table " + upstream + ".t(id int primary key,v int)")
				f.exec("create table " + upstream + ".history(v int)")
				f.exec("insert into " + upstream + ".t values(1,10)")
				f.exec("insert into " + upstream + ".history values(1),(1),(NULL)")
				bu := f.snapshot(upstream, "bu")
				f.exec("data branch create database " + source + " from " + bu)
				bf := f.snapshot(source, "bf")
				f.exec("update " + source + ".t set v=11 where id=1")
				f.exec("insert into " + upstream + ".t values(2,20)")
				f.exec("create database " + meta)
				f.exec("create table " + meta + ".registry(name varchar(64) primary key,db varchar(64),gen int)")
				f.exec("insert into " + meta + ".registry values('feature','" + source + "',0)")
				id := f.scalar("select cast(dat_id as signed) from mo_catalog.mo_database where account_id=0 and datname='" + source + "'")
				version := fmt.Sprintf("select cast(epoch as signed) from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", id)
				fixed := f.scalar(version)
				fh, uh := f.snapshot(source, "fh"), f.snapshot(upstream, "uh")
				// This later upstream write is intentionally outside the fixed U.
				f.exec("insert into " + upstream + ".t values(3,30)")
				if scenario == "before_dependency" {
					f.exec("insert into " + source + ".history values(2)")
				}
				if scenario == "catalog_after_snapshot" {
					f.exec("create table " + source + ".extra(id int primary key,v int)")
					f.exec("insert into " + source + ".extra values(9,90)")
				}

				owner, err := db.Conn(ctx)
				require.NoError(t, err)
				defer owner.Close()
				defer func() {
					cleanupCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
					defer stop()
					_, _ = owner.ExecContext(cleanupCtx, "rollback")
				}()
				var connectionID uint32
				require.NoError(t, owner.QueryRowContext(ctx, "select connection_id()").Scan(&connectionID))
				session := findSession(connectionID)
				require.NotNil(t, session)
				_, err = owner.ExecContext(ctx, "set session transaction isolation level read committed")
				require.NoError(t, err)
				_, err = owner.ExecContext(ctx, "set session lock_wait_timeout=1")
				require.NoError(t, err)
				_, err = owner.ExecContext(ctx, "begin")
				require.NoError(t, err)
				require.Eventually(t, func() bool { return !session.GetQueryInProgress() }, 5*time.Second, 10*time.Millisecond)
				op := session.GetTxnHandler().GetTxn()
				require.NotNil(t, op)
				require.Equal(t, pbtxn.TxnMode_Pessimistic, op.Txn().Mode)
				require.Equal(t, pbtxn.TxnIsolation_RC, op.Txn().Isolation)
				query := fmt.Sprintf("data branch rebase database %s from %s against %s between %s and %s tables (t) for update expect (%d,%d)", target, fh, uh, bf, bu, id, fixed)
				rows, err := owner.QueryContext(ctx, query)
				if scenario == "catalog_after_snapshot" {
					if rows != nil {
						require.NoError(t, rows.Close())
					}
					require.ErrorContains(t, err, "def changed", "successful CREATE/INSERT after F must invalidate publication")
					_, err = owner.ExecContext(ctx, "rollback")
					require.NoError(t, err)
					require.Equal(t, []string{"9:90"}, f.rows(source, "extra"))
					require.Equal(t, []string{"1:11"}, f.rows(source, "t"))
					require.Greater(t, f.scalar(version), fixed)
					require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='"+target+"'"))
					require.Zero(t, f.scalar("select gen from "+meta+".registry where name='feature' and db='"+source+"'"))
					require.Equal(t, []string{"1:10", "2:20", "3:30"}, f.rows(upstream, "t"))
					return
				}
				require.NoError(t, err)
				defer rows.Close()
				require.True(t, rows.Next(), "R1 must return a provisional summary")
				require.NoError(t, rows.Err())
				require.NoError(t, rows.Close())
				if scenario == "after_replay" {
					f.exec("insert into " + source + ".history values(2)")
				}
				var writer *sql.Conn
				if scenario == "publication_first" || scenario == "retirement_before_write" {
					writer, err = db.Conn(ctx)
					require.NoError(t, err)
					defer writer.Close()
					defer func() {
						cleanupCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
						defer stop()
						_, _ = writer.ExecContext(cleanupCtx, "rollback")
					}()
					var writerID uint32
					require.NoError(t, writer.QueryRowContext(ctx, "select connection_id()").Scan(&writerID))
					require.NotEqual(t, connectionID, writerID)
					writerSession := findSession(writerID)
					require.NotNil(t, writerSession)
					statements := []string{
						"set session transaction isolation level read committed",
						"set session lock_wait_timeout=1", "begin",
					}
					if scenario == "publication_first" {
						statements = append(statements, "insert into "+source+".history values(2)")
					}
					for _, statement := range statements {
						_, err = writer.ExecContext(ctx, statement)
						require.NoError(t, err, statement)
					}
					if scenario == "retirement_before_write" {
						var count int
						require.NoError(t, writer.QueryRowContext(ctx, "select count(*) from "+source+".history").Scan(&count))
						require.Equal(t, 3, count, "the old connection must really observe the source before retirement")
					}
					require.Eventually(t, func() bool { return !writerSession.GetQueryInProgress() }, 5*time.Second, 10*time.Millisecond)
					writerTxn := writerSession.GetTxnHandler().GetTxn()
					require.NotNil(t, writerTxn)
					require.NotEqual(t, op.Txn().ID, writerTxn.Txn().ID)
					require.Equal(t, pbtxn.TxnMode_Pessimistic, writerTxn.Txn().Mode)
					require.Equal(t, pbtxn.TxnIsolation_RC, writerTxn.Txn().Isolation)
					require.Zero(t, f.scalar("select count(*) from "+source+".history where v=2"), "the executed source write is still uncommitted")
				}
				result, err := owner.ExecContext(ctx, "update "+meta+".registry set db='"+target+"',gen=1 where name='feature' and db='"+source+"' and gen=0")
				require.NoError(t, err)
				affected, err := result.RowsAffected()
				require.NoError(t, err)
				require.EqualValues(t, 1, affected)
				_, publishErr := owner.ExecContext(ctx, "drop database "+source)
				if publishErr == nil {
					_, publishErr = owner.ExecContext(ctx, "commit")
				}
				if writer != nil {
					if scenario == "retirement_before_write" {
						require.NoError(t, publishErr, "a plain reader must not prevent source publication")
						_, lateErr := writer.ExecContext(ctx, "insert into "+source+".history values(2)")
						if lateErr == nil {
							_, lateErr = writer.ExecContext(ctx, "commit")
						}
						require.Error(t, lateErr, "a retired-source write cannot commit after publication")
						_, err = writer.ExecContext(ctx, "rollback")
						require.NoError(t, err)
					} else {
						if publishErr != nil {
							message := strings.ToLower(publishErr.Error())
							require.True(t, strings.Contains(message, "def changed") || strings.Contains(message, "lock wait timeout"), "unexpected publication rejection: %v", publishErr)
							_, err = owner.ExecContext(ctx, "rollback")
							require.NoError(t, err)
						}
						_, writerErr := writer.ExecContext(ctx, "commit")
						if publishErr == nil {
							require.Error(t, writerErr, "retired-source DML cannot also commit successfully")
							_, err = writer.ExecContext(ctx, "rollback")
							require.NoError(t, err)
						} else {
							require.NoError(t, writerErr, "a rejected publisher must leave the pending source writer able to commit")
						}
						t.Logf("publication-first attempt: publication committed=%t; writer committed=%t", publishErr == nil, writerErr == nil)
					}
				}
				if scenario == "control" || scenario == "retirement_before_write" || scenario == "publication_first" && publishErr == nil {
					require.NoError(t, publishErr)
					require.Equal(t, []string{"1:11", "2:20"}, f.rows(target, "t"))
					require.EqualValues(t, 3, f.scalar("select count(*) from "+target+".history"))
					require.EqualValues(t, 2, f.scalar("select count(*) from "+target+".history where v=1"))
					require.EqualValues(t, 1, f.scalar("select count(*) from "+target+".history where v is null"))
					require.Zero(t, f.scalar(fmt.Sprintf("select count(*) from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", id)))
					require.EqualValues(t, 1, f.scalar("select gen from "+meta+".registry where name='feature' and db='"+target+"'"))
				} else {
					if writer == nil {
						require.ErrorContains(t, publishErr, "def changed", "a committed no-PK INSERT after F must not be retired silently")
					}
					_, err = owner.ExecContext(ctx, "rollback")
					require.NoError(t, err)
					require.EqualValues(t, fixed, f.scalar(version))
					require.Equal(t, []string{"1:11"}, f.rows(source, "t"))
					require.EqualValues(t, 4, f.scalar("select count(*) from "+source+".history"))
					require.EqualValues(t, 2, f.scalar("select count(*) from "+source+".history where v=1"))
					require.EqualValues(t, 1, f.scalar("select count(*) from "+source+".history where v is null"))
					require.EqualValues(t, 1, f.scalar("select count(*) from "+source+".history where v=2"))
					require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='"+target+"'"))
					require.Zero(t, f.scalar("select gen from "+meta+".registry where name='feature' and db='"+source+"'"))
				}
				require.Equal(t, []string{"1:10", "2:20", "3:30"}, f.rows(upstream, "t"))
			}) {
				return
			}
		}
	})
}

// The persistent history format is opt-in and must not change the canonical
// default-config fixture used by unrelated SQL integration tests.
func runRebaseHistoryIntegration(t *testing.T, fn func(embed.Cluster)) {
	t.Helper()
	require.NoError(t, embed.CloseSingleCNBaseClusterTests())
	c, err := embed.StartTestCluster(embed.WithTestAdmissionContext(t.Context()), embed.WithPreStart(func(op embed.ServiceOperator) {
		if op.ServiceType() == metadata.ServiceType_CN {
			op.Adjust(func(cfg *embed.ServiceConfig) {
				cfg.CN.Txn.Mode = pbtxn.TxnMode_Pessimistic.String()
				cfg.CN.Txn.Isolation = pbtxn.TxnIsolation_RC.String()
			})
		}
		if op.ServiceType() == metadata.ServiceType_TN {
			op.Adjust(func(cfg *embed.ServiceConfig) {
				tn := cfg.TN_please_use_getTNServiceConfig
				if tn == nil {
					tn = cfg.TNCompatible
				}
				tn.Txn.EnableRebaseDataHistory = true
			})
		}
	}))
	if c != nil {
		defer func() { require.NoError(t, c.Close()) }()
	}
	require.NoError(t, err)
	fn(c)
}
