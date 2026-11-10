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
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/stretchr/testify/require"
)

func TestNativeRebaseCrossCNSourceProtection(t *testing.T) {
	require.NoError(t, embed.CloseSingleCNBaseClusterTests())
	c, err := embed.StartTestCluster(embed.WithCNCount(2), embed.WithPreStart(func(op embed.ServiceOperator) {
		if op.ServiceType() == metadata.ServiceType_TN {
			op.Adjust(func(cfg *embed.ServiceConfig) {
				tn := cfg.TN_please_use_getTNServiceConfig
				if tn == nil {
					tn = cfg.TNCompatible
				}
				tn.Txn.EnableRebaseDataHistory = true
			})
		}
		if op.ServiceType() == metadata.ServiceType_CN {
			op.Adjust(func(cfg *embed.ServiceConfig) { cfg.CN.Txn.Mode = pbtxn.TxnMode_Pessimistic.String() })
		}
	}))
	if c != nil {
		defer func() { require.NoError(t, c.Close()) }()
	}
	require.NoError(t, err)
	counts := map[metadata.ServiceType]int{}
	c.ForeachServices(func(op embed.ServiceOperator) bool { counts[op.ServiceType()]++; return true })
	require.Equal(t, 2, counts[metadata.ServiceType_CN])
	require.Equal(t, 1, counts[metadata.ServiceType_TN])
	restoreEnabled := frontend.SetRebaseSourceExpectationForTest(true)
	defer restoreEnabled()
	var cn [2]embed.ServiceOperator
	var db [2]*sql.DB
	for i := range 2 {
		cn[i], err = c.GetCNService(i)
		require.NoError(t, err)
		db[i], err = sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn[i].GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db[i].Close()
	}
	require.NotEqual(t, cn[0].ServiceID(), cn[1].ServiceID())
	for _, scenario := range []string{"catalog_after_f", "dml_during_replay", "unrelated_scopes"} {
		if !t.Run(scenario, func(t *testing.T) {
			ctx, stop := context.WithTimeout(t.Context(), 2*time.Minute)
			defer stop()
			prefix := "nr_xcn_" + scenario + "_"
			upstream, source, target, meta, other := prefix+"u", prefix+"f", prefix+"next", prefix+"meta", prefix+"other"
			defer cleanupSQLIntegration(t, cn[0], "drop snapshot if exists "+prefix+"fh", "drop snapshot if exists "+prefix+"uh",
				"drop snapshot if exists "+prefix+"bf", "drop snapshot if exists "+prefix+"bu", "drop database if exists "+target,
				"drop database if exists "+source, "drop database if exists "+upstream, "drop database if exists "+meta, "drop database if exists "+other)
			f := rebaseSQLFixture{t, ctx, db[0], prefix}
			writer := rebaseSQLFixture{t, ctx, db[1], prefix}
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
			epochQuery := fmt.Sprintf("select cast(epoch as signed) from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", id)
			epoch := f.scalar(epochQuery)
			fh, uh := f.snapshot(source, "fh"), f.snapshot(upstream, "uh")
			owner, err := db[0].Conn(ctx)
			require.NoError(t, err)
			defer owner.Close()
			defer func() {
				cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
				defer cancel()
				_, _ = owner.ExecContext(cleanup, "rollback")
			}()
			for _, statement := range []string{"set session transaction isolation level read committed", "begin"} {
				_, err = owner.ExecContext(ctx, statement)
				require.NoError(t, err)
			}
			var ownerID uint32
			require.NoError(t, owner.QueryRowContext(ctx, "select connection_id()").Scan(&ownerID))
			sessions := cn[0].RawService().(interface {
				SessionMgr() *queryservice.SessionManager
			}).SessionMgr()
			var session *frontend.Session
			for _, candidate := range sessions.GetAllSessions() {
				if value, ok := candidate.(*frontend.Session); ok && value.GetConnectionID() == ownerID {
					session = value
				}
			}
			require.NotNil(t, session)
			require.Eventually(t, func() bool { return !session.GetQueryInProgress() }, 5*time.Second, 10*time.Millisecond)
			txn := session.GetTxnHandler().GetTxn()
			require.NotNil(t, txn)
			require.Equal(t, pbtxn.TxnMode_Pessimistic, txn.Txn().Mode)
			require.Equal(t, pbtxn.TxnIsolation_RC, txn.Txn().Isolation)
			t.Logf("publisher CN=%s transaction=%x; writer CN=%s; TN count=1", cn[0].ServiceID(), txn.Txn().ID, cn[1].ServiceID())
			query := fmt.Sprintf("data branch rebase database %s from %s against %s between %s and %s tables(t) for update expect (%d,%d)", target, fh, uh, bf, bu, id, epoch)
			if scenario == "catalog_after_f" {
				writer.exec("create table " + source + ".extra(id int primary key,v int)")
				writer.exec("insert into " + source + ".extra values(9,90)")
				rows, queryErr := owner.QueryContext(ctx, query)
				if rows != nil {
					require.NoError(t, rows.Close())
				}
				require.ErrorContains(t, queryErr, "def changed")
				_, err = owner.ExecContext(ctx, "rollback")
				require.NoError(t, err)
				require.Equal(t, []string{"9:90"}, f.rows(source, "extra"))
				require.Greater(t, f.scalar(epochQuery), epoch)
			} else {
				var tenant *sql.DB
				if scenario == "unrelated_scopes" {
					f.exec("create database " + other)
					f.exec("create account nr_xcn_tenant admin_name 'root' identified by '111'")
					defer cleanupSQLIntegration(t, cn[0], "drop account if exists nr_xcn_tenant")
					tenant, err = sql.Open("mysql", fmt.Sprintf("nr_xcn_tenant#root#accountadmin:111@tcp(127.0.0.1:%d)/", cn[1].GetServiceConfig().CN.Frontend.Port))
					require.NoError(t, err)
					defer tenant.Close()
					tenantFixture := rebaseSQLFixture{t, ctx, tenant, prefix}
					tenantFixture.exec("create database " + source)
				}
				reached, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
				var reachedOnce, releaseOnce sync.Once
				restore := frontend.SetRebaseTableAppliedHookForTest(func(hook context.Context, database, table string) {
					if database == target && table == "t" {
						reachedOnce.Do(func() { close(reached) })
						select {
						case <-release:
						case <-hook.Done():
						}
					}
				})
				joined := false
				defer func() {
					releaseOnce.Do(func() { close(release) })
					if !joined {
						stop()
						select {
						case <-done:
						case <-time.After(15 * time.Second):
							t.Error("cross-CN Rebase query did not terminate")
						}
					}
					restore()
				}()
				go func() {
					rows, queryErr := owner.QueryContext(ctx, query)
					if rows != nil {
						if closeErr := rows.Close(); queryErr == nil {
							queryErr = closeErr
						}
					}
					done <- queryErr
				}()
				select {
				case <-reached:
				case queryErr := <-done:
					joined = true
					t.Fatalf("Rebase exited before its applied-table barrier: %v", queryErr)
				case <-ctx.Done():
					t.Fatal("Rebase did not reach its applied-table barrier")
				}
				if scenario == "dml_during_replay" {
					writer.exec("insert into " + source + ".history values(2)")
				} else {
					writer.exec("create table " + other + ".extra(id int primary key,v int)")
					writer.exec("insert into " + other + ".extra values(7,70)")
					tenantFixture := rebaseSQLFixture{t, ctx, tenant, prefix}
					tenantFixture.exec("create table " + source + ".extra(id int primary key,v int)")
					tenantFixture.exec("insert into " + source + ".extra values(8,80)")
				}
				releaseOnce.Do(func() { close(release) })
				select {
				case err = <-done:
					joined = true
					require.NoError(t, err)
				case <-ctx.Done():
					t.Fatal("cross-CN Rebase query did not return")
				}
				result, err := owner.ExecContext(ctx, "update "+meta+".registry set db='"+target+"',gen=1 where name='feature' and db='"+source+"' and gen=0")
				require.NoError(t, err)
				n, err := result.RowsAffected()
				require.NoError(t, err)
				require.EqualValues(t, 1, n)
				_, publishErr := owner.ExecContext(ctx, "drop database "+source)
				if publishErr == nil {
					_, publishErr = owner.ExecContext(ctx, "commit")
				}
				if scenario == "dml_during_replay" {
					require.ErrorContains(t, publishErr, "def changed")
					_, err = owner.ExecContext(ctx, "rollback")
					require.NoError(t, err)
					require.Equal(t, epoch, f.scalar(epochQuery))
					require.EqualValues(t, 1, f.scalar("select count(*) from "+source+".history where v=2"))
				} else {
					require.NoError(t, publishErr)
					require.Equal(t, []string{"1:11", "2:20"}, f.rows(target, "t"))
					require.EqualValues(t, 3, f.scalar("select count(*) from "+target+".history"))
					require.EqualValues(t, 2, f.scalar("select count(*) from "+target+".history where v=1"))
					require.EqualValues(t, 1, f.scalar("select count(*) from "+target+".history where v is null"))
					require.EqualValues(t, 1, f.scalar("select gen from "+meta+".registry where name='feature' and db='"+target+"'"))
					require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='"+source+"'"))
					require.Equal(t, []string{"7:70"}, writer.rows(other, "extra"))
					require.Equal(t, []string{"8:80"}, (rebaseSQLFixture{t, ctx, tenant, prefix}).rows(source, "extra"))
				}
			}
			if scenario != "unrelated_scopes" {
				require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='"+target+"'"))
				require.Zero(t, f.scalar("select gen from "+meta+".registry where name='feature' and db='"+source+"'"))
				require.Equal(t, []string{"1:11"}, f.rows(source, "t"))
				require.EqualValues(t, 2, f.scalar("select count(*) from "+source+".history where v=1"))
				require.EqualValues(t, 1, f.scalar("select count(*) from "+source+".history where v is null"))
				expectedHistory := int64(3)
				if scenario == "dml_during_replay" {
					expectedHistory = 4
				}
				require.Equal(t, expectedHistory, f.scalar("select count(*) from "+source+".history"))
			}
			require.Equal(t, []string{"1:10", "2:20"}, f.rows(upstream, "t"))
		}) {
			return
		}
	}
}
