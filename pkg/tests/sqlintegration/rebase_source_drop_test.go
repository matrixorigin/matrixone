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
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// The real DROP must queue on the source inventory key, not merely fail to
// finish within a sleep. Both terminal outcomes release that same admission.
func TestRebaseSourceDropAdmission(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		for _, abort := range []bool{false, true} {
			t.Run(fmt.Sprintf("cancel_%t", abort), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
				defer cancel()
				db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
				require.NoError(t, err)
				defer db.Close()
				const source, target = "rebase_p1_drop_source", "rebase_p1_drop_target"
				defer cleanupSQLIntegration(t, cn, "drop database if exists "+target, "drop database if exists "+source)
				exec := func(q string) { t.Helper(); _, err := db.ExecContext(ctx, q); require.NoError(t, err, q) }
				count := func(q string) int {
					t.Helper()
					var n int
					require.NoError(t, db.QueryRowContext(ctx, q).Scan(&n), q)
					return n
				}
				exec("create database " + source)
				for _, table := range []string{"a", "b"} {
					exec("create table " + source + "." + table + "(id int primary key,v int)")
					exec("insert into " + source + "." + table + " values(1,10),(2,20)")
				}
				clone, err := db.Conn(ctx)
				require.NoError(t, err)
				defer clone.Close()
				drop, err := db.Conn(ctx)
				require.NoError(t, err)
				defer drop.Close()
				var cloneID, dropID uint32
				require.NoError(t, clone.QueryRowContext(ctx, "select connection_id()").Scan(&cloneID))
				require.NoError(t, drop.QueryRowContext(ctx, "select connection_id()").Scan(&dropID))
				pacer := newAdmissionPacer(t, "mo_snapshots#0#Exclusive", true)
				cloneDone, dropDone := make(chan error, 1), make(chan error, 1)
				cloneJoined, dropJoined, dropStarted := false, false, false
				defer func() {
					cleanupCtx, stop := context.WithTimeout(context.Background(), time.Minute)
					defer stop()
					for _, id := range []uint32{cloneID, dropID} {
						_, _ = db.ExecContext(cleanupCtx, fmt.Sprintf("kill query %d", id))
					}
					pacer.releaseOnce.Do(func() { close(pacer.release) })
					for _, pending := range []struct {
						done <-chan error
						wait bool
					}{{cloneDone, !cloneJoined}, {dropDone, dropStarted && !dropJoined}} {
						if pending.wait {
							select {
							case <-pending.done:
							case <-cleanupCtx.Done():
								t.Error("source lifecycle action did not terminate")
							}
						}
					}
					pacer.restore()
				}()
				go func() {
					_, err := clone.ExecContext(ctx, "data branch create database "+target+" from "+source)
					cloneDone <- err
				}()
				pacer.waitFired(t, "clone protection pin before concurrent source DROP")
				dropStarted = true
				go func() { _, err := drop.ExecContext(ctx, "drop database "+source); dropDone <- err }()
				packer := types.NewPacker()
				packer.EncodeUint32(0)
				packer.EncodeStringType([]byte(source))
				// serial(account, name) is a varlena primary key. lockop encodes
				// that value again; mo_locks exposes the physical outer encoding.
				catalogKey := append([]byte(nil), packer.Bytes()...)
				packer.Reset()
				packer.EncodeStringType(catalogKey)
				key := hex.EncodeToString(packer.Bytes())
				packer.Close()
				waiter := "select count(*) from mo_catalog.mo_locks where txn_id<>'' and lock_wait<>'' and lock_content='" + key + "' and table_id in (select cast(rel_id as char) from mo_catalog.mo_tables where reldatabase='mo_catalog' and relname='mo_database')"
				deadline := time.Now().Add(30 * time.Second)
				for count(waiter) == 0 {
					require.True(t, time.Now().Before(deadline), "DROP must queue on this exact source database key")
					select {
					case <-ctx.Done():
						t.Fatal("source DROP waiter observation expired")
					case <-time.After(20 * time.Millisecond):
					}
				}
				if abort {
					exec(fmt.Sprintf("kill query %d", cloneID))
				}
				pacer.resume(t)
				select {
				case err := <-cloneDone:
					cloneJoined = true
					if abort {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
					}
				case <-ctx.Done():
					t.Fatal("clone did not finish")
				}
				select {
				case err := <-dropDone:
					dropJoined = true
					require.NoError(t, err)
				case <-ctx.Done():
					t.Fatal("DROP did not finish")
				}
				require.Zero(t, count("select count(*) from mo_catalog.mo_database where datname='"+source+"'"))
				if abort {
					require.Zero(t, count("select count(*) from mo_catalog.mo_database where datname='"+target+"'"))
					require.Zero(t, count("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='"+source+"'"))
				} else {
					for _, table := range []string{"a", "b"} {
						require.Equal(t, 30, count("select sum(v) from "+target+"."+table))
					}
					require.Equal(t, 2, count("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='"+source+"'"))
				}
			})
			if t.Failed() {
				return
			}
		}
	})
}
