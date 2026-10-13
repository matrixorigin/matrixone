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

package dml

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// Issue #29405: HEX(BIT) inserts a private CAST target-type marker. Folding a
// scan predicate must preserve that marker for the remote expression consumer.
// Reuse the shared two-CN fixture and persist just five rows (six after reuse).
func TestHexBitFilterRemote(t *testing.T) {
	var invalidationErr error
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		peer, err := c.GetCNService(1)
		require.NoError(t, err)
		cluster := clusterservice.GetMOCluster(cn.ServiceID())
		inventory, ok := cluster.(cnWorkStateInventory)
		require.True(t, ok)
		refresher, ok := cluster.(clusterservice.AuthoritativeRefresher)
		require.True(t, ok)
		readinessCtx, cancelReadiness := context.WithTimeout(ctx, 30*time.Second)
		defer cancelReadiness()
		readiness, err := waitForCNReadiness(readinessCtx, cnWorkStatePollInterval, inventory, refresher, cn.ServiceID(), peer.ServiceID())
		cancelReadiness()
		require.NoError(t, err)

		db := openRetestSQLDB(t, c)
		defer db.Close()
		name := strings.ToLower(testutils.GetDatabaseName(t))
		defer func() {
			if invalidationErr == nil {
				cleanupTestDatabases(t, db, name)
			}
		}()
		execSQLDB(t, ctx, db, "create database "+name)
		execSQLDB(t, ctx, db, "use "+name)
		execSQLDB(t, ctx, db, "create table bit_probe(v bit(8))")
		execSQLDB(t, ctx, db, "insert into bit_probe values(170),(187),(null)")
		func() {
			execSQLDB(t, ctx, db, "set @bit_value=b'10101010'")
			execSQLDB(t, ctx, db, "prepare bit_text from 'insert into bit_probe values (?)'")
			defer func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cleanupCancel()
				if _, err := db.ExecContext(cleanupCtx, "deallocate prepare bit_text"); err != nil {
					t.Errorf("deallocate text prepared insert: %v", err)
				}
			}()
			execSQLDB(t, ctx, db, "execute bit_text using @bit_value")
		}()
		insert, err := db.PrepareContext(ctx, "insert into bit_probe values (?)")
		require.NoError(t, err)
		defer insert.Close()
		_, err = insert.ExecContext(ctx, []byte{170})
		require.NoError(t, err)
		predicate, err := db.PrepareContext(ctx, "select count(*) from bit_probe where hex(v)=?")
		require.NoError(t, err)
		defer predicate.Close()

		for _, want := range []int{3, 4} {
			if want == 4 {
				_, err = insert.ExecContext(ctx, 170)
				require.NoError(t, err)
			}
			execSQLDB(t, ctx, db, "select mo_ctl('dn','flush','"+name+".bit_probe')")
			stateErr := withCNDraining(ctx, inventory, refresher, cn.ServiceID(),
				[]string{cn.ServiceID(), peer.ServiceID()},
				func(err error) { invalidationErr = err }, func() {
					oldForce := plan.GetForceScanOnMultiCN()
					plan.SetForceScanOnMultiCN(true)
					defer plan.SetForceScanOnMultiCN(oldForce)
					const query = "select count(*) from bit_probe where hex(v)='AA'"
					var count int
					require.NoError(t, db.QueryRowContext(ctx, query).Scan(&count))
					require.Equal(t, want, count, "text, binary byte, and numeric BIT bindings must agree")
					physical, err := testutils.QueryTextResult(ctx, db, "explain phyplan analyze "+query)
					require.NoError(t, err)
					require.NotEmpty(t, readiness.peerAddr)
					require.Contains(t, physical.Text, readiness.peerAddr, "the predicate must execute through a remote consumer")
					// Reuse the same prepared statement before and after insertion;
					// changing the search value must not reuse a folded result.
					for _, tc := range []struct {
						value any
						want  int
					}{{"AA", want}, {"BB", 1}, {nil, 0}, {"AA", want}} {
						require.NoError(t, predicate.QueryRowContext(ctx, tc.value).Scan(&count))
						require.Equal(t, tc.want, count)
					}
					require.NoError(t, db.QueryRowContext(ctx, "select count(*) from bit_probe where hex(v) is null").Scan(&count))
					require.Equal(t, 1, count)
				})
			require.NoError(t, stateErr)
		}
	})
}
