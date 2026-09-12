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

// Reuse the shared two-CN fixture; two persisted rows force the real remote
// consumer without introducing a large workload or an additional cluster.
func TestConvRowBasesRemoteFallback(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		peer, err := c.GetCNService(1)
		require.NoError(t, err)
		inventory := clusterservice.GetMOCluster(cn.ServiceID())
		refresher := inventory.(clusterservice.AuthoritativeRefresher)
		var peerAddr string
		require.Eventually(t, func() bool {
			if refresher.Refresh(ctx) != nil {
				return false
			}
			count := 0
			inventory.GetCNService(clusterservice.NewSelector(), func(node metadata.CNService) bool {
				count++
				if node.ServiceID == peer.ServiceID() {
					peerAddr = node.PipelineServiceAddress
				}
				return true
			})
			return count == 2 && peerAddr != ""
		}, 30*time.Second, 100*time.Millisecond)
		db := openRetestSQLDB(t, c)
		defer db.Close()
		name := strings.ToLower(testutils.GetDatabaseName(t))
		defer cleanupTestDatabases(t, db, name)
		execSQLDB(t, ctx, db, "create database "+name)
		execSQLDB(t, ctx, db, "use "+name)
		execSQLDB(t, ctx, db, "create table src(id int,n varchar(20),f bigint)")
		execSQLDB(t, ctx, db, "insert into src values(1,'ff',16),(2,'1010',2)")
		execSQLDB(t, ctx, db, "select mo_ctl('dn','flush','"+name+".src')")
		require.NoError(t, inventory.DebugUpdateCNWorkState(cn.ServiceID(), int(metadata.WorkState_Draining)))
		defer func() {
			restore, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			require.NoError(t, inventory.DebugUpdateCNWorkState(cn.ServiceID(), int(metadata.WorkState_Working)))
			require.NoError(t, refresher.Refresh(restore))
		}()
		require.NoError(t, refresher.Refresh(ctx))
		oldForce := plan.GetForceScanOnMultiCN()
		plan.SetForceScanOnMultiCN(true)
		defer plan.SetForceScanOnMultiCN(oldForce)
		const query = "select group_concat(conv(n,f,10) order by id) from src"
		physical, err := testutils.QueryTextResult(ctx, db, "explain phyplan analyze "+query)
		require.NoError(t, err)
		require.Contains(t, physical.Text, peerAddr)
		var value string
		require.NoError(t, db.QueryRowContext(ctx, query).Scan(&value))
		require.Equal(t, "255,10", value)
		rt := moruntime.ServiceRuntime(peer.ServiceID())
		oldVersion, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion66)
		defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		physical, err = testutils.QueryTextResult(ctx, db, "explain phyplan analyze "+query)
		require.NoError(t, err)
		require.NotContains(t, physical.Text, peerAddr)
		require.NoError(t, db.QueryRowContext(ctx, query).Scan(&value))
		require.Equal(t, "255,10", value)
		stmt, err := db.PrepareContext(ctx, "select conv(?,?,?)")
		require.NoError(t, err)
		defer stmt.Close()
		for _, tc := range []struct {
			n     string
			f, to int64
			want  string
		}{{"ff", 16, 10, "255"}, {"1010", 2, 16, "A"}, {"-10", 10, -16, "-A"}} {
			require.NoError(t, stmt.QueryRowContext(ctx, tc.n, tc.f, tc.to).Scan(&value))
			require.Equal(t, tc.want, value)
		}
	})
}
