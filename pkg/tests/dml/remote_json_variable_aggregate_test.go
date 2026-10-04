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
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestRemoteJSONVariableAggregateTypes(t *testing.T) {
	var fixtureInvalidationErr error
	defer func() {
		if fixtureInvalidationErr != nil {
			t.Errorf("discarding shared two-CN fixture after an unverified work-state transition: %v", fixtureInvalidationErr)
			if err := embed.CloseBaseClusterTests(); err != nil {
				t.Errorf("failed to discard shared two-CN fixture: %v", err)
			}
		}
	}()

	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		peer, err := c.GetCNService(1)
		require.NoError(t, err)
		clusterInventory := clusterservice.GetMOCluster(cn.ServiceID())
		inventory, ok := clusterInventory.(cnWorkStateInventory)
		require.True(t, ok, "CN inventory must support caller-bounded work-state updates")
		refresher, ok := clusterInventory.(clusterservice.AuthoritativeRefresher)
		require.True(t, ok, "CN inventory must support authoritative refresh")
		readinessCtx, cancelReadiness := context.WithTimeout(ctx, 30*time.Second)
		readiness, readinessErr := waitForCNReadiness(
			readinessCtx, cnWorkStatePollInterval, inventory, refresher, cn.ServiceID(), peer.ServiceID())
		cancelReadiness()
		require.NoError(t, readinessErr,
			"last refresh error=%v, admission-ready CNs=%v, normally discoverable CNs=%v",
			readiness.lastRefreshErr, readiness.admissionReady, readiness.normallyDiscoverable)
		peerAddr := readiness.peerAddr
		require.NotEmpty(t, peerAddr)

		db := openRetestSQLDB(t, c)
		defer db.Close()
		name := strings.ToLower(testutils.GetDatabaseName(t))
		defer func() {
			if fixtureInvalidationErr == nil {
				cleanupTestDatabases(t, db, name)
			}
		}()
		execSQLDB(t, ctx, db, "create database "+name)
		execSQLDB(t, ctx, db, "use "+name)
		execSQLDB(t, ctx, db, "create table src(id int)")
		execSQLDB(t, ctx, db, "insert into src values(1),(2)")
		execSQLDB(t, ctx, db, "select mo_ctl('dn','flush','"+name+".src')")

		oldForce := plan.GetForceScanOnMultiCN()
		plan.SetForceScanOnMultiCN(true)
		defer plan.SetForceScanOnMultiCN(oldForce)
		stateErr := withCNDraining(ctx, inventory, refresher, cn.ServiceID(),
			[]string{cn.ServiceID(), peer.ServiceID()},
			func(err error) { fixtureInvalidationErr = err },
			func() {
				execSQLDB(t, ctx, db, `set @remote_json_24799 = cast('{"i":1,"f":1.5}' as json)`)
				const directArray = "select json_arrayagg(@remote_json_24799) from src"
				physical, err := testutils.QueryTextResult(ctx, db, "explain phyplan analyze "+directArray)
				require.NoError(t, err)
				require.Contains(t, physical.Text, peerAddr, "aggregate must execute on the peer CN")
				require.Contains(t, physical.Text, "Magic: Remote")
				var array string
				require.NoError(t, db.QueryRowContext(ctx, directArray).Scan(&array))
				require.JSONEq(t, `[{"i":1,"f":1.5},{"i":1,"f":1.5}]`, array)
				var integerType, floatType string
				require.NoError(t, db.QueryRowContext(ctx,
					"select json_type(json_extract(json_arrayagg(@remote_json_24799),'$[0].i')), "+
						"json_type(json_extract(json_arrayagg(@remote_json_24799),'$[0].f')) from src").
					Scan(&integerType, &floatType))
				require.Equal(t, "INTEGER", integerType)
				require.Equal(t, "DOUBLE", floatType)

				const directObject = "select json_objectagg(cast(id as char), @remote_json_24799) from src"
				physical, err = testutils.QueryTextResult(ctx, db, "explain phyplan analyze "+directObject)
				require.NoError(t, err)
				require.Contains(t, physical.Text, peerAddr)
				var object string
				require.NoError(t, db.QueryRowContext(ctx, directObject).Scan(&object))
				require.JSONEq(t, `{"1":{"i":1,"f":1.5},"2":{"i":1,"f":1.5}}`, object)

				execSQLDB(t, ctx, db, "prepare remote_json_array_24799 from 'select json_type(json_extract(json_arrayagg(?),''$[0]'')) from src'")
				defer func() { execSQLDB(t, ctx, db, "deallocate prepare remote_json_array_24799") }()
				execSQLDB(t, ctx, db, "prepare remote_json_object_24799 from 'select json_type(json_extract(json_objectagg(cast(id as char),?),''$.\"1\"'')) from src'")
				defer func() { execSQLDB(t, ctx, db, "deallocate prepare remote_json_object_24799") }()

				for _, tc := range []struct {
					name, assignment string
					want             sql.NullString
				}{
					{"object", `cast('{"v":1}' as json)`, sql.NullString{String: "OBJECT", Valid: true}},
					{"array", `cast('[1,2]' as json)`, sql.NullString{String: "ARRAY", Valid: true}},
					{"number", `cast('7' as json)`, sql.NullString{String: "INTEGER", Valid: true}},
					{"boolean", `cast('true' as json)`, sql.NullString{String: "BOOLEAN", Valid: true}},
					{"string", `cast('"word"' as json)`, sql.NullString{String: "STRING", Valid: true}},
					{"json_null", `cast('null' as json)`, sql.NullString{String: "NULL", Valid: true}},
					{"sql_null", `null`, sql.NullString{String: "NULL", Valid: true}},
					{"object_after_sql_null", `cast('{"v":2}' as json)`, sql.NullString{String: "OBJECT", Valid: true}},
				} {
					t.Run(tc.name, func(t *testing.T) {
						execSQLDB(t, ctx, db, "set @remote_json_24799 = "+tc.assignment)
						for _, stmt := range []string{"remote_json_array_24799", "remote_json_object_24799"} {
							var got sql.NullString
							err := db.QueryRowContext(ctx, "execute "+stmt+" using @remote_json_24799").Scan(&got)
							require.NoError(t, err, stmt)
							require.Equal(t, tc.want, got, stmt)
						}
					})
				}
			})
		require.NoError(t, stateErr)
	})
}
