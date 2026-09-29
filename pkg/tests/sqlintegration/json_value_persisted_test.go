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
	"database/sql"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// Override only this CN's read of the authoring gate. Heartbeat publication
// must not race a test's injected phase back to the admitted floor.
type jsonValueAdmissionRuntime struct {
	moruntime.Runtime
	authoring atomic.Int64
}

func (r *jsonValueAdmissionRuntime) GetGlobalVariables(name string) (any, bool) {
	if name == moruntime.PersistedExpressionProtocolAuthoringFloor {
		return r.authoring.Load(), true
	}
	return r.Runtime.GetGlobalVariables(name)
}

// This exercises production SQL/catalog publication with an injected admission
// state. It does not substitute for admission tests with real old binaries.
func TestJSONValuePersistedPublication(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		_, err = conn.ExecContext(ctx, "create database json_value_publication")
		require.NoError(t, err)
		defer cleanupSQLIntegration(t, cn, "drop database if exists json_value_publication")
		_, err = conn.ExecContext(ctx, "use json_value_publication")
		require.NoError(t, err)
		rt := moruntime.ServiceRuntime(cn.ServiceID())
		_, present := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor)
		require.True(t, present, "production CN must initialize authoring admission")
		admission := &jsonValueAdmissionRuntime{Runtime: rt}
		admission.authoring.Store(defines.MORPCVersion99)
		moruntime.SetupServiceBasedRuntime(cn.ServiceID(), admission)
		wrapped := true
		restore := func() {
			if wrapped {
				moruntime.SetupServiceBasedRuntime(cn.ServiceID(), rt)
				wrapped = false
			}
		}
		defer restore()
		statements := []string{
			`create table jv_default (v bigint default (json_value('1', '$' returning signed)))`,
			`create table jv_generated (doc json, v bigint generated always as (json_value(doc, '$' returning signed)) stored, key v_idx(v))`,
			`create table jv_check (doc json, check (json_value(doc, '$' returning signed) > 0))`,
			`create view jv_view as select json_value('1', '$' returning signed) as v`,
			`create view jv_fold as select 1 as v where 1 between json_value('1', '$' returning signed) and 2`,
		}
		for _, statement := range statements {
			_, err = conn.ExecContext(ctx, statement)
			require.ErrorContains(t, err, "protocol version 101", statement)
			var count int
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase='json_value_publication'").Scan(&count))
			require.Zero(t, count, "failed authoring must not leave tables, views or index tables")
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_columns where att_database='json_value_publication'").Scan(&count))
			require.Zero(t, count, "failed authoring must not leave generated/default column metadata")
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_indexes where database_id in (select dat_id from mo_catalog.mo_database where datname='json_value_publication')").Scan(&count))
			require.Zero(t, count, "failed authoring must not leave index metadata")
		}
		// Legacy expressions remain publishable at the predecessor floor.
		_, err = conn.ExecContext(ctx, `create view jv_legacy as select json_value('{"v":1}', '$.v') as v`)
		require.NoError(t, err)
		admission.authoring.Store(defines.MORPCVersion101)
		for _, statement := range statements {
			_, err = conn.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		_, err = conn.ExecContext(ctx, `insert into jv_generated(doc) values ('1')`)
		require.NoError(t, err)
		var value int64
		require.NoError(t, conn.QueryRowContext(ctx, `select v from jv_generated force index(v_idx) where v=1`).Scan(&value))
		require.Equal(t, int64(1), value)
		// Read admission and write admission have distinct responsibilities.
		admission.authoring.Store(defines.MORPCVersion99)
		require.NoError(t, conn.QueryRowContext(ctx, `select v from jv_view`).Scan(&value))
		require.Equal(t, int64(1), value)
		_, err = conn.ExecContext(ctx, `alter view jv_view as select json_value('2', '$' returning signed) as v`)
		require.ErrorContains(t, err, "protocol version 101")
		require.NoError(t, conn.QueryRowContext(ctx, `select v from jv_view`).Scan(&value))
		require.Equal(t, int64(1), value, "failed ALTER must preserve published view")

		// Restart this fixture's real CN and rebind from catalog metadata. This
		// remains a single-binary test; mixed binaries and TN restart are QA gates.
		restore()
		require.NoError(t, conn.Close())
		require.NoError(t, db.Close())
		require.NoError(t, cn.Close())
		require.NoError(t, cn.Start())
		restarted, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/json_value_publication", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer restarted.Close()
		for _, query := range []string{
			`select v from jv_view`,
			`select v from jv_fold`,
			`select v from jv_generated force index(v_idx) where v=1`,
		} {
			require.NoError(t, restarted.QueryRowContext(ctx, query).Scan(&value), query)
			require.Equal(t, int64(1), value)
		}
		_, err = restarted.ExecContext(ctx, `insert into jv_generated(doc) values ('2')`)
		require.NoError(t, err)
		require.NoError(t, restarted.QueryRowContext(ctx, `select v from jv_generated force index(v_idx) where v=2`).Scan(&value))
		require.Equal(t, int64(2), value)
	})
}
