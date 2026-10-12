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
	"strings"
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
	protocol  atomic.Int64
	readFloor atomic.Int64
}

func (r *jsonValueAdmissionRuntime) GetGlobalVariables(name string) (any, bool) {
	if name == moruntime.MOProtocolVersion && r.protocol.Load() != 0 {
		return r.protocol.Load(), true
	}
	if name == moruntime.PersistedExpressionProtocolFloor && r.readFloor.Load() != 0 {
		return r.readFloor.Load(), true
	}
	if name == moruntime.PersistedExpressionProtocolAuthoringFloor {
		return r.authoring.Load(), true
	}
	return r.Runtime.GetGlobalVariables(name)
}

// Both capabilities share a table's generated-column layout and COPY rebuild.
// Independent supported owners compose; this does not widen the functional
// index expression whitelist or simulate an actual predecessor executable.
func TestJSONValueFunctionalIndexComposition(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(query string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		exec("create database json_value_functional_composition")
		defer cleanupSQLIntegration(t, cn, "drop database if exists json_value_functional_composition")
		exec("use json_value_functional_composition")
		rt := moruntime.ServiceRuntime(cn.ServiceID())
		admission := &jsonValueAdmissionRuntime{Runtime: rt}
		moruntime.SetupServiceBasedRuntime(cn.ServiceID(), admission)
		defer moruntime.SetupServiceBasedRuntime(cn.ServiceID(), rt)
		catalogCounts := func() [3]int {
			t.Helper()
			queries := []string{
				"select count(*) from mo_catalog.mo_tables where reldatabase='json_value_functional_composition'",
				"select count(*) from mo_catalog.mo_columns where att_database='json_value_functional_composition'",
				"select count(*) from mo_catalog.mo_indexes where database_id in (select dat_id from mo_catalog.mo_database where datname='json_value_functional_composition')",
			}
			var counts [3]int
			for i, query := range queries {
				require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&counts[i]))
			}
			return counts
		}
		composed := "(id int primary key,doc json,name varchar(40),v bigint generated always as (json_value(doc,'$' returning signed)) stored,index fi ((lower(name)),id),key v_idx(v))"
		for _, protocol := range []int64{defines.MORPCVersion107, defines.MORPCVersion108, defines.MORPCVersion109, defines.MORPCVersion110} {
			admission.protocol.Store(protocol)
			admission.readFloor.Store(protocol)
			admission.authoring.Store(defines.MORPCVersion107)
			// Functional107 and bare legacy JSON_VALUE remain authorable even
			// when the new JSON_VALUE authoring floor is not enabled.
			exec(fmt.Sprintf("create table fi_control_%d(id int primary key,name varchar(40),index fi ((lower(name)),id))", protocol))
			exec(fmt.Sprintf("create view legacy_control_%d as select json_value('1','$') as v", protocol))
			before := catalogCounts()
			_, err := conn.ExecContext(ctx, fmt.Sprintf("create table rejected_%d%s", protocol, composed))
			require.ErrorContains(t, err, "protocol version 110")
			require.Equal(t, before, catalogCounts(), "rejected composition must not publish table, column or index metadata")
		}
		admission.protocol.Store(0)
		admission.readFloor.Store(0)
		admission.authoring.Store(defines.MORPCVersion110)
		for _, query := range []string{
			"create table rejected_direct(doc json,index fi ((json_value(doc,'$' returning signed))))",
			"create table rejected_dependency(doc json,v bigint generated always as (json_value(doc,'$' returning signed)) stored,index fi ((v+1)))",
		} {
			before := catalogCounts()
			_, err := conn.ExecContext(ctx, query)
			require.Error(t, err, "unsupported functional expression must not become supported through composition")
			require.Equal(t, before, catalogCounts())
		}
		exec("create table composed" + composed)
		exec("insert into composed(id,doc,name) values(1,'1','ABC'),(2,'2','DEF')")
		assertKeys := func(name string, value int64, want int) {
			t.Helper()
			for _, query := range []string{
				fmt.Sprintf("select count(*) from composed force index(fi) where lower(name)='%s' and v=%d", name, value),
				fmt.Sprintf("select count(*) from composed force index(v_idx) where v=%d and lower(name)='%s'", value, name),
				fmt.Sprintf("select count(*) from composed ignore index(fi,v_idx) where v=%d and lower(name)='%s'", value, name),
			} {
				var count int
				require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&count), query)
				require.Equal(t, want, count, query)
			}
		}
		assertIndexPath := func(name string) {
			t.Helper()
			rows, err := conn.QueryContext(ctx, "explain select id from composed force index(fi) where lower(name)='"+name+"'")
			require.NoError(t, err)
			defer rows.Close()
			var lines []string
			for rows.Next() {
				var line string
				require.NoError(t, rows.Scan(&line))
				lines = append(lines, line)
			}
			require.NoError(t, rows.Err())
			require.Contains(t, strings.Join(lines, "\n"), "Index Table Scan on composed.fi")
		}
		assertKeys("abc", 1, 1)
		assertIndexPath("abc")
		exec("update composed set doc='3',name='GHI' where id=1")
		assertKeys("abc", 1, 0)
		assertKeys("ghi", 3, 1)
		exec("alter table composed add column extra int default 7, algorithm=copy")
		assertKeys("ghi", 3, 1)
		assertKeys("def", 2, 1)
		assertIndexPath("ghi")
		var tableName, ddl string
		require.NoError(t, conn.QueryRowContext(ctx, "show create table composed").Scan(&tableName, &ddl))
		require.Contains(t, strings.ToLower(ddl), "json_value")
		require.Contains(t, ddl, "lower(`name`)")
		exec("update composed set doc='4',name='JKL' where id=1")
		exec("insert into composed(id,doc,name) values(3,'5','MNO')")
		assertKeys("ghi", 3, 0)
		assertKeys("jkl", 4, 1)
		assertKeys("mno", 5, 1)
		assertIndexPath("jkl")
	})
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
		admission.authoring.Store(defines.MORPCVersion107)
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
			require.ErrorContains(t, err, "protocol version 110", statement)
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
		admission.authoring.Store(defines.MORPCVersion110)
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
		admission.authoring.Store(defines.MORPCVersion107)
		require.NoError(t, conn.QueryRowContext(ctx, `select v from jv_view`).Scan(&value))
		require.Equal(t, int64(1), value)
		_, err = conn.ExecContext(ctx, `alter view jv_view as select json_value('2', '$' returning signed) as v`)
		require.ErrorContains(t, err, "protocol version 110")
		require.NoError(t, conn.QueryRowContext(ctx, `select v from jv_view`).Scan(&value))
		require.Equal(t, int64(1), value, "failed ALTER must preserve published view")

		t.Run("legacy and contract expression reuse", func(t *testing.T) {
			protocol, readFloor, authoring := admission.protocol.Load(), admission.readFloor.Load(), admission.authoring.Load()
			defer func() {
				admission.protocol.Store(protocol)
				admission.readFloor.Store(readFloor)
				admission.authoring.Store(authoring)
			}()
			var sqlMode string
			require.NoError(t, conn.QueryRowContext(ctx, "select @@sql_mode").Scan(&sqlMode))
			_, err := conn.ExecContext(ctx, "set sql_mode = ''")
			require.NoError(t, err)
			defer func() {
				_, err := conn.ExecContext(ctx, "set sql_mode = ?", sqlMode)
				require.NoError(t, err)
			}()
			_, err = conn.ExecContext(ctx, "create table jv_reuse(doc json)")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "insert into jv_reuse values (null)")
			require.NoError(t, err)
			var got sql.NullString
			var count int64
			require.NoError(t, conn.QueryRowContext(ctx,
				`select json_value(doc, '$['), count(*) from jv_reuse group by json_value(doc, '$[')`).Scan(&got, &count))
			require.False(t, got.Valid)
			require.Equal(t, int64(1), count)
			for _, query := range []string{
				`select json_value(doc, '$[' null on error), count(*) from jv_reuse group by json_value(doc, '$[')`,
				`select count(*) from jv_reuse group by json_value(doc, '$[') having json_value(doc, '$[' null on error) is null`,
			} {
				err := conn.QueryRowContext(ctx, query).Scan(&got, &count)
				require.ErrorContains(t, err, "invalid path expression", query)
			}
			_, err = conn.ExecContext(ctx, "delete from jv_reuse")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "insert into jv_reuse values (json_quote(repeat('x',512))), (json_quote(repeat('x',513)))")
			require.NoError(t, err)
			for _, width := range []int{512, 513} {
				query := fmt.Sprintf(`select json_value(doc, '$' null on error), count(*) from jv_reuse where length(json_value(doc, '$'))=%d group by json_value(doc, '$')`, width)
				require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&got, &count))
				require.Equal(t, int64(1), count)
				require.Equal(t, width == 512, got.Valid)
				if got.Valid {
					require.Equal(t, strings.Repeat("x", width), got.String)
				}
			}
			require.NoError(t, conn.QueryRowContext(ctx,
				`select count(*) from jv_reuse group by json_value(doc, '$') having json_value(doc, '$' null on error) is null`).Scan(&count))
			require.Equal(t, int64(1), count)
			// Equivalent clause forms must still reuse a grouped expression.
			require.NoError(t, conn.QueryRowContext(ctx,
				`select json_value(doc, '$' null on error), count(*) from jv_reuse where length(json_value(doc, '$'))=512 group by json_value(doc, '$' null on empty) having json_value(doc, '$' null on empty null on error) is not null`).Scan(&got, &count))
			require.Equal(t, strings.Repeat("x", 512), got.String)
			require.Equal(t, int64(1), count)
			views := []string{
				`create view jv_reuse_projection as select json_value(doc, '$' null on error) as v, count(*) as n from jv_reuse group by json_value(doc, '$')`,
				`create view jv_reuse_having as select count(*) as n from jv_reuse group by json_value(doc, '$') having json_value(doc, '$' null on error) is null`,
			}
			for _, state := range []struct{ protocol, authoring int64 }{
				{defines.MORPCVersion109, defines.MORPCVersion109},
				{defines.MORPCVersion110, defines.MORPCVersion109},
			} {
				admission.protocol.Store(state.protocol)
				admission.readFloor.Store(state.protocol)
				admission.authoring.Store(state.authoring)
				for _, query := range views {
					_, err := conn.ExecContext(ctx, query)
					require.ErrorContains(t, err, "protocol version 110", query)
					var published int
					require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase='json_value_publication' and relname in ('jv_reuse_projection','jv_reuse_having')").Scan(&published))
					require.Zero(t, published, "rejected mixed-form views must not be published")
				}
			}
			admission.protocol.Store(defines.MORPCVersion110)
			admission.readFloor.Store(defines.MORPCVersion110)
			admission.authoring.Store(defines.MORPCVersion110)
			for _, query := range views {
				_, err := conn.ExecContext(ctx, query)
				require.NoError(t, err, query)
			}
			admission.authoring.Store(defines.MORPCVersion109)
			require.NoError(t, conn.QueryRowContext(ctx, "select n from jv_reuse_projection where v is null").Scan(&count))
			require.Equal(t, int64(1), count)
			require.NoError(t, conn.QueryRowContext(ctx, "select n from jv_reuse_having").Scan(&count))
			require.Equal(t, int64(1), count)
			admission.protocol.Store(0)
			admission.readFloor.Store(0)
		})

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
		restartedConn, err := restarted.Conn(ctx)
		require.NoError(t, err)
		defer restartedConn.Close()
		// Mixed legacy/contract grouping uses permissive SQL mode. Rebind the
		// stored views under the same mode on this new, pinned connection.
		var restartedSQLMode string
		require.NoError(t, restartedConn.QueryRowContext(ctx, "select @@sql_mode").Scan(&restartedSQLMode))
		_, err = restartedConn.ExecContext(ctx, "set sql_mode = ''")
		require.NoError(t, err)
		defer func() {
			_, err := restartedConn.ExecContext(ctx, "set sql_mode = ?", restartedSQLMode)
			require.NoError(t, err)
		}()
		for _, query := range []string{
			`select v from jv_view`,
			`select v from jv_fold`,
			`select v from jv_generated force index(v_idx) where v=1`,
			`select n from jv_reuse_projection where v is null`,
			`select n from jv_reuse_having`,
		} {
			require.NoError(t, restartedConn.QueryRowContext(ctx, query).Scan(&value), query)
			require.Equal(t, int64(1), value)
		}
		_, err = restartedConn.ExecContext(ctx, `insert into jv_generated(doc) values ('2')`)
		require.NoError(t, err)
		require.NoError(t, restartedConn.QueryRowContext(ctx, `select v from jv_generated force index(v_idx) where v=2`).Scan(&value))
		require.Equal(t, int64(2), value)
	})
}
