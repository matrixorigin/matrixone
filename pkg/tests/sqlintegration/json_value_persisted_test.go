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
		textComposed := strings.Replace(composed, "doc json", "doc varchar(40)", 1)
		// Same-binary injected floors check that the existing functional-index
		// authoring gate survives the composition; they do not emulate an old CN.
		admission.protocol.Store(defines.MORPCVersion129)
		admission.readFloor.Store(defines.MORPCVersion129)
		admission.authoring.Store(defines.MORPCVersion106)
		before := catalogCounts()
		_, err = conn.ExecContext(ctx, "create table rejected_floor"+textComposed)
		require.ErrorContains(t, err, "protocol version 107")
		require.Equal(t, before, catalogCounts(), "rejected index must not publish metadata")
		// JSON_VALUE by itself is authorable below the functional-index floor.
		exec("create view legacy_control as select json_value('1','$') as v")
		exec("create view contract_control as select json_value('1','$' returning signed) as v")
		var contractValue int64
		require.NoError(t, conn.QueryRowContext(ctx, "select v from contract_control").Scan(&contractValue))
		require.Equal(t, int64(1), contractValue)
		admission.authoring.Store(defines.MORPCVersion107)
		exec("create table text_composed" + textComposed)
		exec("insert into text_composed(id,doc,name) values(1,'1','ABC')")
		require.NoError(t, conn.QueryRowContext(ctx, "select v from text_composed force index(fi) where lower(name)='abc'").Scan(&contractValue))
		require.Equal(t, int64(1), contractValue)
		// JSON-typed input independently requires the existing v129 opaque-text
		// capability; the functional-index floor is not a substitute for it.
		admission.authoring.Store(defines.MORPCVersion128)
		before = catalogCounts()
		_, err = conn.ExecContext(ctx, "create table rejected_json"+composed)
		require.ErrorContains(t, err, "protocol version 129")
		require.Equal(t, before, catalogCounts())
		admission.authoring.Store(defines.MORPCVersion129)
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

// This exercises production SQL/catalog publication with injected same-binary
// floor values. Supported release-data upgrade remains a separate test.
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
		// MEDIUMINT and JSON_VALUE both execute with the existing v110 floor.
		admission.protocol.Store(defines.MORPCVersion110)
		admission.readFloor.Store(defines.MORPCVersion110)
		admission.authoring.Store(defines.MORPCVersion110)
		statements := []string{
			`create table jv_default (v bigint default (json_value('1', '$' returning signed)))`,
			`create table jv_generated (doc json, v bigint generated always as (json_value(doc, '$' returning signed)) stored, key v_idx(v))`,
			`create table jv_check (doc json, check (json_value(doc, '$' returning signed) > 0))`,
			`create view jv_view as select json_value('1', '$' returning signed) as v`,
			`create view jv_fold as select 1 as v where 1 between json_value('1', '$' returning signed) and 2`,
			`create view jv_calendar_json as select week(cast('2024-12-31' as date), 3) as w, dayofyear(cast('2024-12-31' as date)) as d, json_value('1', '$' returning signed) as v`,
			`create view jv_least_json as select json_value('1', '$' returning signed) as j, least('a' collate utf8mb4_unicode_ci, 'B' collate utf8mb4_unicode_ci) as v`,
		}
		// VARCHAR-input expressions add no JSON_VALUE-specific floor. JSON
		// column inputs retain the independently landed opaque-text floor.
		for _, statement := range []string{statements[0], statements[3], statements[4]} {
			_, err = conn.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		admission.protocol.Store(defines.MORPCVersion129)
		admission.readFloor.Store(defines.MORPCVersion129)
		admission.authoring.Store(defines.MORPCVersion128)
		for _, statement := range []string{statements[1], statements[2]} {
			_, err = conn.ExecContext(ctx, statement)
			require.ErrorContains(t, err, "protocol version 129", statement)
		}
		admission.authoring.Store(defines.MORPCVersion129)
		for _, statement := range []string{statements[1], statements[2]} {
			_, err = conn.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		_, err = conn.ExecContext(ctx, `insert into jv_default values (default)`)
		require.NoError(t, err)
		var defaultValue int64
		require.NoError(t, conn.QueryRowContext(ctx, `select v from jv_default`).Scan(&defaultValue))
		require.Equal(t, int64(1), defaultValue)
		err = conn.QueryRowContext(ctx, `select json_value('"bad"', '$' returning signed error on error)`).Scan(&defaultValue)
		require.Error(t, err)
		require.NoError(t, conn.QueryRowContext(ctx, `select json_value('"bad"', '$' returning signed default 7 on error)`).Scan(&defaultValue))
		require.Equal(t, int64(7), defaultValue)
		// MEDIUMINT still executes at its independently landed v110 floor.
		admission.protocol.Store(defines.MORPCVersion110)
		admission.readFloor.Store(defines.MORPCVersion110)
		admission.authoring.Store(defines.MORPCVersion110)
		_, err = conn.ExecContext(ctx, `create table jv_mediumint_control (v mediumint, u mediumint unsigned)`)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, `insert into jv_mediumint_control values (8388607, 16777215)`)
		require.NoError(t, err)
		var mediumSigned, mediumUnsigned int64
		require.NoError(t, conn.QueryRowContext(ctx, `select v,u from jv_mediumint_control`).Scan(&mediumSigned, &mediumUnsigned))
		require.Equal(t, int64(8388607), mediumSigned)
		require.Equal(t, int64(16777215), mediumUnsigned)
		// Legacy expressions remain publishable at the predecessor floor.
		_, err = conn.ExecContext(ctx, `create view jv_legacy as select json_value('{"v":1}', '$.v') as v`)
		require.NoError(t, err)
		// A combined view must still reject the independently gated calendar
		// expression without publishing catalog metadata.
		admission.protocol.Store(defines.MORPCVersion111)
		admission.readFloor.Store(defines.MORPCVersion111)
		admission.authoring.Store(defines.MORPCVersion111)
		_, err = conn.ExecContext(ctx, statements[5])
		require.ErrorContains(t, err, "protocol version 112")
		var published int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase='json_value_publication' and relname='jv_calendar_json'").Scan(&published))
		require.Zero(t, published)
		admission.protocol.Store(defines.MORPCVersion112)
		admission.readFloor.Store(defines.MORPCVersion112)
		admission.authoring.Store(defines.MORPCVersion112)
		_, err = conn.ExecContext(ctx, statements[5])
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, `create view jv_calendar_control as select week(cast('2024-12-31' as date), 3) as w, dayofyear(cast('2024-12-31' as date)) as d, json_value('1', '$') as v`)
		require.NoError(t, err)
		var controlWeek, controlDay int64
		var controlValue string
		require.NoError(t, conn.QueryRowContext(ctx, `select w,d,v from jv_calendar_control`).Scan(&controlWeek, &controlDay, &controlValue))
		require.Equal(t, int64(1), controlWeek)
		require.Equal(t, int64(366), controlDay)
		require.Equal(t, "1", controlValue)
		// A composed LEAST view retains its independent v117 authoring floor.
		admission.protocol.Store(defines.MORPCVersion116)
		admission.readFloor.Store(defines.MORPCVersion116)
		admission.authoring.Store(defines.MORPCVersion116)
		_, err = conn.ExecContext(ctx, statements[6])
		require.ErrorContains(t, err, "protocol version 117")
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase='json_value_publication' and relname='jv_least_json'").Scan(&published))
		require.Zero(t, published)
		admission.protocol.Store(defines.MORPCVersion117)
		admission.readFloor.Store(defines.MORPCVersion117)
		admission.authoring.Store(defines.MORPCVersion117)
		_, err = conn.ExecContext(ctx, statements[6])
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, `create view jv_least_control as select least('a' collate utf8mb4_unicode_ci, 'B' collate utf8mb4_unicode_ci) as v`)
		require.NoError(t, err)
		require.NoError(t, conn.QueryRowContext(ctx, `select v from jv_least_control`).Scan(&controlValue))
		require.Equal(t, "a", controlValue)
		admission.protocol.Store(defines.MORPCVersion129)
		admission.readFloor.Store(defines.MORPCVersion129)
		admission.authoring.Store(defines.MORPCVersion129)
		_, err = conn.ExecContext(ctx, `insert into jv_generated(doc) values ('1')`)
		require.NoError(t, err)
		var value int64
		require.NoError(t, conn.QueryRowContext(ctx, `select j,v from jv_least_json`).Scan(&value, &controlValue))
		require.Equal(t, int64(1), value)
		require.Equal(t, "a", controlValue)
		require.NoError(t, conn.QueryRowContext(ctx, `select v from jv_generated force index(v_idx) where v=1`).Scan(&value))
		require.Equal(t, int64(1), value)
		admission.readFloor.Store(defines.MORPCVersion128)
		// Use a distinct statement so the injected read floor is checked while
		// rebinding, rather than reusing the plan cached at the higher floor.
		require.ErrorContains(t, conn.QueryRowContext(ctx, `select v from jv_generated force index(v_idx) where v=1 and v>=0`).Scan(&value), "protocol version 129")
		admission.readFloor.Store(defines.MORPCVersion129)
		var week, day int64
		require.NoError(t, conn.QueryRowContext(ctx, `select w,d,v from jv_calendar_json`).Scan(&week, &day, &value))
		require.Equal(t, int64(1), week)
		require.Equal(t, int64(366), day)
		require.Equal(t, int64(1), value)
		// Standalone JSON_VALUE has no read-side feature floor. Composed
		// persisted views still enforce the independent calendar/LEAST floors.
		for _, floor := range []int64{defines.MORPCVersion110, defines.MORPCVersion117, defines.MORPCVersion134} {
			admission.readFloor.Store(floor)
			require.NoError(t, conn.QueryRowContext(ctx, `select v from jv_view`).Scan(&value))
			require.Equal(t, int64(1), value)
		}
		admission.readFloor.Store(defines.MORPCVersion111)
		require.ErrorContains(t, conn.QueryRowContext(ctx, `select v from jv_calendar_json`).Scan(&value), "protocol version 112")
		admission.readFloor.Store(defines.MORPCVersion116)
		require.ErrorContains(t, conn.QueryRowContext(ctx, `select j from jv_least_json`).Scan(&value), "protocol version 117")
		admission.readFloor.Store(defines.MORPCVersion117)
		// Failed ALTER involving an independently gated expression preserves
		// the original view. A JSON_VALUE-only ALTER succeeds at the same floor.
		admission.authoring.Store(defines.MORPCVersion111)
		_, err = conn.ExecContext(ctx, `alter view jv_view as select json_value('2', '$' returning signed) + week(cast('2024-12-31' as date),3) as v`)
		require.ErrorContains(t, err, "protocol version 112")
		require.NoError(t, conn.QueryRowContext(ctx, `select v from jv_view`).Scan(&value))
		require.Equal(t, int64(1), value, "failed ALTER must preserve published view")
		_, err = conn.ExecContext(ctx, `alter view jv_view as select json_value('2', '$' returning signed) as v`)
		require.NoError(t, err)
		require.NoError(t, conn.QueryRowContext(ctx, `select v from jv_view`).Scan(&value))
		require.Equal(t, int64(2), value)
		_, err = conn.ExecContext(ctx, `alter view jv_view as select json_value('1', '$' returning signed) as v`)
		require.NoError(t, err)

		t.Run("legacy and contract expression reuse", func(t *testing.T) {
			protocol, readFloor, authoring := admission.protocol.Load(), admission.readFloor.Load(), admission.authoring.Load()
			defer func() {
				admission.protocol.Store(protocol)
				admission.readFloor.Store(readFloor)
				admission.authoring.Store(authoring)
			}()
			admission.protocol.Store(defines.MORPCVersion129)
			admission.readFloor.Store(defines.MORPCVersion129)
			admission.authoring.Store(defines.MORPCVersion129)
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
			// Publish both forms on the current binary with no JSON_VALUE floor;
			// then reuse the stored plans under more than one injected floor.
			admission.protocol.Store(defines.MORPCVersion129)
			admission.readFloor.Store(defines.MORPCVersion129)
			admission.authoring.Store(defines.MORPCVersion129)
			for _, query := range views {
				_, err := conn.ExecContext(ctx, query)
				require.NoError(t, err, query)
			}
			for _, floor := range []int64{defines.MORPCVersion129, defines.MORPCVersion134} {
				admission.protocol.Store(floor)
				admission.readFloor.Store(floor)
				require.NoError(t, conn.QueryRowContext(ctx, "select n from jv_reuse_projection where v is null").Scan(&count))
				require.Equal(t, int64(1), count)
				require.NoError(t, conn.QueryRowContext(ctx, "select n from jv_reuse_having").Scan(&count))
				require.Equal(t, int64(1), count)
			}
			admission.authoring.Store(defines.MORPCVersion129)
			require.NoError(t, conn.QueryRowContext(ctx, "select n from jv_reuse_projection where v is null").Scan(&count))
			require.Equal(t, int64(1), count)
			require.NoError(t, conn.QueryRowContext(ctx, "select n from jv_reuse_having").Scan(&count))
			require.Equal(t, int64(1), count)
			admission.protocol.Store(0)
			admission.readFloor.Store(0)
		})

		// Restart this fixture's real CN and rebind from catalog metadata. This
		// remains a single-binary test; full storage restart is a separate gate.
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
			`select v from jv_calendar_json`,
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
		require.NoError(t, restartedConn.QueryRowContext(ctx, `select v,u from jv_mediumint_control`).Scan(&mediumSigned, &mediumUnsigned))
		require.Equal(t, int64(8388607), mediumSigned, "landed MEDIUMINT data survives the same real CN restart")
		require.Equal(t, int64(16777215), mediumUnsigned)
		require.NoError(t, restartedConn.QueryRowContext(ctx, `select w,d,v from jv_calendar_control`).Scan(&controlWeek, &controlDay, &controlValue))
		require.Equal(t, int64(1), controlWeek)
		require.Equal(t, int64(366), controlDay)
		require.Equal(t, "1", controlValue, "landed calendar and legacy JSON_VALUE view survives the same real CN restart")
	})
}
