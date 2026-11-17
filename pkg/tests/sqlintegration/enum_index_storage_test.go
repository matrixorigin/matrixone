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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func queryEnumStorageRows(t *testing.T, ctx context.Context, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx, query)
	require.NoError(t, err, query)
	defer rows.Close()
	var out []string
	for rows.Next() {
		var value string
		require.NoError(t, rows.Scan(&value))
		out = append(out, value)
	}
	require.NoError(t, rows.Err())
	return out
}

// DDL and DML must write the same complete key and origin value. Correct
// payload ordinals alone would miss orphaned rows under a different tuple tag.
func testEnumIndexStorageParity(t *testing.T, ctx context.Context, db *sql.DB) {
	const enum = "e enum('a','A','') collate utf8mb4_bin"
	for _, tc := range []struct {
		name, columns, values, inlineIndex, addIndex            string
		result, filter, changedFilter, update, remove, reinsert string
		unique, composite                                       bool
		entries                                                 int
	}{
		{"regular_pk", enum + " primary key,k int", "(0,0),(1,10),(2,20),(3,30)", "key ix(k)", "create index ix on %s(k)",
			"cast(e as unsigned)", "k=20", "k=21", "k=21 where e+0=2", "k=21", "(2,20)", false, false, 1},
		{"unique_pk", enum + " primary key,k int", "(0,0),(1,10),(2,20),(3,30)", "unique key ix(k)", "create unique index ix on %s(k)",
			"cast(e as unsigned)", "k=20", "k=21", "k=21 where e+0=2", "k=21", "(2,20)", true, false, 1},
		{"on_pk", enum + " primary key,k int", "(0,0),(1,10),(2,20),(3,30)", "key ix(e)", "create index ix on %s(e)",
			"cast(e as unsigned)", "e='A'", "e='A'", "k=21 where e+0=2", "e+0=2", "(2,20)", false, false, 1},
		{"on_enum", "id int primary key," + enum, "(0,0),(1,1),(2,2),(3,3),(4,NULL)", "key ix(e)", "create index ix on %s(e)",
			"cast(id as char)", "e='A'", "e='A'", "e=3 where id=2", "id=2", "(2,2)", false, false, 1},
		{"composite_enum", "id int primary key,k int," + enum, "(0,0,0),(1,10,1),(2,20,2),(3,30,3),(4,40,NULL)", "key ix(k,e)", "create index ix on %s(k,e)",
			"cast(e as unsigned)", "k=20", "k=21", "k=21 where id=2", "id=2", "(2,20,2)", false, false, 1},
		{"master_pk", enum + " primary key,v varchar(20),w varchar(20)", "(0,'zero',NULL),(1,'one','x'),(2,'two','y'),(3,'empty','z')", "index ix using master(v,w)", "create index ix using master on %s(v,w)",
			"cast(e as unsigned)", "v='two'", "v='changed'", "v='changed' where e+0=2", "v='changed'", "(2,'two','y')", false, false, 2},
		{"master_composite", enum + ",n int,v varchar(20),w varchar(20),primary key(e,n)", "(0,0,'zero',NULL),(1,1,'one','x'),(2,2,'two','y'),(3,3,'empty','z')", "index ix using master(v,w)", "create index ix using master on %s(v,w)",
			"cast(e as unsigned)", "v='two'", "v='changed'", "v='changed' where n=2", "v='changed'", "(2,2,'two','y')", false, true, 2},
		{"legacy_pk", "e enum('a','A','') collate __mo_enum_legacy_main_v1 primary key,k int", "(0,0),(1,10),(2,20),(3,30)", "key ix(k)", "create index ix on %s(k)",
			"cast(e as unsigned)", "k=20", "k=21", "k=21 where e+0=2", "k=21", "(2,20)", false, false, 1},
		{"mixed_set", enum + " primary key,k int,s set('x','y')", "(0,0,0),(1,10,1),(2,20,2),(3,30,3)", "key ix(k,s)", "create index ix on %s(k,s)",
			"cast(e as unsigned)", "k=20", "k=21", "k=21 where e+0=2", "k=21", "(2,20,2)", false, false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := func(statement string) { t.Helper(); execSQLRequire(t, ctx, db, statement) }
			inline, added := "storage_"+tc.name+"_inline", "storage_"+tc.name+"_added"
			exec("create table " + inline + "(" + tc.columns + "," + tc.inlineIndex + ")")
			exec("create table " + added + "(" + tc.columns + ")")
			exec("insert ignore into " + inline + " values " + tc.values)
			exec("insert ignore into " + added + " values " + tc.values)
			exec(fmt.Sprintf(tc.addIndex, added))
			hidden := func(table string) string {
				t.Helper()
				var name string
				require.NoError(t, db.QueryRowContext(ctx, "select distinct i.index_table_name from mo_catalog.mo_indexes i join mo_catalog.mo_tables t on i.table_id=t.rel_id where t.reldatabase=database() and t.relname=? and i.name='ix'", table).Scan(&name))
				return fmt.Sprintf("%c%s%c", 96, strings.ReplaceAll(name, string(rune(96)), strings.Repeat(string(rune(96)), 2)), 96)
			}
			keys := func(table string) []string {
				t.Helper()
				key := "hex(__mo_index_idx_col)"
				payload := "cast(__mo_index_pri_col as unsigned)"
				if tc.name == "on_pk" {
					key = "cast(__mo_index_idx_col as unsigned)"
				}
				if tc.composite {
					payload = "hex(__mo_index_pri_col)"
				}
				return queryEnumStorageRows(t, ctx, db, "select concat("+key+",':',"+payload+") from "+hidden(table)+" order by 1")
			}
			originalRows := 4
			if tc.name == "on_enum" || tc.name == "composite_enum" {
				originalRows = 5
			}
			compare := func(wantRows int) {
				t.Helper()
				left, right := keys(inline), keys(added)
				require.Len(t, right, wantRows*tc.entries)
				require.Equal(t, left, right, "populated backfill and maintained keys must match byte for byte")
				for _, table := range []string{inline, added} {
					var count int
					require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+table).Scan(&count))
					require.Equal(t, wantRows, count)
					sourcePK := "e"
					if tc.name == "on_enum" || tc.name == "composite_enum" {
						sourcePK = "id"
					}
					if tc.composite {
						sourcePK = "__mo_cpkey_col"
					}
					sourceValue, indexValue := "cast("+sourcePK+" as unsigned)", "cast(__mo_index_pri_col as unsigned)"
					if tc.composite {
						sourceValue, indexValue = "hex("+sourcePK+")", "hex(__mo_index_pri_col)"
					}
					require.Equal(t,
						queryEnumStorageRows(t, ctx, db, "select "+sourceValue+" from "+table+" order by 1"),
						queryEnumStorageRows(t, ctx, db, "select distinct "+indexValue+" from "+hidden(table)+" order by 1"),
						"origin payload must retain the exact stored primary key, including zero vs empty")
					for _, filter := range []string{tc.filter, tc.changedFilter} {
						query := "select " + tc.result + " from " + table + " %s where " + filter + " order by 1"
						require.Equal(t, queryEnumStorageRows(t, ctx, db, fmt.Sprintf(query, "ignore index(ix)")),
							queryEnumStorageRows(t, ctx, db, fmt.Sprintf(query, "force index(ix)")), filter)
					}
				}
			}
			compare(originalRows)
			// An index on the PK itself may be superseded by direct primary-key access.
			// Other cases must prove that the physical hidden index really executes.
			if tc.name != "on_pk" && tc.name != "on_enum" {
				plan := strings.Join(queryEnumStorageRows(t, ctx, db, "explain select "+tc.result+" from "+added+" force index(ix) where "+tc.filter), "\n")
				if strings.HasPrefix(tc.name, "master_") {
					require.Contains(t, plan, "__mo_index_secondary_", plan)
				} else {
					require.Contains(t, plan, "Index Table Scan", plan)
				}
			}
			for _, table := range []string{inline, added} {
				exec("update " + table + " set " + tc.update)
			}
			compare(originalRows)
			for _, table := range []string{inline, added} {
				exec("delete from " + table + " where " + tc.remove)
			}
			compare(originalRows - 1)
			for _, table := range []string{inline, added} {
				exec("insert into " + table + " values " + tc.reinsert)
			}
			compare(originalRows)
			if tc.unique {
				for _, table := range []string{inline, added} {
					_, err := db.ExecContext(ctx, "update "+table+" set k=20 where e+0=3")
					require.ErrorContains(t, err, "Duplicate entry")
				}
				compare(originalRows)
			}
			// Exercise the rebuilt writer after DML as well as the first population.
			exec("drop index ix on " + added)
			exec(fmt.Sprintf(tc.addIndex, added))
			compare(originalRows)
		})
	}
}

func testMySQLSpecialSerialContract(t *testing.T, ctx context.Context, db *sql.DB) {
	for _, tc := range []struct {
		name, declaration string
		bitmap            bool
	}{
		{"legacy", "enum('a','A','') collate __mo_enum_legacy_main_v1", false},
		{"binary", "enum('a','A','') collate utf8mb4_bin", false},
		{"general", "enum('a','A','') collate utf8mb4_general_ci", false},
		{"unicode", "enum('a','A','') collate utf8mb4_unicode_ci", false},
		{"set", "set('x','y')", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := "serial_contract_" + tc.name
			execSQLRequire(t, ctx, db, "create table "+table+"(id int primary key,e "+tc.declaration+")")
			execSQLRequire(t, ctx, db, "insert ignore into "+table+" values(0,0),(1,1),(2,2),(3,3),(4,NULL)")
			packer := types.NewPacker()
			defer packer.Close()
			packed := func(ordinal int, label string, text bool) string {
				packer.Reset()
				if ordinal < 0 {
					packer.EncodeNull()
				} else if text {
					packer.EncodeStringType([]byte(label))
				} else if tc.bitmap {
					packer.EncodeUint64(uint64(ordinal))
				} else {
					packer.EncodeEnum(types.Enum(ordinal))
				}
				return fmt.Sprintf("%X", packer.GetBuf())
			}
			labels := []string{"", "a", "A", ""}
			if tc.bitmap {
				labels = []string{"", "x", "y", "x,y"}
			}
			rawWant, textWant := make([]string, 0, 5), make([]string, 0, 5)
			lowerWant, viewWant := make([]string, 0, 5), make([]string, 0, 5)
			for n := 0; n < 5; n++ {
				ordinal := n
				label := ""
				if n == 4 {
					ordinal = -1
				} else {
					label = labels[n]
				}
				raw, display := packed(ordinal, label, false), packed(ordinal, label, true)
				rawWant = append(rawWant, raw)
				textWant = append(textWant, display)
				lowerWant = append(lowerWant, packed(ordinal, strings.ToLower(label), true))
				viewWant = append(viewWant, fmt.Sprintf("%d:%s", n, raw))
			}
			for _, name := range []string{"serial_full", "physical_serial_full", "serial", "physical_serial"} {
				want := append([]string(nil), rawWant...)
				if name == "serial" || name == "physical_serial" {
					want[4] = "NULL"
				}
				require.Equal(t, want, queryEnumStorageRows(t, ctx, db, "select coalesce(hex("+name+"((e))),'NULL') from "+table+" order by id"), name)
			}
			require.Equal(t, rawWant, queryEnumStorageRows(t, ctx, db,
				"select hex((select serial_full(outer_row.e))) from "+table+" outer_row order by id"),
				"correlated direct columns retain the same storage contract")
			require.Equal(t, textWant, queryEnumStorageRows(t, ctx, db, "select hex(serial_full(cast(e as char))) from "+table+" order by id"))
			require.Equal(t, lowerWant, queryEnumStorageRows(t, ctx, db, "select hex(serial_full(lower(e))) from "+table+" order by id"))
			view := "serial_view_" + tc.name
			execSQLRequire(t, ctx, db, "create view "+view+" as select id,hex(serial_full(e)) k from "+table)
			require.Equal(t, viewWant, queryEnumStorageRows(t, ctx, db, "select concat(id,':',k) from "+view+" order by id"),
				"SQL-text views bind the corrected storage contract")
		})
	}
}
