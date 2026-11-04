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

package embed

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// Exercise admission and legacy cleanup through the public MySQL protocol.
func TestCatalogIdentifierEncoding(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()
		const database = "identifier_encoding"
		_, err = db.ExecContext(ctx, "create database "+database)
		require.NoError(t, err)
		defer func() {
			_, cleanupErr := db.ExecContext(ctx, "drop database if exists "+database)
			require.NoError(t, cleanupErr)
		}()
		for _, statement := range []string{
			"create table identifier_encoding.t_\xe3g(id int)",
			"create table identifier_encoding.`t_\xe3g`(id int)",
			"create table identifier_encoding.ctas as select 1 as `t_\xe3g`",
			"create view identifier_encoding.v as select 1 as `t_\xe3g`",
			"create sequence identifier_encoding.seq_\xe3g",
			"create sequence identifier_encoding.`seq_\xe3g`",
			"create sequence identifier_encoding.`" + strings.Repeat("s", 189) + "\xe3g`",
		} {
			_, err = db.ExecContext(ctx, statement)
			require.ErrorContains(t, err, "not valid UTF-8", "%q", statement)
		}
		var count int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase='identifier_encoding'").Scan(&count))
		require.Zero(t, count)
		// Binary parameters and string data are not identifier validation targets.
		var raw []byte
		require.NoError(t, db.QueryRowContext(ctx, "select ?", []byte("t_\xe3g")).Scan(&raw))
		require.Equal(t, []byte("t_\xe3g"), raw)
		for _, name := range []string{"表", "t_xe3g", "t_�g", "quote'\\tail"} {
			_, err = db.ExecContext(ctx, "create table "+database+"."+sqlquote.Ident(name)+"(id int)")
			require.NoError(t, err)
		}
		// Use the existing typed engine interface to emulate a catalog written by an
		// older CN, bypassing only the new authoring validation.
		svc := cn.RawService().(cnservice.Service)
		legacyCtx := defines.AttachAccountId(ctx, 0)
		seedLegacyTable := func() error {
			return svc.GetSQLExecutor().ExecTxn(legacyCtx, func(txn executor.TxnExecutor) error {
				database, err := svc.GetEngine().Database(legacyCtx, database, txn.Txn())
				if err != nil {
					return err
				}
				rel, err := database.Relation(legacyCtx, "表", nil)
				if err != nil {
					return err
				}
				defs, err := rel.TableDefs(legacyCtx)
				if err != nil {
					return err
				}
				return database.Create(legacyCtx, "t_\xe3g", defs)
			}, executor.Options{}.WithWaitCommittedLogApplied())
		}
		require.NoError(t, seedLegacyTable())
		var hexName string
		require.NoError(t, db.QueryRowContext(ctx, "select hex(relname) from mo_catalog.mo_tables where reldatabase='identifier_encoding' and hex(relname)='745FE367'").Scan(&hexName))
		require.Equal(t, "745FE367", hexName)
		// The legacy name must resolve to its own row, not t_xe3g or U+FFFD.
		_, err = db.ExecContext(ctx, "drop table identifier_encoding.`t_\xe3g`")
		require.NoError(t, err)
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase='identifier_encoding'").Scan(&count))
		require.Equal(t, 4, count)
		require.NoError(t, seedLegacyTable())
		_, err = db.ExecContext(ctx, "drop database identifier_encoding")
		require.NoError(t, err)
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase='identifier_encoding'").Scan(&count))
		require.Zero(t, count)
		legacyDatabase := "db_\xe3g"
		err = svc.GetSQLExecutor().ExecTxn(legacyCtx, func(txn executor.TxnExecutor) error {
			return svc.GetEngine().Create(legacyCtx, legacyDatabase, txn.Txn())
		}, executor.Options{}.WithWaitCommittedLogApplied())
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "drop database "+sqlquote.Ident(legacyDatabase))
		require.NoError(t, err)
	})
}

// Public DDL must preserve maintenance-task identity and isolate neighboring names.
func TestIndexUpdateIdentifierIsolation(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		exec := func(query string) {
			t.Helper()
			_, err := db.ExecContext(ctx, query)
			require.NoError(t, err, "%q", query)
		}
		// The first name used to decode to the second; the third used to contain a TAB.
		names := []string{`index_update\path`, "index_updatepath", `index_update\tail`}
		for _, name := range names {
			defer func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cleanupCancel()
				_, err := db.ExecContext(cleanupCtx, "drop database if exists "+sqlquote.Ident(name))
				require.NoError(t, err)
			}()
		}
		const indexName = "idx'\\tail"
		ids := make([]uint64, len(names))
		metadata := make([]map[string]any, len(names))
		assertTask := func(i int, tableName string) map[string]any {
			t.Helper()
			var actualDB, actualTable, actualIndex, rawMetadata string
			require.NoError(t, db.QueryRowContext(ctx, "select db_name, table_name, index_name, metadata from mo_catalog.mo_index_update where account_id=0 and table_id=?", ids[i]).Scan(&actualDB, &actualTable, &actualIndex, &rawMetadata))
			require.Equal(t, names[i], actualDB)
			require.Equal(t, tableName, actualTable)
			require.Equal(t, indexName, actualIndex)
			var decoded map[string]any
			require.NoError(t, json.Unmarshal([]byte(rawMetadata), &decoded))
			if metadata[i] != nil {
				require.Equal(t, metadata[i], decoded)
			}
			return decoded
		}
		assertCount := func(i, expected int) {
			t.Helper()
			var count int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_index_update where account_id=0 and table_id=?", ids[i]).Scan(&count))
			require.Equal(t, expected, count)
		}
		for i, name := range names {
			exec("create database " + sqlquote.Ident(name))
			table := sqlquote.QualifiedIdent(name, "t")
			exec("create table " + table + "(id int primary key,v vecf32(3),w vecf32(3))")
			exec("insert into " + table + " values(1,'[1,0,0]','[1,0,0]'),(2,'[0,1,0]','[0,1,0]'),(3,'[0,0,1]','[0,0,1]')")
			exec("create index " + sqlquote.Ident(indexName) + " using ivfflat on " + table + "(v) lists=1 op_type 'vector_l2_ops'")
			exec("alter table " + table + " alter index " + sqlquote.Ident(indexName) + " ivfflat auto_update=true day=25 hour=23")
			require.NoError(t, db.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='t'", name).Scan(&ids[i]))
			metadata[i] = assertTask(i, "t")
			assertCount(i, 1)
		}
		// A rejected rename leaves both the source data and maintenance identity intact.
		exec("create table " + sqlquote.QualifiedIdent(names[0], "occupied") + "(id int)")
		_, err = db.ExecContext(ctx, "rename table "+sqlquote.QualifiedIdent(names[0], "t")+" to "+sqlquote.QualifiedIdent(names[0], "occupied"))
		require.Error(t, err)
		assertTask(0, "t")
		exec("drop table " + sqlquote.QualifiedIdent(names[0], "occupied"))
		oldName := "t"
		for _, newName := range []string{"quote'\\tail", "renamed'\\path"} {
			exec("rename table " + sqlquote.QualifiedIdent(names[0], oldName) + " to " + sqlquote.QualifiedIdent(names[0], newName))
			assertTask(0, newName)
			var actualID uint64
			require.NoError(t, db.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname=?", names[0], newName).Scan(&actualID))
			require.Equal(t, ids[0], actualID)
			var rows int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+sqlquote.QualifiedIdent(names[0], newName)).Scan(&rows))
			require.Equal(t, 3, rows)
			assertTask(1, "t")
			oldName = newName
		}
		table := sqlquote.QualifiedIdent(names[0], oldName)
		exec("alter table " + table + " alter index " + sqlquote.Ident(indexName) + " ivfflat auto_update=false")
		// Disabling maintenance retains the task; the executor reads the latest IndexDef gate.
		assertCount(0, 1)
		var rawParams string
		require.NoError(t, db.QueryRowContext(ctx, "select algo_params from mo_catalog.mo_indexes where table_id=? and name=?", ids[0], indexName).Scan(&rawParams))
		var params map[string]any
		require.NoError(t, json.Unmarshal([]byte(rawParams), &params))
		require.Equal(t, "false", params["auto_update"])
		assertCount(1, 1)
		exec("alter table " + table + " alter index " + sqlquote.Ident(indexName) + " ivfflat auto_update=true day=25 hour=23")
		assertTask(0, oldName)
		exec("drop database " + sqlquote.Ident(names[0]))
		assertCount(0, 0)
		for _, i := range []int{1, 2} {
			assertCount(i, 1)
			assertTask(i, "t")
			var rows int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+sqlquote.QualifiedIdent(names[i], "t")).Scan(&rows))
			require.Equal(t, 3, rows)
		}
		// A competing index on each source distinguishes exact-name deletion from table-wide cleanup.
		for _, i := range []int{1, 2} {
			table := sqlquote.QualifiedIdent(names[i], "t")
			exec("create index keeper using ivfflat on " + table + "(w) lists=1 op_type 'vector_l2_ops'")
			exec("alter table " + table + " alter index keeper ivfflat auto_update=true day=25 hour=23")
			assertCount(i, 2)
		}
		exec("drop index " + sqlquote.Ident(indexName) + " on " + sqlquote.QualifiedIdent(names[1], "t"))
		assertCount(1, 1)
		assertCount(2, 2)
		exec("use " + sqlquote.Ident(names[2]))
		exec("alter table " + sqlquote.QualifiedIdent(names[2], "t") + " drop index " + sqlquote.Ident(indexName))
		for _, i := range []int{1, 2} {
			assertCount(i, 1)
			var count int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_index_update where account_id=0 and table_id=? and index_name='keeper'", ids[i]).Scan(&count))
			require.Equal(t, 1, count)
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_indexes where table_id=? and name=?", ids[i], indexName).Scan(&count))
			require.Zero(t, count)
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_indexes where table_id=? and name='keeper'", ids[i]).Scan(&count))
			require.Equal(t, 3, count)
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+sqlquote.QualifiedIdent(names[i], "t")).Scan(&count))
			require.Equal(t, 3, count)
		}
		exec("drop table " + sqlquote.QualifiedIdent(names[2], "t"))
		assertCount(2, 0)
		for _, name := range names {
			exec("drop database if exists " + sqlquote.Ident(name))
			var count int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase=?", name).Scan(&count))
			require.Zero(t, count)
		}
		for i := range ids {
			assertCount(i, 0)
		}
	})
}
