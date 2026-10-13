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
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/partitionservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// Seed through the existing typed engine API, as a catalog written by an older
// CN. Current CREATE validation stays enabled throughout the public scenario.
func TestLegacyPartitionColumnLimitCopy(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 180*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		const database = "legacy_partition_column_limit"
		execSQL := func(stmt string) {
			t.Helper()
			_, e := conn.ExecContext(ctx, stmt)
			require.NoError(t, e, stmt[:min(len(stmt), 120)])
		}
		execSQL("create database " + database)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+database)
		execSQL("use " + database)
		createSQL := func(name string, n int, partitioned bool) string {
			cols := make([]string, n)
			for i := range cols {
				cols[i] = fmt.Sprintf("c%d int", i)
			}
			text := "create table \x60" + name + "\x60(" + strings.Join(cols, ",") + ",primary key(c0))"
			if partitioned {
				text += " partition by hash(c0) partitions 2"
			}
			return text
		}
		execSQL(createSQL("seed", 4096, true))
		svc := cn.RawService().(cnservice.Service)
		legacyCtx := defines.AttachAccountId(ctx, 0)
		err = svc.GetSQLExecutor().ExecTxn(legacyCtx, func(txn executor.TxnExecutor) error {
			databaseObj, e := svc.GetEngine().Database(legacyCtx, database, txn.Txn())
			if e != nil {
				return e
			}
			seed, e := databaseObj.Relation(legacyCtx, "seed", nil)
			if e != nil {
				return e
			}
			seedID := seed.GetTableID(legacyCtx)
			copyLegacy := func(source engine.Relation, name string, parentID uint64, partitioned bool) error {
				def := proto.Clone(source.CopyTableDef(legacyCtx)).(*pb.TableDef)
				def.Name = name
				def.Createsql = createSQL(name, 4098, partitioned)
				// Preserve the original rowid while inserting two ordinary columns.
				last := def.Cols[len(def.Cols)-1]
				if last.Name != catalog.Row_ID {
					return fmt.Errorf("seed rowid is not last: %s", last.Name)
				}
				def.Cols = def.Cols[: len(def.Cols)-1 : len(def.Cols)-1]
				for i := 4096; i < 4098; i++ {
					def.Cols = append(def.Cols, &pb.ColDef{Name: fmt.Sprintf("c%d", i), OriginName: fmt.Sprintf("c%d", i), Typ: pb.Type{Id: int32(types.T_int32)}, Default: &pb.Default{NullAbility: true}})
				}
				def.Cols = append(def.Cols, last)
				for _, d := range def.Defs {
					if p := d.GetProperties(); p != nil {
						for _, v := range p.Properties {
							if v.Key == catalog.SystemRelAttr_CreateSQL {
								v.Value = def.Createsql
							}
						}
					}
				}
				defs, extra, e := engine.PlanDefsToExeDefs(def)
				if e != nil {
					return e
				}
				defs = append(engine.PlanColsToExeCols(def.Cols), defs...)
				extra.ParentTableID = parentID
				return databaseObj.Create(legacyCtx, name, defs)
			}
			if e = copyLegacy(seed, "wide", 0, true); e != nil {
				return e
			}
			wide, e := databaseObj.Relation(legacyCtx, "wide", nil)
			if e != nil {
				return e
			}
			wideID := wide.GetTableID(legacyCtx)
			txn.Use(catalog.MO_CATALOG)
			res, e := txn.Exec(fmt.Sprintf("insert into mo_partition_metadata select %d,'wide','%s',partition_method,partition_description,partition_count from mo_partition_metadata where table_id=%d", wideID, database, seedID), executor.StatementOption{})
			if e != nil {
				return e
			}
			res.Close()
			for _, p := range []string{"p0", "p1"} {
				oldName := partitionservice.GetPartitionTableName("seed", p)
				newName := partitionservice.GetPartitionTableName("wide", p)
				old, e := databaseObj.Relation(legacyCtx, oldName, nil)
				if e != nil {
					return e
				}
				if e = copyLegacy(old, newName, wideID, false); e != nil {
					return e
				}
				child, e := databaseObj.Relation(legacyCtx, newName, nil)
				if e != nil {
					return e
				}
				res, e = txn.Exec(fmt.Sprintf("insert into mo_partition_tables select %d,'%s',%d,partition_name,partition_ordinal_position,partition_expression_str,partition_expression from mo_partition_tables where primary_table_id=%d and partition_name='%s'", child.GetTableID(legacyCtx), newName, wideID, seedID, p), executor.StatementOption{})
				if e != nil {
					return e
				}
				res.Close()
			}
			return nil
		}, executor.Options{}.WithWaitCommittedLogApplied())
		require.NoError(t, err, "safe legacy catalog seeding")
		execSQL("insert into wide(c0,c4095,c4096,c4097) values(1,95,96,97),(2,195,196,197)")
		verify := func(width int) {
			t.Helper()
			var count int
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_columns where att_database=? and att_relname='wide' and att_is_hidden=0", database).Scan(&count))
			require.Equal(t, width, count)
			rows, e := conn.QueryContext(ctx, "select c0,c4095 from wide order by c0")
			require.NoError(t, e)
			defer rows.Close()
			var got [][2]int
			for rows.Next() {
				var v [2]int
				require.NoError(t, rows.Scan(&v[0], &v[1]))
				got = append(got, v)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, [][2]int{{1, 95}, {2, 195}}, got)
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_partition_tables p join mo_catalog.mo_tables t on p.primary_table_id=t.rel_id where t.reldatabase=? and t.relname='wide'", database).Scan(&count))
			require.Equal(t, 2, count)
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase=? and relname like '%_copy_%' and rel_id not in (select partition_id from mo_catalog.mo_partition_tables)", database).Scan(&count))
			require.Zero(t, count)
		}
		verify(4098)
		execSQL("begin")
		execSQL("alter table wide drop c4097")
		// In-transaction partition COPY visibility also fails with ordinary-width
		// tables on the merge-base control. Assert the rollback boundary here;
		// committed COPY data is checked below.
		execSQL("rollback")
		verify(4098)
		execSQL("alter table wide drop c4097")
		verify(4097)
		execSQL("alter table wide add replacement int, drop c4096")
		verify(4097)
		_, err = conn.ExecContext(ctx, "alter table wide add forbidden int")
		require.ErrorContains(t, err, "Too many columns")
		var packet *mysql.MySQLError
		require.ErrorAs(t, err, &packet)
		require.Equal(t, uint16(1117), packet.Number)
		require.Equal(t, "HY000", string(packet.SQLState[:]))
		verify(4097)
		execSQL("alter table wide drop replacement")
		verify(4096)
		_, err = conn.ExecContext(ctx, "alter table wide add forbidden int")
		require.ErrorContains(t, err, "Too many columns")
		verify(4096)
		execSQL("create table session_alive(id int)")
		execSQL("drop database " + database)
		var count int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase=?", database).Scan(&count))
		require.Zero(t, count)
	})
}
