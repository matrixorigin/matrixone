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
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/stretchr/testify/require"
)

func TestCDCTargetGenerationVerificationOnMO(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(cluster Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		_, err = conn.ExecContext(ctx, "DROP DATABASE IF EXISTS cdc_generation_verify")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "CREATE DATABASE cdc_generation_verify")
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			if _, cleanupErr := conn.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS cdc_generation_verify"); cleanupErr != nil {
				t.Errorf("drop CDC target verification database: %v", cleanupErr)
			}
		}()
		_, err = conn.ExecContext(ctx, "CREATE TABLE cdc_generation_verify.good (id INT PRIMARY KEY, v VARCHAR(20) COLLATE utf8mb4_bin, UNIQUE KEY uk_v(v))")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "CREATE TABLE cdc_generation_verify.bad (id INT PRIMARY KEY, v VARCHAR(20) COLLATE utf8mb4_general_ci, UNIQUE KEY uk_v(v))")
		require.NoError(t, err)

		source := &plan.TableDef{
			Cols: []*plan.ColDef{
				{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}},
				{Name: "v", Typ: plan.Type{Id: int32(types.T_varchar), Width: 20, Charset: uint32(types.CharsetUTF8MB4Bin)}},
			},
			Pkey:    &plan.PrimaryKeyDef{Names: []string{"id"}},
			Indexes: []*plan.IndexDef{{IndexName: "uk_v", Parts: []string{"v"}, Unique: true}},
		}
		sink := cdc.UriInfo{SinkTyp: cdc.CDCSinkType_MO, User: "dump", Password: "111", Ip: "127.0.0.1", Port: int(port)}
		fence := cdc.NewOwnerFenceForGeneration(time.UnixMicro(1), func(context.Context) error { return nil })
		good := &cdc.DbTableInfo{SinkDbName: "cdc_generation_verify", SinkTblName: "good", SourceTblId: 42}
		require.NoError(t, cdc.VerifyOwnedTarget(ctx, sink, 0, "verify", good, source, fence, cdc.CDCDefaultSendSqlTimeout))
		bad := &cdc.DbTableInfo{SinkDbName: "cdc_generation_verify", SinkTblName: "bad", SourceTblId: 42}
		require.ErrorContains(t, cdc.VerifyOwnedTarget(ctx, sink, 0, "verify", bad, source, fence, cdc.CDCDefaultSendSqlTimeout), "collation differs")
		for _, tc := range []struct {
			name, ddl string
			typ       plan.Type
			wantError bool
		}{
			{"year_type", "YEAR", plan.Type{Id: int32(types.T_year), Width: 4}, false},
			{"float_width_only", "FLOAT(5)", plan.Type{Id: int32(types.T_float32), Width: 5, Scale: -1}, false},
			{"double_width_only", "DOUBLE(6)", plan.Type{Id: int32(types.T_float64), Width: 6, Scale: -1}, false},
			{"float_scale", "FLOAT(5,2)", plan.Type{Id: int32(types.T_float32), Width: 5, Scale: 2}, false},
			{"double_scale", "DOUBLE(6,5)", plan.Type{Id: int32(types.T_float64), Width: 6, Scale: 5}, false},
			{"wrong_float_scale", "FLOAT(5,3)", plan.Type{Id: int32(types.T_float32), Width: 5, Scale: 2}, true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, createErr := conn.ExecContext(ctx, fmt.Sprintf(
					"CREATE TABLE cdc_generation_verify.%s (id INT PRIMARY KEY, v %s)", tc.name, tc.ddl))
				require.NoError(t, createErr)
				typedSource := &plan.TableDef{Cols: []*plan.ColDef{
					{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}},
					{Name: "v", Typ: tc.typ},
				}, Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}}}
				info := &cdc.DbTableInfo{SinkDbName: "cdc_generation_verify", SinkTblName: tc.name, SourceTblId: 42}
				verifyErr := cdc.VerifyOwnedTarget(ctx, sink, 0, "verify", info,
					typedSource, fence, cdc.CDCDefaultSendSqlTimeout)
				if tc.wantError {
					require.ErrorContains(t, verifyErr, "column 2 differs")
				} else {
					require.NoError(t, verifyErr)
				}
			})
		}
	})
}

func TestCDCTenantCatalogGuardBlocksOtherCNDrop(t *testing.T) {
	RunBaseClusterTests(t, func(cluster Cluster) {
		ports := make([]int, 2)
		for i := range ports {
			cn, err := cluster.GetCNService(i)
			require.NoError(t, err)
			ports[i] = int(cn.GetServiceConfig().CN.Frontend.Port)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		root, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", ports[0]))
		require.NoError(t, err)
		defer root.Close()
		require.NoError(t, execSQL(ctx, root, "CREATE ACCOUNT cdc_guard_probe ADMIN_NAME 'admin' IDENTIFIED BY '111'"))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = root.ExecContext(cleanupCtx, "DROP ACCOUNT IF EXISTS cdc_guard_probe")
		}()
		connect := func(port int) *sql.DB {
			db, openErr := sql.Open("mysql", fmt.Sprintf("cdc_guard_probe#admin:111@tcp(127.0.0.1:%d)/", port))
			require.NoError(t, openErr)
			return db
		}
		first, second := connect(ports[0]), connect(ports[1])
		defer first.Close()
		defer second.Close()
		require.NoError(t, execSQL(ctx, first, "CREATE DATABASE cdc_guard_db"))
		require.NoError(t, execSQL(ctx, first, "CREATE TABLE cdc_guard_db.t (id INT PRIMARY KEY)"))
		require.NoError(t, execSQL(ctx, first, "CREATE DATABASE cdc_guard_other"))
		require.NoError(t, execSQL(ctx, first, "CREATE ROLE cdc_guard_writer"))
		require.NoError(t, execSQL(ctx, first, "CREATE USER cdc_guard_writer_user IDENTIFIED BY '111' DEFAULT ROLE cdc_guard_writer"))
		require.NoError(t, execSQL(ctx, first, "GRANT CONNECT ON ACCOUNT * TO cdc_guard_writer"))
		require.NoError(t, execSQL(ctx, first, "GRANT INSERT ON TABLE cdc_guard_db.t TO cdc_guard_writer"))
		require.NoError(t, execSQL(ctx, first, "GRANT cdc_guard_writer TO cdc_guard_writer_user"))
		writer, err := sql.Open("mysql", fmt.Sprintf("cdc_guard_probe#cdc_guard_writer_user#cdc_guard_writer:111@tcp(127.0.0.1:%d)/", ports[0]))
		require.NoError(t, err)
		defer writer.Close()
		writerTx, err := writer.BeginTx(ctx, nil)
		require.NoError(t, err)
		_, err = writerTx.ExecContext(ctx, "CALL mo_cdc_target_guard_capability()")
		require.NoError(t, err)
		var writerID uint64
		require.NoError(t, writerTx.QueryRowContext(ctx, "CALL mo_cdc_target_identity('cdc_guard_db', 't')").Scan(&writerID))
		require.NotZero(t, writerID)
		require.NoError(t, writerTx.Rollback())
		require.NoError(t, execSQL(ctx, first, "CREATE ROLE cdc_guard_other_role"))
		require.NoError(t, execSQL(ctx, first, "CREATE USER cdc_guard_other_user IDENTIFIED BY '111' DEFAULT ROLE cdc_guard_other_role"))
		require.NoError(t, execSQL(ctx, first, "GRANT CONNECT ON ACCOUNT * TO cdc_guard_other_role"))
		require.NoError(t, execSQL(ctx, first, "GRANT CREATE VIEW ON DATABASE cdc_guard_other TO cdc_guard_other_role"))
		require.NoError(t, execSQL(ctx, first, "GRANT cdc_guard_other_role TO cdc_guard_other_user"))
		other, err := sql.Open("mysql", fmt.Sprintf("cdc_guard_probe#cdc_guard_other_user#cdc_guard_other_role:111@tcp(127.0.0.1:%d)/", ports[0]))
		require.NoError(t, err)
		defer other.Close()
		otherTx, err := other.BeginTx(ctx, nil)
		require.NoError(t, err)
		var otherID uint64
		err = otherTx.QueryRowContext(ctx, "CALL mo_cdc_target_identity('cdc_guard_db', 't')").Scan(&otherID)
		require.ErrorContains(t, err, "do not have privilege")
		require.NoError(t, otherTx.Rollback())
		var autocommitID uint64
		err = first.QueryRowContext(ctx, "CALL mo_cdc_target_identity('cdc_guard_db', 't')").Scan(&autocommitID)
		require.ErrorContains(t, err, "requires an explicit transaction")
		_, err = first.ExecContext(ctx, "CALL mo_cdc_target_guard_capability()")
		require.ErrorContains(t, err, "requires an explicit transaction")
		tx, err := first.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		var tableID uint64
		err = tx.QueryRowContext(ctx, "CALL mo_cdc_target_identity('cdc_guard_db', 't')").Scan(&tableID)
		require.NoError(t, err)
		require.NotZero(t, tableID)
		ddlConn, err := second.Conn(ctx)
		require.NoError(t, err)
		defer ddlConn.Close()
		_, err = ddlConn.ExecContext(ctx, "SET lock_wait_timeout = 1")
		require.NoError(t, err)
		dropDone := make(chan error, 1)
		go func() { _, dropErr := ddlConn.ExecContext(ctx, "DROP TABLE cdc_guard_db.t"); dropDone <- dropErr }()
		select {
		case dropErr := <-dropDone:
			var sqlErr *mysql.MySQLError
			require.ErrorAs(t, dropErr, &sqlErr)
			require.Equal(t, uint16(1205), sqlErr.Number)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		require.NoError(t, tx.Rollback())
		_, err = ddlConn.ExecContext(ctx, "DROP TABLE cdc_guard_db.t")
		require.NoError(t, err)
	})
}

func execSQL(ctx context.Context, db *sql.DB, query string) error {
	_, err := db.ExecContext(ctx, query)
	return err
}

func TestCDCEndTsWildcardLateTableOnMO(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(cluster Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		root, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer root.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		_, err = root.ExecContext(ctx, "CREATE ACCOUNT cdc_end_probe ADMIN_NAME 'admin' IDENTIFIED BY '111'")
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			if _, cleanupErr := root.ExecContext(cleanupCtx, "DROP ACCOUNT IF EXISTS cdc_end_probe"); cleanupErr != nil {
				t.Errorf("drop CDC EndTs account: %v", cleanupErr)
			}
		}()
		account, err := sql.Open("mysql", fmt.Sprintf("cdc_end_probe#admin:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer account.Close()
		_, err = account.ExecContext(ctx, "CREATE DATABASE cdc_end_src")
		require.NoError(t, err)
		_, err = account.ExecContext(ctx, "CREATE TABLE cdc_end_src.early (id INT PRIMARY KEY)")
		require.NoError(t, err)
		_, err = account.ExecContext(ctx, "INSERT INTO cdc_end_src.early VALUES (1)")
		require.NoError(t, err)
		_, err = account.ExecContext(ctx, "CREATE PITR cdc_end_pitr FOR DATABASE cdc_end_src RANGE 2 'h'")
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			if _, cleanupErr := account.ExecContext(cleanupCtx, "DROP PITR cdc_end_pitr"); cleanupErr != nil {
				t.Errorf("drop CDC EndTs PITR: %v", cleanupErr)
			}
		}()

		entered, release := make(chan struct{}), make(chan struct{})
		var once, released sync.Once
		releaseAdmission := func() { released.Do(func() { close(release) }) }
		restore := frontend.SetCDCTestAdmissionHookForTest(func() {
			once.Do(func() {
				close(entered)
				<-release
			})
		})
		defer restore()
		defer releaseAdmission()
		endTime := time.Now().UTC().Add(10 * time.Second).Truncate(time.Second)
		uri := fmt.Sprintf("mysql://cdc_end_probe#admin:111@127.0.0.1:%d", port)
		_, err = account.ExecContext(ctx, fmt.Sprintf(
			"CREATE CDC cdc_end_task '%s' 'matrixone' '%s' 'cdc_end_src:cdc_end_dst' {'Level'='database','EndTs'='%s'}",
			uri, uri, endTime.Format("2006-01-02T15:04:05Z")))
		require.NoError(t, err)
		defer func() {
			releaseAdmission()
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			if _, cleanupErr := account.ExecContext(cleanupCtx, "DROP CDC TASK cdc_end_task"); cleanupErr != nil {
				t.Errorf("drop CDC EndTs task: %v", cleanupErr)
			}
		}()
		select {
		case <-entered:
		case <-time.After(30 * time.Second):
			t.Fatal("CDC initial table admission did not start")
		}
		if wait := time.Until(endTime.Add(250 * time.Millisecond)); wait > 0 {
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
		_, err = account.ExecContext(ctx, "CREATE TABLE cdc_end_src.late (id INT PRIMARY KEY)")
		require.NoError(t, err)
		releaseAdmission()
		require.Eventually(t, func() bool {
			var id int
			return account.QueryRowContext(ctx, "SELECT id FROM cdc_end_dst.early").Scan(&id) == nil && id == 1
		}, 90*time.Second, 250*time.Millisecond)
		require.Eventually(t, func() bool {
			var diagnostic string
			err := root.QueryRowContext(ctx,
				"SELECT w.err_msg FROM mo_catalog.mo_cdc_watermark AS w "+
					"JOIN mo_catalog.mo_cdc_task AS t ON t.account_id = w.account_id AND t.task_id = w.task_id "+
					"WHERE t.task_name = 'cdc_end_task' AND w.db_name = 'cdc_end_src' AND w.table_name = 'late'").Scan(&diagnostic)
			return err == nil && strings.Contains(diagnostic, "was absent at EndTs")
		}, 90*time.Second, 250*time.Millisecond)
		var targetCount, epochCount int
		require.NoError(t, account.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM mo_catalog.mo_tables WHERE reldatabase = 'cdc_end_dst' AND relname = 'late'").Scan(&targetCount))
		require.Zero(t, targetCount)
		require.NoError(t, root.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM mo_catalog.mo_cdc_snapshot AS s "+
				"JOIN mo_catalog.mo_cdc_task AS t ON t.account_id = s.account_id AND t.task_id = s.task_id "+
				"WHERE t.task_name = 'cdc_end_task' AND s.db_name = 'cdc_end_src' AND s.table_name = 'late'").Scan(&epochCount))
		require.Zero(t, epochCount)
	})
}

func TestCDCGenerationReplacementOnMO(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(cluster Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		root, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer root.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
		defer cancel()
		_, err = root.ExecContext(ctx, "DROP ACCOUNT IF EXISTS cdc_generation_probe")
		require.NoError(t, err)
		_, err = root.ExecContext(ctx, "CREATE ACCOUNT IF NOT EXISTS cdc_generation_probe ADMIN_NAME 'admin' IDENTIFIED BY '111'")
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			if _, cleanupErr := root.ExecContext(cleanupCtx, "DROP ACCOUNT IF EXISTS cdc_generation_probe"); cleanupErr != nil {
				t.Errorf("drop CDC test account: %v", cleanupErr)
			}
		}()
		account, err := sql.Open("mysql", fmt.Sprintf("cdc_generation_probe#admin:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer account.Close()
		_, err = account.ExecContext(ctx, "CREATE DATABASE cdc_generation_src")
		require.NoError(t, err)
		createdPitr, createdTask := false, false
		defer func() {
			if createdTask {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
				if _, cleanupErr := account.ExecContext(cleanupCtx, "DROP CDC TASK cdc_generation_task"); cleanupErr != nil {
					t.Errorf("drop CDC test task: %v", cleanupErr)
				}
				cleanupCancel()
			}
			if createdPitr {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
				if _, cleanupErr := account.ExecContext(cleanupCtx, "DROP PITR cdc_generation_pitr"); cleanupErr != nil {
					t.Errorf("drop CDC test PITR: %v", cleanupErr)
				}
				cleanupCancel()
			}
		}()
		_, err = account.ExecContext(ctx, "CREATE TABLE cdc_generation_src.t (id INT PRIMARY KEY, v INT)")
		require.NoError(t, err)
		_, err = account.ExecContext(ctx, "INSERT INTO cdc_generation_src.t VALUES (1,10),(2,20)")
		require.NoError(t, err)
		_, err = account.ExecContext(ctx, "CREATE PITR cdc_generation_pitr FOR DATABASE cdc_generation_src RANGE 2 'h'")
		require.NoError(t, err)
		createdPitr = true
		uri := fmt.Sprintf("mysql://cdc_generation_probe#admin:111@127.0.0.1:%d", port)
		_, err = account.ExecContext(ctx, fmt.Sprintf("CREATE CDC cdc_generation_task '%s' 'matrixone' '%s' 'cdc_generation_src:cdc_generation_dst' {'Level'='database'}", uri, uri))
		require.NoError(t, err)
		createdTask = true
		readRows := func(target string) []int {
			rows, readErr := account.QueryContext(ctx, "SELECT id FROM "+target+".t ORDER BY id")
			if readErr != nil {
				return nil
			}
			defer rows.Close()
			var ids []int
			for rows.Next() {
				var id int
				if rows.Scan(&id) != nil {
					return nil
				}
				ids = append(ids, id)
			}
			if rows.Err() != nil {
				return nil
			}
			return ids
		}
		readProgress := func() (uint64, string, error) {
			var generation uint64
			var watermark string
			err := root.QueryRowContext(ctx,
				"SELECT w.source_table_id, w.watermark FROM mo_catalog.mo_cdc_watermark AS w "+
					"JOIN mo_catalog.mo_cdc_task AS t ON t.account_id = w.account_id AND t.task_id = w.task_id "+
					"WHERE t.task_name = 'cdc_generation_task' AND w.db_name = 'cdc_generation_src' "+
					"AND w.table_name = 't'").Scan(&generation, &watermark)
			return generation, watermark, err
		}
		require.Eventually(t, func() bool { return reflect.DeepEqual(readRows("cdc_generation_dst"), []int{1, 2}) }, 90*time.Second, 250*time.Millisecond)
		var oldGeneration uint64
		require.Eventually(t, func() bool {
			generation, watermark, readErr := readProgress()
			oldGeneration = generation
			return readErr == nil && generation > 0 && watermark != "" && watermark != "0-0"
		}, 30*time.Second, 250*time.Millisecond)
		_, err = account.ExecContext(ctx, "PAUSE CDC TASK cdc_generation_task")
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			var state string
			return root.QueryRowContext(ctx,
				"SELECT state FROM mo_catalog.mo_cdc_task WHERE task_name = 'cdc_generation_task'").Scan(&state) == nil &&
				state == cdc.CDCState_Paused
		}, 30*time.Second, 250*time.Millisecond)
		var targetIdentity string
		var pending sql.NullInt64
		require.NoError(t, root.QueryRowContext(ctx,
			"SELECT w.target_identity, w.pending_source_table_id FROM mo_catalog.mo_cdc_watermark AS w "+
				"JOIN mo_catalog.mo_cdc_task AS t ON t.account_id = w.account_id AND t.task_id = w.task_id "+
				"WHERE t.task_name = 'cdc_generation_task' AND w.db_name = 'cdc_generation_src' AND w.table_name = 't'").Scan(&targetIdentity, &pending))
		require.True(t, strings.HasPrefix(targetIdentity, "mo:"))
		require.False(t, pending.Valid)
		var oldEpochs int
		require.NoError(t, root.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM mo_catalog.mo_cdc_snapshot AS s "+
				"JOIN mo_catalog.mo_cdc_task AS t ON t.account_id = s.account_id AND t.task_id = s.task_id "+
				"WHERE t.task_name = 'cdc_generation_task' AND s.db_name = 'cdc_generation_src' AND s.table_name = 't'").Scan(&oldEpochs))
		_, err = account.ExecContext(ctx, "DROP TABLE cdc_generation_src.t")
		require.NoError(t, err)
		_, err = account.ExecContext(ctx, "CREATE TABLE cdc_generation_src.t (id INT PRIMARY KEY, v INT)")
		require.NoError(t, err)
		_, err = account.ExecContext(ctx, "INSERT INTO cdc_generation_src.t VALUES (3,30),(4,40)")
		require.NoError(t, err)
		_, err = account.ExecContext(ctx, "RESUME CDC TASK cdc_generation_task")
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			var state string
			return root.QueryRowContext(ctx,
				"SELECT state FROM mo_catalog.mo_cdc_task WHERE task_name = 'cdc_generation_task'").Scan(&state) == nil &&
				state == cdc.CDCState_Failed
		}, 90*time.Second, 250*time.Millisecond)
		var taskError string
		require.NoError(t, root.QueryRowContext(ctx,
			"SELECT err_msg FROM mo_catalog.mo_cdc_task WHERE task_name = 'cdc_generation_task'").Scan(&taskError))
		require.Contains(t, taskError, "permanent table error")
		var tableError string
		require.NoError(t, root.QueryRowContext(ctx,
			"SELECT w.err_msg FROM mo_catalog.mo_cdc_watermark AS w "+
				"JOIN mo_catalog.mo_cdc_task AS t ON t.account_id = w.account_id AND t.task_id = w.task_id "+
				"WHERE t.task_name = 'cdc_generation_task' AND w.db_name = 'cdc_generation_src' AND w.table_name = 't'").Scan(&tableError))
		require.Contains(t, tableError, "explicit target rebuild")
		require.Equal(t, []int{1, 2}, readRows("cdc_generation_dst"))
		generation, _, readErr := readProgress()
		require.NoError(t, readErr)
		require.Equal(t, oldGeneration, generation)
		var currentIdentity string
		var currentPending sql.NullInt64
		require.NoError(t, root.QueryRowContext(ctx,
			"SELECT w.target_identity, w.pending_source_table_id FROM mo_catalog.mo_cdc_watermark AS w "+
				"JOIN mo_catalog.mo_cdc_task AS t ON t.account_id = w.account_id AND t.task_id = w.task_id "+
				"WHERE t.task_name = 'cdc_generation_task' AND w.db_name = 'cdc_generation_src' AND w.table_name = 't'").Scan(&currentIdentity, &currentPending))
		require.Equal(t, targetIdentity, currentIdentity)
		require.False(t, currentPending.Valid)
		var newEpochs int
		require.NoError(t, root.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM mo_catalog.mo_cdc_snapshot AS s "+
				"JOIN mo_catalog.mo_cdc_task AS t ON t.account_id = s.account_id AND t.task_id = s.task_id "+
				"WHERE t.task_name = 'cdc_generation_task' AND s.db_name = 'cdc_generation_src' AND s.table_name = 't'").Scan(&newEpochs))
		require.Equal(t, oldEpochs, newEpochs)

		// A replacement with the same schema must not inherit acknowledged progress.
		_, err = account.ExecContext(ctx, fmt.Sprintf("CREATE CDC cdc_generation_target_task '%s' 'matrixone' '%s' 'cdc_generation_src:cdc_generation_dst2' {'Level'='database'}", uri, uri))
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			if _, cleanupErr := account.ExecContext(cleanupCtx, "DROP CDC TASK cdc_generation_target_task"); cleanupErr != nil {
				t.Errorf("drop target replacement CDC task: %v", cleanupErr)
			}
		}()
		require.Eventually(t, func() bool { return reflect.DeepEqual(readRows("cdc_generation_dst2"), []int{3, 4}) }, 90*time.Second, 250*time.Millisecond)
		_, err = account.ExecContext(ctx, "PAUSE CDC TASK cdc_generation_target_task")
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			var state string
			return root.QueryRowContext(ctx,
				"SELECT state FROM mo_catalog.mo_cdc_task WHERE task_name = 'cdc_generation_target_task'").Scan(&state) == nil &&
				state == cdc.CDCState_Paused
		}, 30*time.Second, 250*time.Millisecond)
		var replacementIdentity string
		require.NoError(t, root.QueryRowContext(ctx,
			"SELECT w.target_identity FROM mo_catalog.mo_cdc_watermark AS w "+
				"JOIN mo_catalog.mo_cdc_task AS t ON t.account_id = w.account_id AND t.task_id = w.task_id "+
				"WHERE t.task_name = 'cdc_generation_target_task' AND w.db_name = 'cdc_generation_src' AND w.table_name = 't'").Scan(&replacementIdentity))
		require.True(t, strings.HasPrefix(replacementIdentity, "mo:"))
		_, err = account.ExecContext(ctx, "DROP TABLE cdc_generation_dst2.t")
		require.NoError(t, err)
		_, err = account.ExecContext(ctx, "CREATE TABLE cdc_generation_dst2.t (id INT PRIMARY KEY, v INT)")
		require.NoError(t, err)
		_, err = account.ExecContext(ctx, "INSERT INTO cdc_generation_dst2.t VALUES (3,30)")
		require.NoError(t, err)
		_, err = account.ExecContext(ctx, "INSERT INTO cdc_generation_src.t VALUES (5,50)")
		require.NoError(t, err)
		_, err = account.ExecContext(ctx, "RESUME CDC TASK cdc_generation_target_task")
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			var state string
			return root.QueryRowContext(ctx,
				"SELECT state FROM mo_catalog.mo_cdc_task WHERE task_name = 'cdc_generation_target_task'").Scan(&state) == nil &&
				state == cdc.CDCState_Failed
		}, 90*time.Second, 250*time.Millisecond)
		require.Equal(t, []int{3}, readRows("cdc_generation_dst2"))
		var replacementError, persistedIdentity string
		require.NoError(t, root.QueryRowContext(ctx,
			"SELECT w.err_msg, w.target_identity FROM mo_catalog.mo_cdc_watermark AS w "+
				"JOIN mo_catalog.mo_cdc_task AS t ON t.account_id = w.account_id AND t.task_id = w.task_id "+
				"WHERE t.task_name = 'cdc_generation_target_task' AND w.db_name = 'cdc_generation_src' AND w.table_name = 't'").Scan(&replacementError, &persistedIdentity))
		require.Contains(t, replacementError, "was replaced; explicit rebuild")
		require.Equal(t, replacementIdentity, persistedIdentity)
	})
}

func TestCDCFirstAckHoldsSourceGenerationAcrossCN(t *testing.T) {
	RunBaseClusterTests(t, func(cluster Cluster) {
		ports := make([]int, 2)
		for i := range ports {
			cn, err := cluster.GetCNService(i)
			require.NoError(t, err)
			ports[i] = int(cn.GetServiceConfig().CN.Frontend.Port)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		root, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", ports[0]))
		require.NoError(t, err)
		defer root.Close()
		require.NoError(t, execSQL(ctx, root, "CREATE ACCOUNT cdc_ack_guard ADMIN_NAME 'admin' IDENTIFIED BY '111'"))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = root.ExecContext(cleanupCtx, "DROP ACCOUNT IF EXISTS cdc_ack_guard")
		}()
		connect := func(port int) *sql.DB {
			db, openErr := sql.Open("mysql", fmt.Sprintf("cdc_ack_guard#admin:111@tcp(127.0.0.1:%d)/", port))
			require.NoError(t, openErr)
			return db
		}
		first, second := connect(ports[0]), connect(ports[1])
		defer first.Close()
		defer second.Close()
		require.NoError(t, execSQL(ctx, first, "CREATE DATABASE cdc_ack_src"))
		require.NoError(t, execSQL(ctx, first, "CREATE TABLE cdc_ack_src.t (id INT PRIMARY KEY)"))
		require.NoError(t, execSQL(ctx, first, "INSERT INTO cdc_ack_src.t VALUES (1)"))
		require.NoError(t, execSQL(ctx, first, "CREATE PITR cdc_ack_pitr FOR DATABASE cdc_ack_src RANGE 2 'h'"))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = first.ExecContext(cleanupCtx, "DROP CDC TASK cdc_ack_task")
			_, _ = first.ExecContext(cleanupCtx, "DROP PITR cdc_ack_pitr")
		}()
		entered, release := make(chan struct{}), make(chan struct{})
		var once, released sync.Once
		releaseAck := func() { released.Do(func() { close(release) }) }
		restore := frontend.SetCDCSourceGuardHookForTest(func() {
			once.Do(func() {
				close(entered)
				<-release
			})
		})
		defer restore()
		defer releaseAck()
		uri := fmt.Sprintf("mysql://cdc_ack_guard#admin:111@127.0.0.1:%d", ports[0])
		_, err = first.ExecContext(ctx, fmt.Sprintf(
			"CREATE CDC cdc_ack_task '%s' 'matrixone' '%s' 'cdc_ack_src:cdc_ack_dst' {'Level'='database'}", uri, uri))
		require.NoError(t, err)
		select {
		case <-entered:
		case <-time.After(60 * time.Second):
			t.Fatal("first target ACK did not reach the source guard")
		}
		ddlConn, err := second.Conn(ctx)
		require.NoError(t, err)
		defer ddlConn.Close()
		_, err = ddlConn.ExecContext(ctx, "SET lock_wait_timeout = 1")
		require.NoError(t, err)
		_, err = ddlConn.ExecContext(ctx, "DROP TABLE cdc_ack_src.t")
		var sqlErr *mysql.MySQLError
		require.ErrorAs(t, err, &sqlErr)
		require.Equal(t, uint16(1205), sqlErr.Number)
		releaseAck()
		require.Eventually(t, func() bool {
			var id int
			return first.QueryRowContext(ctx, "SELECT id FROM cdc_ack_dst.t").Scan(&id) == nil && id == 1
		}, 90*time.Second, 250*time.Millisecond)
		var generation uint64
		var identity string
		var pending sql.NullInt64
		require.NoError(t, root.QueryRowContext(ctx,
			"SELECT w.source_table_id, w.target_identity, w.pending_source_table_id "+
				"FROM mo_catalog.mo_cdc_watermark AS w JOIN mo_catalog.mo_cdc_task AS t "+
				"ON t.account_id = w.account_id AND t.task_id = w.task_id "+
				"WHERE t.task_name = 'cdc_ack_task' AND w.db_name = 'cdc_ack_src' AND w.table_name = 't'").Scan(&generation, &identity, &pending))
		require.NotZero(t, generation)
		require.True(t, strings.HasPrefix(identity, "mo:"))
		require.False(t, pending.Valid)
	})
}

func TestCDCTargetGuardRejectsOptimisticModeBeforeTargetCreate(t *testing.T) {
	require.NoError(t, CloseBaseClusterTests())
	require.NoError(t, CloseSingleCNBaseClusterTests())
	cluster, err := NewCluster(WithTesting(), WithCNCount(2), WithPreStart(func(svc ServiceOperator) {
		if svc.ServiceType() == metadata.ServiceType_CN {
			svc.Adjust(func(cfg *ServiceConfig) {
				cfg.CN.Txn.Mode = txn.TxnMode_Optimistic.String()
				cfg.CN.Txn.Isolation = txn.TxnIsolation_SI.String()
			})
		}
	}))
	require.NoError(t, err)
	defer cluster.Close()
	require.NoError(t, cluster.Start())
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	port := cn.GetServiceConfig().CN.Frontend.Port
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
	require.NoError(t, err)
	defer db.Close()
	_, err = db.ExecContext(ctx, "CREATE DATABASE cdc_optimistic_probe")
	require.NoError(t, err)
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_, _ = db.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS cdc_optimistic_probe")
	}()
	probeTx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = probeTx.ExecContext(ctx, "CALL mo_cdc_target_guard_capability()")
	require.ErrorContains(t, err, "requires a pessimistic read committed transaction")
	require.NoError(t, probeTx.Rollback())
	var tableCount int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM mo_catalog.mo_tables WHERE reldatabase = 'cdc_optimistic_probe' AND relname = 't'").Scan(&tableCount))
	require.Zero(t, tableCount)
	_, err = db.ExecContext(ctx, "CREATE TABLE cdc_optimistic_probe.t (id INT PRIMARY KEY, v INT)")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "ALTER TABLE cdc_optimistic_probe.t ADD COLUMN extra INT")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "TRUNCATE TABLE cdc_optimistic_probe.t")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "RENAME TABLE cdc_optimistic_probe.t TO cdc_optimistic_probe.renamed")
	require.NoError(t, err)
	activeTx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = activeTx.ExecContext(ctx, "CREATE TABLE cdc_optimistic_probe.rejected (id INT PRIMARY KEY)")
	require.ErrorContains(t, err, "require an existing pessimistic RC transaction")
	require.NoError(t, activeTx.Rollback())
	_, err = db.ExecContext(ctx, "DROP TABLE cdc_optimistic_probe.renamed")
	require.NoError(t, err)
}
