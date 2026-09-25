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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
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
	})
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
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
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
		readRows := func() []int {
			rows, readErr := account.QueryContext(ctx, "SELECT id FROM cdc_generation_dst.t ORDER BY id")
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
		require.Eventually(t, func() bool { return reflect.DeepEqual(readRows(), []int{1, 2}) }, 90*time.Second, 250*time.Millisecond)
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
		var accountID uint64
		var taskID string
		require.NoError(t, root.QueryRowContext(ctx,
			"SELECT account_id, task_id FROM mo_catalog.mo_cdc_task WHERE task_name = 'cdc_generation_task'").Scan(&accountID, &taskID))
		// Exercise the recovery boundary where an old generation checkpoint is
		// later than the new table's snapshot epoch. The replacement must replay
		// from its own epoch, never from this retired watermark.
		futureTS := types.BuildTS(4_000_000_000_000_000_000, 0)
		futureWatermark := futureTS.ToString()
		catalogExecutor := frontend.NewInternalExecutor(cn.GetServiceConfig().CN.UUID)
		err = catalogExecutor.Exec(defines.AttachAccountId(ctx, catalog.System_Account),
			fmt.Sprintf("UPDATE mo_catalog.mo_cdc_watermark SET watermark = '%s' "+
				"WHERE account_id = %d AND task_id = '%s' AND db_name = 'cdc_generation_src' AND table_name = 't'",
				futureWatermark, accountID, taskID), ie.SessionOverrideOptions{})
		require.NoError(t, err)
		injectedGeneration, injectedWatermark, readErr := readProgress()
		require.NoError(t, readErr)
		require.Equal(t, oldGeneration, injectedGeneration)
		require.Equal(t, futureWatermark, injectedWatermark)
		_, err = account.ExecContext(ctx, "DROP TABLE cdc_generation_src.t")
		require.NoError(t, err)
		_, err = account.ExecContext(ctx, "CREATE TABLE cdc_generation_src.t (id INT PRIMARY KEY, v INT)")
		require.NoError(t, err)
		_, err = account.ExecContext(ctx, "INSERT INTO cdc_generation_src.t VALUES (3,30),(4,40)")
		require.NoError(t, err)
		_, err = account.ExecContext(ctx, "RESUME CDC TASK cdc_generation_task")
		require.NoError(t, err)
		require.Eventually(t, func() bool { return reflect.DeepEqual(readRows(), []int{3, 4}) }, 90*time.Second, 250*time.Millisecond)
		require.Eventually(t, func() bool {
			generation, watermark, readErr := readProgress()
			return readErr == nil && generation > oldGeneration && watermark != "" &&
				watermark != "0-0" && watermark != futureWatermark
		}, 30*time.Second, 250*time.Millisecond)
		_, recoveredWatermark, readErr := readProgress()
		require.NoError(t, readErr)
		recoveredTS := types.StringToTS(recoveredWatermark)
		require.True(t, recoveredTS.LT(&futureTS))
	})
}
