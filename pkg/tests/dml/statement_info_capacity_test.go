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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/stretchr/testify/require"
)

type diagnosticCapacityPlan []byte

func (p diagnosticCapacityPlan) Marshal(context.Context) []byte { return p }
func (diagnosticCapacityPlan) Free()                            {}
func (diagnosticCapacityPlan) Stats(context.Context) (statistic.StatsArray, motrace.Statistic) {
	return *statistic.NewStatsArray(), motrace.Statistic{}
}

func TestStatementInfoDiagnosticCapacitySQL(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		db := openRetestSQLDB(t, c)
		defer db.Close()
		execSQLDB(t, ctx, db, "set sql_mode='STRICT_TRANS_TABLES'")
		execSQLDB(t, ctx, db, "create database if not exists system")
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		created, err := testutils.GetSQLExecutor(cn).Exec(ctx, motrace.SingleStatementTable.ToCreateSql(ctx, true), executor.Options{})
		require.NoError(t, err)
		created.Close()
		user, err := db_holder.GetSQLWriterDBUser()
		require.NoError(t, err)
		config := mysql.NewConfig()
		config.User, config.Passwd = user.UserName, user.Password
		config.Net, config.Addr = "tcp", fmt.Sprintf("127.0.0.1:%d", cn.GetServiceConfig().CN.Frontend.Port)
		loggerDB, err := sql.Open("mysql", config.FormatDSN())
		require.NoError(t, err)
		defer loggerDB.Close()
		loggerDB.SetMaxOpenConns(1)
		execSQLDB(t, ctx, loggerDB, "set sql_mode='STRICT_TRANS_TABLES'")
		ids := []string{uuid.NewString(), uuid.NewString()}
		defer func() {
			_, err := loggerDB.ExecContext(context.Background(), "delete from system.statement_info where statement_id in (?,?)", ids[0], ids[1])
			require.NoError(t, err)
		}()
		plan := `{"plan":"` + strings.Repeat("p", 65536) + `"}`
		s := &motrace.StatementInfo{StatementID: [16]byte(uuid.MustParse(ids[0])), Account: "sys", User: "dump", Statement: []byte(strings.Repeat("你", 30000)), Error: errors.New(strings.Repeat("e", 65536)), ExecPlan: diagnosticCapacityPlan(plan), Status: motrace.StatementStatusFailed, RequestAt: time.Now().UTC(), ResponseAt: time.Now().UTC(), RowsRead: 7, BytesScan: 123}
		row := motrace.SingleStatementTable.GetRow(ctx)
		defer row.Free()
		s.FillRow(ctx, row)
		live := row.ToStrings()
		legacy := append([]string(nil), live...)
		var stats string
		for i, col := range motrace.SingleStatementTable.Columns {
			switch col.Name {
			case "statement_id":
				legacy[i] = ids[1]
			case "statement":
				legacy[i] = strings.Repeat("x", 65536)
			case "error":
				legacy[i] = strings.Repeat("'\\\n", 22000)
			case "exec_plan":
				legacy[i] = plan
			case "stats":
				stats = live[i]
			}
		}
		count, err := db_holder.WriteRowRecords([][]string{live, legacy}, motrace.SingleStatementTable, time.Minute)
		require.NoError(t, err)
		require.Equal(t, 2, count)
		for _, id := range ids {
			var statement, errorText, execPlan, gotStats string
			var rowsRead, bytesScan int64
			require.NoError(t, loggerDB.QueryRowContext(ctx, "select statement,error,exec_plan,stats,rows_read,bytes_scan from system.statement_info where statement_id=?", id).Scan(&statement, &errorText, &execPlan, &gotStats, &rowsRead, &bytesScan))
			for _, text := range []string{statement, errorText} {
				require.LessOrEqual(t, len(text), 65535)
				require.True(t, utf8.ValidString(text))
				require.True(t, strings.HasSuffix(text, db_holder.StatementInfoTruncationMarker))
			}
			var summary map[string]any
			require.NoError(t, json.Unmarshal([]byte(execPlan), &summary))
			require.Equal(t, true, summary["truncated"])
			require.Equal(t, float64(len(plan)), summary["original_bytes"])
			if id == ids[1] {
				want := strings.Repeat("'\\\n", 22000)
				want = want[:65535-len(db_holder.StatementInfoTruncationMarker)] + db_holder.StatementInfoTruncationMarker
				require.Equal(t, want, errorText)
			}
			require.Equal(t, stats, gotStats)
			require.Equal(t, int64(7), rowsRead)
			require.Equal(t, int64(123), bytesScan)
		}
		// The telemetry policy must not weaken ordinary strict TEXT assignments.
		execSQLDB(t, ctx, db, "create database issue29797_control")
		defer cleanupTestDatabases(t, db, "issue29797_control")
		execSQLDB(t, ctx, db, "create table issue29797_control.t(d text)")
		execSQLDB(t, ctx, db, "LOAD DATA INLINE FORMAT='csv', DATA='normal\n' INTO TABLE issue29797_control.t FIELDS TERMINATED BY ','")
		_, err = db.ExecContext(ctx, "LOAD DATA INLINE FORMAT='csv', DATA='"+strings.Repeat("x", 65536)+"\n' INTO TABLE issue29797_control.t FIELDS TERMINATED BY ','")
		var sqlErr *mysql.MySQLError
		require.ErrorAs(t, err, &sqlErr)
		require.Equal(t, uint16(1406), sqlErr.Number)
	})
}
