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

package db_holder

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/stretchr/testify/require"
)

func TestStatementInfoDiagnosticCapacity(t *testing.T) {
	for _, input := range []string{
		"", "\xff", strings.Repeat("a", StatementInfoTextLimit-1), strings.Repeat("a", StatementInfoTextLimit), strings.Repeat("a", StatementInfoTextLimit+1),
		strings.Repeat("é", StatementInfoTextLimit), strings.Repeat("你", StatementInfoTextLimit), strings.Repeat("🙂", StatementInfoTextLimit),
		strings.Repeat("a", StatementInfoTextLimit) + "...[truncated]", strings.Repeat("a\xff", StatementInfoTextLimit),
	} {
		got := CapStatementInfoText(input, StatementInfoTextLimit)
		require.LessOrEqual(t, len(got), StatementInfoTextLimit)
		if len(input) <= StatementInfoTextLimit {
			require.Equal(t, input, got)
		} else {
			require.True(t, utf8.ValidString(got))
			require.True(t, strings.HasSuffix(got, StatementInfoTruncationMarker))
			require.NotEmpty(t, strings.TrimSuffix(got, StatementInfoTruncationMarker))
		}
		require.Equal(t, got, CapStatementInfoText(got, StatementInfoTextLimit))
	}
	normal := strings.Repeat("select 1", 1024)
	var retained string
	require.Zero(t, testing.AllocsPerRun(100, func() { retained = CapStatementInfoText(normal, StatementInfoTextLimit) }))
	require.Equal(t, normal, retained)
	for _, original := range []int{StatementInfoTextLimit + 1, math.MaxInt} {
		summary := StatementInfoPlanSummary(original)
		require.Less(t, len(summary), 256)
		var result struct {
			Code          int
			Truncated     bool
			OriginalBytes int `json:"original_bytes"`
			LimitBytes    int `json:"limit_bytes"`
		}
		require.NoError(t, json.Unmarshal(summary, &result))
		require.Equal(t, 200, result.Code)
		require.True(t, result.Truncated)
		require.Equal(t, original, result.OriginalBytes)
		require.Equal(t, StatementInfoTextLimit, result.LimitBytes)
	}
}

func TestBulkInsertStatementInfoHistoricalDiagnostics(t *testing.T) {
	columns := []table.Column{table.TextColumn("exec_plan", ""), table.TextColumn("other", ""), table.TextColumn("error", ""), table.TextColumn("statement", "")}
	normal := []string{`{"normal":"\\'\n"}`, "untouched", "error ' \\ \n", strings.Repeat("'\\\n", 21845)}
	oversized := []string{`{"plan":"` + strings.Repeat("p", StatementInfoTextLimit) + `"}`, "untouched", strings.Repeat("🙂", 20000), strings.Repeat("x", StatementInfoTextLimit+1)}
	for _, database := range []string{"system", "other"} {
		t.Run(database, func(t *testing.T) {
			records := [][]string{append([]string(nil), normal...), append([]string(nil), oversized...)}
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(func(_, query string) error {
				prefix := "LOAD DATA INLINE FORMAT='csv', DATA='"
				suffix := fmt.Sprintf("' INTO TABLE %s.statement_info FIELDS TERMINATED BY ','", database)
				if !strings.HasPrefix(query, prefix) || !strings.HasSuffix(query, suffix) {
					return fmt.Errorf("unexpected LOAD envelope")
				}
				data := strings.TrimSuffix(strings.TrimPrefix(query, prefix), suffix)
				data = strings.ReplaceAll(strings.ReplaceAll(data, "''", "'"), "\\\\", "\\")
				decoded, err := csv.NewReader(strings.NewReader(data)).ReadAll()
				if err != nil {
					return err
				}
				require.Equal(t, normal, decoded[0])
				require.Equal(t, "untouched", decoded[1][1])
				if database == "system" {
					for _, idx := range []int{2, 3} {
						require.LessOrEqual(t, len(decoded[1][idx]), StatementInfoTextLimit)
						require.True(t, utf8.ValidString(decoded[1][idx]))
						require.True(t, strings.HasSuffix(decoded[1][idx], StatementInfoTruncationMarker))
					}
					var plan map[string]any
					require.NoError(t, json.Unmarshal([]byte(decoded[1][0]), &plan))
					require.Equal(t, true, plan["truncated"])
					require.Equal(t, float64(len(oversized[0])), plan["original_bytes"])
				} else {
					require.Equal(t, oversized, decoded[1])
				}
				return nil
			})))
			require.NoError(t, err)
			defer db.Close()
			mock.ExpectExec("load").WillReturnResult(sqlmock.NewResult(0, 2))
			require.NoError(t, bulkInsert(context.Background(), db, records, &table.Table{Database: database, Table: "statement_info", Columns: columns}))
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestBulkInsertStatementInfoRejectsAmbiguousLayout(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	columns := []table.Column{table.TextColumn("statement", ""), table.TextColumn("error", ""), table.TextColumn("exec_plan", "")}
	for _, tc := range []struct {
		columns []table.Column
		row     []string
	}{
		{nil, []string{"unknown"}}, {append(append([]table.Column(nil), columns...), columns[0]), []string{"s", "e", "{}", "s"}}, {columns, []string{"s", "e"}},
	} {
		require.Error(t, bulkInsert(context.Background(), db, [][]string{tc.row}, &table.Table{Database: "system", Table: "statement_info", Columns: tc.columns}))
	}
	require.NoError(t, mock.ExpectationsWereMet())
}

// A no-op connection isolates normalization, escaping and CSV construction from
// network timing while retaining bulkInsert's database/sql execution boundary.
type capacityBenchmarkConnector struct{}
type capacityBenchmarkDriver struct{}
type capacityBenchmarkConn struct{}

func (capacityBenchmarkConnector) Connect(context.Context) (driver.Conn, error) {
	return capacityBenchmarkConn{}, nil
}
func (capacityBenchmarkConnector) Driver() driver.Driver          { return capacityBenchmarkDriver{} }
func (capacityBenchmarkDriver) Open(string) (driver.Conn, error)  { return capacityBenchmarkConn{}, nil }
func (capacityBenchmarkConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unused") }
func (capacityBenchmarkConn) Close() error                        { return nil }
func (capacityBenchmarkConn) Begin() (driver.Tx, error)           { return nil, errors.New("unused") }
func (capacityBenchmarkConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return driver.RowsAffected(8), nil
}

func BenchmarkStatementInfoHistoricalBatch(b *testing.B) {
	db := sql.OpenDB(capacityBenchmarkConnector{})
	defer db.Close()
	tbl := &table.Table{Database: "system", Table: "statement_info", Columns: []table.Column{table.TextColumn("statement", ""), table.TextColumn("error", ""), table.TextColumn("exec_plan", "")}}
	fixture := []string{"select 'normal' \\ \n", "normal error", `{"plan":"small"}`}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		records := make([][]string, 8)
		for j := range records {
			records[j] = append([]string(nil), fixture...)
		}
		if err := bulkInsert(ctx, db, records, tbl); err != nil {
			b.Fatal(err)
		}
	}
}
