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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	fulltextcompile "github.com/matrixorigin/matrixone/pkg/fulltext/plugin/compile"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// Only the hook's SQL runs for real; registration is counted so failures cannot
// publish a task. The enclosing public SQL fixture separately checks DDL rollback.
type classicAdmissionContext struct {
	compileplugin.CompileContext
	table      *plan.TableDef
	database   string
	run        func(string) error
	registered int
}

func (c *classicAdmissionContext) IndexInfo() *plan.CreateTable     { return nil }
func (c *classicAdmissionContext) IsCCPRTaskTransaction() bool      { return false }
func (c *classicAdmissionContext) OriginalTableDef() *plan.TableDef { return c.table }
func (c *classicAdmissionContext) QryDatabase() string              { return c.database }
func (c *classicAdmissionContext) RunSql(sql string) error          { return c.run(sql) }
func (c *classicAdmissionContext) SinkerTypeFromAlgo(_ string) int8 { return 0 }
func (c *classicAdmissionContext) CreateIndexCdcTask(_, _ string, _ uint64, _ string, _ int8, _ bool, _ string, _ *plan.TableDef) error {
	c.registered++
	return nil
}

func testClassicAsyncAdmissionResults(t *testing.T, conn *sql.Conn, service cnservice.Service, database, local, remote string) {
	// generate_series emits serial batches of at most 8192 rows. One extra row
	// proves full consumption without a large stored table or another cluster.
	const rows = 8193
	const source = "classic_admission_source"
	for _, tc := range []struct {
		name     string
		rows     int
		value    string
		badLast  bool
		canceled bool
	}{
		{name: "empty"},
		{name: "one NULL", rows: 1},
		{name: "multiple NULL batches", rows: rows},
		{name: "remote", rows: 1, value: remote},
		{name: "bad second column in later batch", rows: rows, value: local, badLast: true},
		{name: "canceled", rows: 1, canceled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value := "NULL"
			if tc.value != "" {
				value = sqlquote.String(tc.value)
				if tc.badLast {
					value = fmt.Sprintf("case when result=%d then %s else NULL end", rows, value)
				}
			}
			_, err := conn.ExecContext(t.Context(), fmt.Sprintf("create view %s as select result as id, cast(NULL as datalink) as p1, cast(%s as datalink) as p2 from generate_series(1,%d) g where result<=%d", source, value, rows, tc.rows))
			require.NoError(t, err)
			defer conn.ExecContext(t.Context(), "drop view "+source)

			run := func(ctx context.Context, query string, inspect func(executor.Result)) error {
				return sqlexec.RunTxnWithSqlContext(ctx, service.GetEngine(), service.GetTxnClient(), service.ID(), 0, time.Minute, nil, nil, func(sp *sqlexec.SqlProcess, _ any) error {
					result, err := sqlexec.RunSql(sp, query)
					if err != nil {
						return err
					}
					defer result.Close()
					inspect(result)
					return nil
				})
			}
			if tc.badLast {
				var batchRows []int
				badBatch := -1
				require.NoError(t, run(t.Context(), "select id from "+sqlquote.QualifiedIdent(database, source), func(result executor.Result) {
					for i, b := range result.Batches {
						batchRows = append(batchRows, b.RowCount())
						for _, id := range executor.GetFixedRows[int64](b.Vecs[0]) {
							if id == rows {
								badBatch = i
							}
						}
					}
				}))
				require.Equal(t, []int{8192, 1}, batchRows)
				require.Equal(t, 1, badBatch, "serial source puts the forbidden value in the later batch")
			}

			ctx := t.Context()
			if tc.canceled {
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = canceled
			}
			var resultRows, batches, allocated int
			var sums []types.Decimal128
			var nulls []bool
			hookCtx := &classicAdmissionContext{database: database, table: &plan.TableDef{Name: source, Cols: []*plan.ColDef{
				{Name: "p1", Typ: plan.Type{Id: int32(types.T_datalink)}},
				{Name: "p2", Typ: plan.Type{Id: int32(types.T_datalink)}},
			}}}
			hookCtx.run = func(query string) error {
				return run(ctx, query, func(result executor.Result) {
					for _, b := range result.Batches {
						batches++
						resultRows += b.RowCount()
						for _, v := range b.Vecs {
							allocated += v.Allocated()
							if v.GetType().Oid == types.T_decimal128 {
								sums = append(sums, executor.GetFixedRows[types.Decimal128](v)...)
								nulls = append(nulls, v.GetNulls().Contains(0))
							}
						}
					}
				})
			}
			err = (fulltextcompile.Hooks{}).HandleCreateIndex(hookCtx, map[string]*plan.IndexDef{"": {
				IndexAlgo: catalog.MOIndexFullTextAlgo.ToString(), IndexAlgoParams: `{"async":"true"}`, Parts: []string{"p1", "p2"},
			}})
			if tc.badLast || tc.canceled {
				require.Error(t, err)
				if tc.badLast {
					require.ErrorContains(t, err, "asynchronous indexes require remote Datalink sources")
				}
				require.Zero(t, hookCtx.registered)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1, hookCtx.registered)
			require.Equal(t, 1, resultRows, "admission must not retain one output per source row")
			require.Equal(t, 1, batches)
			require.Len(t, sums, 2)
			require.Equal(t, []bool{tc.rows == 0, tc.rows == 0}, nulls)
			if tc.rows != 0 {
				require.Equal(t, []types.Decimal128{types.Decimal128FromInt64(int64(tc.rows)), types.Decimal128FromInt64(int64(tc.rows))}, sums)
				require.LessOrEqual(t, allocated, 512, "two fixed-width result vectors must not grow with source rows")
			}
			t.Logf("source rows=%d retained rows=%d batches=%d allocated vector bytes=%d", tc.rows, resultRows, batches, allocated)
		})
	}
}
