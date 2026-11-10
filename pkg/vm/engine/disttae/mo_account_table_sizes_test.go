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

package disttae

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/stretchr/testify/require"
)

type accountSizeExecutor struct {
	ie.InternalExecutor
	query func(context.Context, string) ie.InternalExecResult
}

func (e accountSizeExecutor) Query(ctx context.Context, sql string, _ ie.SessionOverrideOptions) ie.InternalExecResult {
	return e.query(ctx, sql)
}

type accountSizeResult struct {
	ie.InternalExecResult
	rows     [][]any
	err      error
	valueErr func(uint64, uint64) error
}

func (r accountSizeResult) Error() error        { return r.err }
func (r accountSizeResult) RowCount() uint64    { return uint64(len(r.rows)) }
func (r accountSizeResult) ColumnCount() uint64 { return 3 }
func (r accountSizeResult) Value(_ context.Context, row, col uint64) (any, error) {
	if r.valueErr != nil {
		if err := r.valueErr(row, col); err != nil {
			return nil, err
		}
	}
	return r.rows[row][col], nil
}
func (r accountSizeResult) GetUint64(ctx context.Context, row, col uint64) (uint64, error) {
	value, err := r.Value(ctx, row, col)
	if err != nil {
		return 0, err
	}
	return value.(uint64), nil
}

func TestQueryAccountTableSizesNormal(t *testing.T) {
	d := &dynamicCtx{}
	calls := 0
	d.executorPool.New = func() any {
		return accountSizeExecutor{query: func(ctx context.Context, sql string) ie.InternalExecResult {
			calls++
			account, err := defines.GetAccountId(ctx)
			require.NoError(t, err)
			require.Zero(t, account, "catalog query must run as sys with an explicit account filter")
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			for _, fragment := range []string{
				"from mo_catalog.mo_tables as m", "left join mo_catalog.mo_table_stats_alpha as s",
				"m.account_id = s.account_id", "m.reldatabase_id = s.database_id", "m.rel_id = s.table_id",
				"where m.account_id in (7,9,11)", "group by m.account_id",
				"sum(cast(floor(coalesce(json_extract_float64(s.table_stats, '$.table_size'), 0))",
			} {
				require.Contains(t, sql, fragment)
			}
			require.NotContains(t, sql, "relkind", "size includes all live catalog tables, not the displayed table-count subset")
			require.NotContains(t, sql, "table_id in")
			require.NotContains(t, sql, "database_id in")
			return accountSizeResult{rows: [][]any{
				{uint64(9), uint64(0), uint64(1)},
				{uint64(7), uint64(1<<53 + 1), uint64(3)},
			}}
		}}
	}
	sizes, ok, err := d.QueryAccountTableSizes(defines.AttachAccountId(context.Background(), 7), []uint64{7, 9, 11}, false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, map[uint64]uint64{7: 1<<53 + 1, 9: 0}, sizes)
	require.NotContains(t, sizes, uint64(11), "no live tables must not erase the TN fallback")
	require.Equal(t, 1, calls, "normal queries must not enumerate table IDs first")
}

func TestQueryAccountTableSizesAdmission(t *testing.T) {
	for _, name := range []string{"empty", "old implementation", "canceled"} {
		t.Run(name, func(t *testing.T) {
			d := &dynamicCtx{}
			calls := 0
			d.executorPool.New = func() any {
				calls++
				return statsDispatchExecutor{err: errors.New("unexpected SQL")}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			accounts := []uint64{7}
			switch name {
			case "empty":
				accounts = nil
			case "old implementation":
				d.conf.StatsUsingOldImpl = true
				d.conf.ForceUpdate = true
			case "canceled":
				cancel()
			}
			sizes, ok, err := d.QueryAccountTableSizes(ctx, accounts, true)
			if name == "canceled" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}
			require.False(t, ok)
			require.Nil(t, sizes)
			require.Zero(t, calls)
		})
	}
}

func TestQueryAccountTableSizesErrors(t *testing.T) {
	sentinel := errors.New("account statistics unavailable")
	for _, name := range []string{"query", "account column", "size column", "count column", "canceled after query", "canceled empty query", "canceled while decoding"} {
		t.Run(name, func(t *testing.T) {
			d := &dynamicCtx{}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			d.executorPool.New = func() any {
				return accountSizeExecutor{query: func(context.Context, string) ie.InternalExecResult {
					ret := accountSizeResult{rows: [][]any{
						{uint64(7), uint64(10), uint64(1)},
						{uint64(9), uint64(20), uint64(1)},
					}}
					if name == "query" {
						ret.err = sentinel
					} else if name == "canceled while decoding" {
						ret.valueErr = func(uint64, uint64) error { cancel(); return nil }
					} else if strings.HasPrefix(name, "canceled") {
						cancel()
						if name == "canceled empty query" {
							ret.rows = nil
						}
					} else {
						badColumn := map[string]uint64{"account column": 0, "size column": 1, "count column": 2}[name]
						ret.valueErr = func(row, col uint64) error {
							if row == 1 && col == badColumn {
								return sentinel
							}
							return nil
						}
					}
					return ret
				}}
			}
			sizes, ok, err := d.QueryAccountTableSizes(ctx, []uint64{7, 9}, false)
			if strings.HasPrefix(name, "canceled") {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, sentinel)
			}
			require.Nil(t, sizes, "a partially decoded account map must never be published")
			require.False(t, ok)
		})
	}
}

func TestQueryAccountTableSizesForceUpdate(t *testing.T) {
	for _, ownerForce := range []bool{false, true} {
		for _, phase := range []string{"catalog error", "decode error", "no tables", "refresh"} {
			t.Run(fmt.Sprintf("ownerForce=%v/%s", ownerForce, phase), func(t *testing.T) {
				d := &dynamicCtx{}
				d.conf.ForceUpdate = ownerForce
				sentinel := errors.New("selected force-update path")
				calls := 0
				d.executorPool.New = func() any {
					return accountSizeExecutor{query: func(_ context.Context, sql string) ie.InternalExecResult {
						calls++
						if calls == 1 {
							require.Contains(t, sql, "account_id, reldatabase_id, rel_id")
							require.NotContains(t, sql, "group by")
							d.setUseOldImpl(true)
							d.setForceUpdate(false)
							ret := accountSizeResult{}
							switch phase {
							case "catalog error":
								ret.err = sentinel
							case "decode error":
								ret.rows = [][]any{{uint32(7), uint64(10), uint64(20)}}
								ret.valueErr = func(uint64, uint64) error { return sentinel }
							case "refresh":
								ret.rows = [][]any{{uint32(7), uint64(10), uint64(20)}}
							}
							return ret
						}
						require.Equal(t, 2, calls)
						require.Contains(t, sql, "table_id, update_time")
						require.NotContains(t, sql, "COALESCE", "must refresh, not reinterpret the changed owner policy")
						return accountSizeResult{err: sentinel}
					}}
				}
				sizes, ok, err := d.QueryAccountTableSizes(context.Background(), []uint64{7}, !ownerForce)
				if phase == "no tables" {
					require.NoError(t, err)
					require.True(t, ok)
					require.Empty(t, sizes)
				} else {
					require.ErrorIs(t, err, sentinel)
					require.Nil(t, sizes)
				}
				if phase == "refresh" {
					require.Equal(t, 2, calls)
				} else {
					require.Equal(t, 1, calls)
				}
			})
		}
	}
}

func TestQueryAccountTableSizesNormalPolicyIsStable(t *testing.T) {
	d := &dynamicCtx{}
	calls := 0
	d.executorPool.New = func() any {
		return accountSizeExecutor{query: func(_ context.Context, sql string) ie.InternalExecResult {
			calls++
			require.True(t, strings.Contains(sql, "group by m.account_id"))
			d.setUseOldImpl(true)
			d.setForceUpdate(true)
			return accountSizeResult{rows: [][]any{{uint64(7), uint64(12), uint64(2)}}}
		}}
	}
	sizes, ok, err := d.QueryAccountTableSizes(context.Background(), []uint64{7}, false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, map[uint64]uint64{7: 12}, sizes)
	sizes, ok, err = d.QueryAccountTableSizes(context.Background(), []uint64{7}, false)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, sizes)
	require.Equal(t, 1, calls)
}

// Measure only Go-side query construction/decoding, not SQL execution. Both
// paths receive prebuilt executor results for the same four-account dataset.
func BenchmarkAccountTableSizesMaterialization(b *testing.B) {
	stats, err := types.ParseStringToByteJson(`{"table_size":1.8,"table_rows":1,"tobject_cnt":0,"dobject_cnt":1,"tblock_cnt":0,"dblock_cnt":1}`)
	require.NoError(b, err)
	accounts := []uint64{0, 1, 2, 3}
	for _, tables := range []int{100, 10000} {
		ids := accountSizeResult{rows: make([][]any, tables)}
		values := accountSizeResult{rows: make([][]any, tables)}
		for i := range tables {
			ids.rows[i] = []any{uint32(i % 4), uint64(i%4 + 1), uint64(i + 100)}
			values.rows[i] = []any{int64(i + 100), nil, stats}
		}
		aggregates := accountSizeResult{}
		for _, account := range accounts {
			aggregates.rows = append(aggregates.rows, []any{account, uint64(tables / 4), uint64(tables / 4)})
		}
		for _, aggregate := range []bool{false, true} {
			b.Run(fmt.Sprintf("tables=%d/aggregate=%v", tables, aggregate), func(b *testing.B) {
				d := &dynamicCtx{}
				d.executorPool.New = func() any {
					return accountSizeExecutor{query: func(_ context.Context, sql string) ie.InternalExecResult {
						if strings.Contains(sql, "group by m.account_id") {
							return aggregates
						}
						if strings.Contains(sql, "account_id, reldatabase_id, rel_id") {
							return ids
						}
						return values
					}}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					var sizes map[uint64]uint64
					var ok bool
					var err error
					if aggregate {
						sizes, ok, err = d.QueryAccountTableSizes(context.Background(), accounts, false)
					} else {
						var vals [][]any
						var tableAccounts []uint64
						vals, tableAccounts, ok, err = d.QueryTableStatsByAccounts(context.Background(), []int{TableStatsTableSize}, accounts, false, false)
						sizes = make(map[uint64]uint64)
						for i, account := range tableAccounts {
							sizes[account] += uint64(vals[0][i].(float64))
						}
					}
					if err != nil || !ok || len(sizes) != len(accounts) || sizes[0] != uint64(tables/4) {
						b.Fatalf("unexpected account sizes: %v, %v, %v", sizes, ok, err)
					}
				}
			})
		}
	}
}
