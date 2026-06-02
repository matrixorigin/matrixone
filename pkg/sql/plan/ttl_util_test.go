// Copyright 2021 - 2022 Matrix Origin
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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// ttlProps collects the persisted TTL properties of a TableDef keyed by their catalog attr.
func ttlProps(tableDef *plan.TableDef) map[string]string {
	out := make(map[string]string)
	for _, def := range tableDef.Defs {
		proDef, ok := def.Def.(*plan.TableDef_DefType_Properties)
		if !ok {
			continue
		}
		for _, kv := range proDef.Properties.Properties {
			switch kv.Key {
			case catalog.SystemRelAttr_TTL, catalog.SystemRelAttr_TTLEnable, catalog.SystemRelAttr_TTLJobInterval:
				out[kv.Key] = kv.Value
			}
		}
	}
	return out
}

// parseTTLExpr parses a CREATE TABLE statement and returns its TTL expiry expression.
func parseTTLExpr(t *testing.T, sql string) tree.Expr {
	t.Helper()
	stmts, err := mysql.Parse(context.Background(), sql, 1)
	require.NoError(t, err)
	ct, ok := stmts[0].(*tree.CreateTable)
	require.True(t, ok)
	for _, opt := range ct.Options {
		if ttl, ok := opt.(*tree.TableOptionTTL); ok {
			return ttl.Expr
		}
	}
	t.Fatalf("no TTL option in %q", sql)
	return nil
}

func TestValidateTTLExpr(t *testing.T) {
	ctx := context.Background()
	cols := map[string]int32{
		"created_at": int32(types.T_timestamp),
		"dt":         int32(types.T_datetime),
		"d":          int32(types.T_date),
		"id":         int32(types.T_int32),
	}

	t.Run("timestamp column ok", func(t *testing.T) {
		expr := parseTTLExpr(t, "CREATE TABLE t (created_at TIMESTAMP) TTL = `created_at` + INTERVAL 7 DAY")
		require.NoError(t, validateTTLExpr(ctx, expr, cols))
	})
	t.Run("datetime column ok", func(t *testing.T) {
		expr := parseTTLExpr(t, "CREATE TABLE t (dt DATETIME) TTL = dt + INTERVAL 1 HOUR")
		require.NoError(t, validateTTLExpr(ctx, expr, cols))
	})
	t.Run("date column ok", func(t *testing.T) {
		expr := parseTTLExpr(t, "CREATE TABLE t (d DATE) TTL = d + INTERVAL 30 DAY")
		require.NoError(t, validateTTLExpr(ctx, expr, cols))
	})
	t.Run("missing column", func(t *testing.T) {
		expr := parseTTLExpr(t, "CREATE TABLE t (created_at TIMESTAMP) TTL = nope + INTERVAL 7 DAY")
		require.Error(t, validateTTLExpr(ctx, expr, cols))
	})
	t.Run("non time-type column", func(t *testing.T) {
		expr := parseTTLExpr(t, "CREATE TABLE t (id INT) TTL = id + INTERVAL 7 DAY")
		require.Error(t, validateTTLExpr(ctx, expr, cols))
	})
}

func TestNormalizeTTLEnable(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		in, want string
		wantErr  bool
	}{
		{"ON", "on", false},
		{"on", "on", false},
		{"OFF", "off", false},
		{" off ", "off", false},
		{"yes", "", true},
		{"", "", true},
	} {
		got, err := normalizeTTLEnable(ctx, c.in)
		if c.wantErr {
			require.Error(t, err, "input %q", c.in)
			continue
		}
		require.NoError(t, err, "input %q", c.in)
		require.Equal(t, c.want, got)
	}
}

func TestValidateTTLJobInterval(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		in      string
		wantErr bool
	}{
		{"1h", false},
		{"30m", false},
		{"10s", false},
		{"0s", true},
		{"-1h", true},
		{"abc", true},
		{"", true},
	} {
		got, err := validateTTLJobInterval(ctx, c.in)
		if c.wantErr {
			require.Error(t, err, "input %q", c.in)
			continue
		}
		require.NoError(t, err, "input %q", c.in)
		require.Equal(t, c.in, got)
	}
}

func TestTTLExprStringRoundTrip(t *testing.T) {
	expr := parseTTLExpr(t, "CREATE TABLE t (created_at TIMESTAMP) TTL = `created_at` + INTERVAL 7 DAY")
	s := ttlExprString(expr)

	// The reflected expression must parse again as a TTL option.
	stmts, err := mysql.Parse(context.Background(), "CREATE TABLE t (created_at TIMESTAMP) TTL = "+s, 1)
	require.NoError(t, err)
	ct := stmts[0].(*tree.CreateTable)
	var found bool
	for _, opt := range ct.Options {
		if ttl, ok := opt.(*tree.TableOptionTTL); ok {
			require.Equal(t, s, ttlExprString(ttl.Expr))
			found = true
		}
	}
	require.True(t, found, "reflected TTL expr did not round-trip: %q", s)
}

func TestCreateTableTTLPersist(t *testing.T) {
	mock := NewMockOptimizer(false)

	t.Run("defaults applied", func(t *testing.T) {
		tableDef, err := buildTestCreateTableStmt(mock,
			"CREATE TABLE t (id INT, created_at TIMESTAMP) TTL = `created_at` + INTERVAL 7 DAY")
		require.NoError(t, err)
		props := ttlProps(tableDef)
		require.Equal(t, "`created_at` + INTERVAL 7 day", props[catalog.SystemRelAttr_TTL])
		require.Equal(t, "on", props[catalog.SystemRelAttr_TTLEnable])
		require.Equal(t, "1h", props[catalog.SystemRelAttr_TTLJobInterval])
	})

	t.Run("explicit enable and job interval", func(t *testing.T) {
		tableDef, err := buildTestCreateTableStmt(mock,
			"CREATE TABLE t (id INT, dt DATETIME) TTL = dt + INTERVAL 1 HOUR TTL_ENABLE = 'OFF' TTL_JOB_INTERVAL = '30m'")
		require.NoError(t, err)
		props := ttlProps(tableDef)
		require.Equal(t, "`dt` + INTERVAL 1 hour", props[catalog.SystemRelAttr_TTL])
		require.Equal(t, "off", props[catalog.SystemRelAttr_TTLEnable])
		require.Equal(t, "30m", props[catalog.SystemRelAttr_TTLJobInterval])
	})

	t.Run("no TTL means no TTL properties", func(t *testing.T) {
		tableDef, err := buildTestCreateTableStmt(mock, "CREATE TABLE t (id INT, created_at TIMESTAMP)")
		require.NoError(t, err)
		require.Empty(t, ttlProps(tableDef))
	})
}

func buildTTLCreateErr(t *testing.T, sql string) error {
	t.Helper()
	mock := NewMockOptimizer(false)
	cctx := mock.CurrentContext()
	stmts, err := mysql.Parse(cctx.GetContext(), sql, 1)
	require.NoError(t, err)
	_, err = BuildPlan(cctx, stmts[0], false)
	return err
}

func TestCreateTableTTLErrors(t *testing.T) {
	for _, c := range []struct {
		name string
		sql  string
	}{
		{"column not exist", "CREATE TABLE t (id INT, created_at TIMESTAMP) TTL = nope + INTERVAL 7 DAY"},
		{"column not time type", "CREATE TABLE t (id INT) TTL = id + INTERVAL 7 DAY"},
		{"bad job interval", "CREATE TABLE t (created_at TIMESTAMP) TTL = `created_at` + INTERVAL 7 DAY TTL_JOB_INTERVAL = 'abc'"},
		{"bad enable", "CREATE TABLE t (created_at TIMESTAMP) TTL = `created_at` + INTERVAL 7 DAY TTL_ENABLE = 'maybe'"},
		{"enable without ttl", "CREATE TABLE t (created_at TIMESTAMP) TTL_ENABLE = 'on'"},
		{"job interval without ttl", "CREATE TABLE t (created_at TIMESTAMP) TTL_JOB_INTERVAL = '1h'"},
		{"temporary table", "CREATE TEMPORARY TABLE t (created_at TIMESTAMP) TTL = `created_at` + INTERVAL 7 DAY"},
	} {
		t.Run(c.name, func(t *testing.T) {
			require.Error(t, buildTTLCreateErr(t, c.sql), "sql=%s", c.sql)
		})
	}
}

func TestShowCreateTableTTLRoundTrip(t *testing.T) {
	for _, sql := range []string{
		"CREATE TABLE t (id INT, created_at TIMESTAMP) TTL = `created_at` + INTERVAL 7 DAY",
		"CREATE TABLE t (id INT, created_at TIMESTAMP) TTL = `created_at` + INTERVAL 7 DAY TTL_ENABLE = 'OFF' TTL_JOB_INTERVAL = '30m'",
		"CREATE TABLE t (id INT, dt DATETIME) TTL = dt + INTERVAL 1 HOUR",
	} {
		got, err := buildTestShowCreateTable(sql)
		require.NoError(t, err, "sql=%s", sql)
		require.Contains(t, got, " TTL = ")
		require.Contains(t, got, " TTL_ENABLE = '")
		require.Contains(t, got, " TTL_JOB_INTERVAL = '")
		// The reflected SHOW CREATE TABLE statement must parse again.
		_, perr := mysql.Parse(context.Background(), got, 1)
		require.NoError(t, perr, "SHOW output not re-parseable: %s", got)
	}
}
