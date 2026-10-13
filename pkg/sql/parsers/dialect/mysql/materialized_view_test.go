// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.
package mysql

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewSyntax(t *testing.T) {
	for _, sql := range []string{
		"create materialized view mv refresh manual as select k,sum(v) as s from t group by k",
		"create materialized view if not exists db.mv refresh every interval 2 minute as select * from t",
		"refresh materialized view db.mv",
		"drop materialized view if exists db.mv",
		"alter materialized view db.mv refresh manual",
		"alter materialized view db.mv refresh every interval 1 hour",
		"show materialized views",
		"show materialized view refreshes db.mv",
		"show create materialized view db.mv",
	} {
		t.Run(sql, func(t *testing.T) {
			stmts, err := Parse(context.Background(), sql, 1)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			defer stmts[0].Free()
			canonical := tree.String(stmts[0], dialect.MYSQL)
			again, err := Parse(context.Background(), canonical, 1)
			require.NoError(t, err)
			require.Len(t, again, 1)
			defer again[0].Free()
			require.IsType(t, stmts[0], again[0])
			if c, ok := stmts[0].(*tree.CreateMaterializedView); ok {
				require.NotNil(t, c.AsSource)
				if c.IfNotExists {
					require.Equal(t, int64(120), c.IntervalSeconds)
				}
			}
		})
	}
	for _, sql := range []string{
		"create materialized view mv as select * from t",
		"create materialized view mv refresh manual as select db.abs(v) from t",
		"create materialized view mv refresh every interval 0 second as select * from t",
		"alter materialized view mv refresh every interval 366 day",
		"create materialized view mv refresh every interval 9223372036854775808 hour as select * from t",
	} {
		t.Run(sql, func(t *testing.T) {
			stmts, err := Parse(context.Background(), sql, 1)
			for _, s := range stmts {
				s.Free()
			}
			require.Error(t, err)
		})
	}
}
