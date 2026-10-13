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

package tree_test

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestFmtCtxCapturesDateFormatLiteralsThroughScalarSubquery(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"select concat(date_format(col2, '%M'), (select time_format(col2, '%H') from time01 limit 1)) from time01", 1)
	require.NoError(t, err)
	defer stmt.Free()

	selectStmt, ok := stmt.(*tree.Select)
	require.True(t, ok)
	selectClause, ok := selectStmt.Select.(*tree.SelectClause)
	require.True(t, ok)
	expr := selectClause.Exprs[0].Expr

	var positions []tree.StringLiteralPosition
	ctx := tree.NewFmtCtx(
		dialect.MYSQL,
		tree.WithSingleQuoteString(),
		tree.WithDateTimeFormatDetection(),
		tree.WithStringLiteralPositions(&positions),
	)
	expr.Format(ctx)

	require.True(t, ctx.HasDateTimeFormatFunction())
	require.Len(t, positions, 2)
	for _, position := range positions {
		require.GreaterOrEqual(t, position.Start, 0)
		require.Greater(t, position.End, position.Start)
		require.Equal(t, byte('\''), ctx.String()[position.Start])
		require.Equal(t, byte('\''), ctx.String()[position.End-1])
	}
}

func TestFmtCtxCapturesBinaryDateFormatLiteral(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"select date_format(col2, _binary '%M') from time01", 1)
	require.NoError(t, err)
	defer stmt.Free()

	selectStmt, ok := stmt.(*tree.Select)
	require.True(t, ok)
	selectClause, ok := selectStmt.Select.(*tree.SelectClause)
	require.True(t, ok)
	expr := selectClause.Exprs[0].Expr

	var positions []tree.StringLiteralPosition
	ctx := tree.NewFmtCtx(
		dialect.MYSQL,
		tree.WithSingleQuoteString(),
		tree.WithDateTimeFormatDetection(),
		tree.WithStringLiteralPositions(&positions),
	)
	expr.Format(ctx)

	require.True(t, ctx.HasDateTimeFormatFunction())
	require.Equal(t, "date_format(col2, _binary '%M')", ctx.String())
	require.Len(t, positions, 1)
	position := positions[0]
	require.Equal(t, "_binary '%M'", ctx.String()[position.Start:position.End])
}
