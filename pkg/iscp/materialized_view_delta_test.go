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

package iscp

import (
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewDeltaSQLIsBatchedAndReparseable(t *testing.T) {
	desc := &incrementalDescription{
		SourceAlias: "e", SourceColumns: []string{"duration", "event_ts", "region", "service", "status"},
		Filter: "e.region = 'us'",
		Groups: []incrementalGroup{
			{Expression: "e.service", OutputColumn: "service"},
			{Expression: "date_trunc('minute', e.event_ts)", OutputColumn: "minute"},
		},
		Aggregates: []incrementalAggregate{
			{Kind: "count_star", OutputColumn: "requests"},
			{Kind: "sum", InputExpression: "case when e.status >= 500 then 1 else 0 end", OutputColumn: "errors", StateCountColumn: "__sum_count"},
			{Kind: "avg", InputExpression: "e.duration", OutputColumn: "avg_duration", StateSumColumn: "__avg_sum", StateCountColumn: "__avg_count"},
		},
		RowCountColumn: "__row_count",
	}
	floatType, datetimeType := types.T_float64.ToType(), types.T_datetime.ToType()
	regionType, serviceType, statusType := types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_int64.ToType()
	columnTypes := []*types.Type{&floatType, &datetimeType, &regionType, &serviceType, &statusType}
	rows := []materializedViewSignedRow{
		{values: map[string]any{"duration": 10.0, "event_ts": types.DatetimeFromClock(2026, 8, 20, 10, 1, 2, 0), "region": []byte("us"), "service": []byte("api"), "status": int64(503)}, sign: 1},
		{values: map[string]any{"duration": 5.0, "event_ts": types.DatetimeFromClock(2026, 8, 20, 10, 1, 30, 0), "region": []byte("us"), "service": []byte("api"), "status": int64(200)}, sign: -1},
	}
	cte, err := materializedViewDeltaCTE(t.Context(), desc, columnTypes, rows)
	require.NoError(t, err)
	require.Equal(t, 2, strings.Count(cte, "ROW("))
	require.Contains(t, cte, "CAST(10 AS DOUBLE)")
	require.Contains(t, cte, "date_trunc('minute', e.event_ts)")
	require.Contains(t, cte, "case when e.status >= 500 then 1 else 0 end")
	require.NotContains(t, cte, "select count(*) from")

	target := sqlquote.QualifiedIdent("obs", "mv")
	join := materializedViewDeltaJoin(desc, "t", "d")
	update := fmt.Sprintf("%s UPDATE %s AS t JOIN delta AS d ON %s SET %s", cte, target, join, strings.Join(materializedViewDeltaUpdateSets(desc, "t", "d"), ","))
	require.Contains(t, update, "coalesce(t.`__avg_sum`,0) + coalesce(d.__mo_a_2_sum,0)")
	columns, values := materializedViewDeltaInsertProjection(desc, "d")
	insert := fmt.Sprintf("%s INSERT INTO %s (%s) SELECT %s FROM delta AS d LEFT JOIN %s AS t ON %s WHERE t.%s IS NULL AND d.__mo_row_delta > 0",
		cte, target, strings.Join(columns, ","), strings.Join(values, ","), target, join, sqlquote.Ident(catalog.FakePrimaryKeyColName))
	for _, sql := range []string{update, insert} {
		stmt, parseErr := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, parseErr, sql)
		stmt.Free()
	}
}

func TestMaterializedViewDeltaExecOptionsAdvanceStatementBoundary(t *testing.T) {
	opts := materializedViewDeltaExecOptions(nil)
	require.False(t, opts.DisableIncrStatement(),
		"successive delta DML must finalize preceding workspace writes as separate statements")
}
