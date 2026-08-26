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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewSourceColumnTypesPreservesTemporalPrecision(t *testing.T) {
	tableDef := &planpb.TableDef{Cols: []*planpb.ColDef{
		{Name: "event_time", Typ: planpb.Type{Id: int32(types.T_datetime), Scale: 0}},
		{Name: "duration", Typ: planpb.Type{Id: int32(types.T_int64), Scale: 0}},
	}}

	got, err := materializedViewSourceColumnTypes(tableDef, []string{"event_time", "duration"})
	require.NoError(t, err)
	require.Equal(t, int32(6), got[0].Scale)
	require.Equal(t, int32(0), got[1].Scale)
}

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
			{Kind: "sum", InputExpression: "case when e.status >= 500 then 1 else 0 end", OutputColumn: "errors", StateSumColumn: "__sum_sum", StateCountColumn: "__sum_count"},
			{Kind: "avg", InputExpression: "e.duration", OutputColumn: "avg_duration", StateSumColumn: "__avg_sum", StateCountColumn: "__avg_count"},
		},
		RowCountColumn: "__row_count",
	}
	floatType, datetimeType := types.T_float64.ToType(), types.T_datetime.ToTypeWithScale(6)
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
	require.Contains(t, cte, "AS DATETIME(6)")
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
	for i := range desc.Groups {
		desc.Groups[i].NotNullable = true
	}
	desc.GroupKeyColumn = "__group_key"
	columns, values = materializedViewDeltaInsertProjection(desc, "d")
	upsert := fmt.Sprintf("%s INSERT INTO %s (%s) SELECT %s FROM delta AS d ON DUPLICATE KEY UPDATE %s",
		cte, target, strings.Join(columns, ","), strings.Join(values, ","), strings.Join(materializedViewDeltaUpsertSets(desc), ","))
	stmt, parseErr := parsers.ParseOne(t.Context(), dialect.MYSQL, upsert, 1)
	require.NoError(t, parseErr, upsert)
	stmt.Free()
	require.Contains(t, upsert, "ON DUPLICATE KEY UPDATE")
	require.Contains(t, upsert, "VALUES(`__sum_sum`)")
	require.Contains(t, upsert, "serial_full(d.__mo_g_0,d.__mo_g_1)")

	legacy := *desc
	legacy.GroupKeyColumn = ""
	legacy.StateColumns = []string{"__row_count", "__sum_count", "__avg_sum", "__avg_count"}
	legacy.Aggregates = append([]incrementalAggregate(nil), desc.Aggregates...)
	legacy.Aggregates[1].StateSumColumn = ""
	b, err := json.Marshal(&legacy)
	require.NoError(t, err)
	decoded, err := decodeIncrementalDescription(base64.StdEncoding.EncodeToString(b))
	require.NoError(t, err)
	legacySets := strings.Join(materializedViewDeltaUpdateSets(decoded, "t", "d"), ",")
	require.Contains(t, legacySets, "coalesce(t.`errors`,0) + coalesce(d.__mo_a_1_sum,0)")
	legacyColumns, _ := materializedViewDeltaInsertProjection(decoded, "d")
	require.NotContains(t, legacyColumns, "`__sum_sum`")
}

func TestMaterializedViewDeltaExecOptionsAdvanceStatementBoundary(t *testing.T) {
	opts := materializedViewDeltaExecOptions(nil)
	require.False(t, opts.DisableIncrStatement(),
		"successive delta DML must finalize preceding workspace writes as separate statements")
}

func TestMaterializedViewDeltaJoinUsesEqualityForNonNullableGroups(t *testing.T) {
	desc := &incrementalDescription{Groups: []incrementalGroup{
		{OutputColumn: "service", NotNullable: true},
		{OutputColumn: "region"},
	}}
	join := materializedViewDeltaJoin(desc, "t", "d")
	require.Contains(t, join, "t.`service` = d.__mo_g_0")
	require.NotContains(t, join, "t.`service` IS NULL")
	require.Contains(t, join, "t.`region` <=> d.__mo_g_1")
}

func TestMaterializedViewDeltaCTEReportsOversizedRows(t *testing.T) {
	desc := &incrementalDescription{
		SourceAlias: "e", SourceColumns: []string{"payload"},
		Groups:         []incrementalGroup{{Expression: "e.payload", OutputColumn: "payload"}},
		Aggregates:     []incrementalAggregate{{Kind: "count_star", OutputColumn: "rows"}},
		RowCountColumn: "__row_count",
	}
	varcharType := types.T_varchar.ToType()
	_, err := materializedViewDeltaCTE(t.Context(), desc, []*types.Type{&varcharType}, []materializedViewSignedRow{{
		values: map[string]any{"payload": []byte(strings.Repeat("x", materializedViewDeltaMaxSQL))}, sign: 1,
	}})
	require.Error(t, err)
	require.True(t, errors.Is(err, errMaterializedViewDeltaSQLTooLarge))
}
