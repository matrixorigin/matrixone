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
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type recordingMVRowIDReader struct {
	snapshots [][]types.TS
}

type controlledMVRowIDReader struct {
	rows [][]any
	err  error
}

func (r *controlledMVRowIDReader) ReadRowsByRowID(
	context.Context, []types.Rowid, types.TS, []string, *mpool.MPool,
) ([][]any, error) {
	return r.rows, r.err
}

func (r *recordingMVRowIDReader) ReadRowsByRowID(
	_ context.Context,
	rowids []types.Rowid,
	snapshot types.TS,
	_ []string,
	_ *mpool.MPool,
) ([][]any, error) {
	call := make([]types.TS, len(rowids))
	rows := make([][]any, len(rowids))
	for i := range rowids {
		call[i] = snapshot
		rows[i] = []any{int64(rowids[i].GetRowOffset())}
	}
	r.snapshots = append(r.snapshots, call)
	return rows, nil
}

var _ engine.RowIDReader = (*recordingMVRowIDReader)(nil)

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

func TestReadMaterializedViewDeletedRowsUsesPreCommitSnapshot(t *testing.T) {
	var block types.Blockid
	commitA := types.BuildTS(20, 3)
	commitB := types.BuildTS(30, 1)
	deletes := []materializedViewChangeRow{
		{RowID: types.NewRowid(&block, 1), CommitTS: commitA},
		{RowID: types.NewRowid(&block, 2), CommitTS: commitB},
		{RowID: types.NewRowid(&block, 3), CommitTS: commitA},
	}
	reader := &recordingMVRowIDReader{}
	rows, err := readMaterializedViewDeletedRows(t.Context(), reader, deletes, types.BuildTS(10, 0), []string{"id"})
	require.NoError(t, err)
	require.Equal(t, [][]any{{int64(1)}, {int64(2)}, {int64(3)}}, rows)
	require.Equal(t, [][]types.TS{{commitA.Prev(), commitA.Prev()}, {commitB.Prev()}}, reader.snapshots)
}

func TestReadMaterializedViewDeletedRowsFailureBoundaries(t *testing.T) {
	var block types.Blockid
	deletes := []materializedViewChangeRow{{RowID: types.NewRowid(&block, 1)}}
	sourceErr := errors.New("row lookup failed")

	_, err := readMaterializedViewDeletedRows(t.Context(), &controlledMVRowIDReader{err: sourceErr}, deletes, types.BuildTS(10, 0), []string{"id"})
	require.ErrorIs(t, err, sourceErr)
	_, err = readMaterializedViewDeletedRows(t.Context(), &controlledMVRowIDReader{}, deletes, types.BuildTS(10, 0), []string{"id"})
	require.ErrorContains(t, err, "returned 0 rows for 1 deletes")
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

func TestMaterializedViewDeltaRequiresInternalSQLExecutor(t *testing.T) {
	service := "materialized-view-delta-missing-executor"
	moruntime.SetupServiceBasedRuntime(service, moruntime.NewRuntime(metadata.ServiceType_CN, service, zap.NewNop()))
	require.PanicsWithValue(t, "missing internal SQL executor", func() {
		_, _ = execMaterializedViewDeltaSQL(t.Context(), "select 1", service, nil)
	})
}

func TestDecodeIncrementalDescriptionVersionAndDistinctState(t *testing.T) {
	base := incrementalDescription{
		Version: 2, SourceAlias: "e", SourceColumns: []string{"service", "trace_id"},
		Groups: []incrementalGroup{{Expression: "e.service", OutputColumn: "service"}},
		Aggregates: []incrementalAggregate{{
			Kind: "count_distinct", InputExpression: "e.trace_id", OutputColumn: "traces", StateIndex: 1,
		}},
		RowCountColumn: "__rows", StateColumns: []string{"__rows"}, StateTable: "__state",
	}
	encode := func(desc incrementalDescription) string {
		b, err := json.Marshal(desc)
		require.NoError(t, err)
		return base64.StdEncoding.EncodeToString(b)
	}
	_, err := decodeIncrementalDescription(encode(base))
	require.NoError(t, err)

	missingState := base
	missingState.StateTable = ""
	_, err = decodeIncrementalDescription(encode(missingState))
	require.ErrorContains(t, err, "versioned auxiliary state")

	future := base
	future.Version = 4
	_, err = decodeIncrementalDescription(encode(future))
	require.ErrorContains(t, err, "unsupported materialized view incremental specification version")
}

func TestDecodeUnionAllIncrementalDescription(t *testing.T) {
	branch := func(id int, table string) *incrementalDescription {
		return &incrementalDescription{
			Version: 3, Strategy: "direct-delta", BranchID: id,
			SourceDatabase: "obs", SourceTable: table, SourceAlias: "e",
			SourceColumns:  []string{"service"},
			Groups:         []incrementalGroup{{Expression: "e.service", OutputColumn: "service"}},
			Aggregates:     []incrementalAggregate{{Kind: "count_star", OutputColumn: "requests"}},
			GroupKeyColumn: "__key", RowCountColumn: "__rows", StateColumns: []string{"__rows", "__key"},
		}
	}
	desc := incrementalDescription{
		Version: 3, Strategy: "union-all", SourceAlias: "__union__",
		GroupKeyColumn: "__key", RowCountColumn: "__rows", StateColumns: []string{"__rows", "__key"},
		Branches: []incrementalBranch{{Description: branch(1, "events")}, {Description: branch(2, "archive")}},
	}
	encoded := encodeMaterializedViewIncrementalDescription(t, desc)
	decoded, err := decodeIncrementalDescription(encoded)
	require.NoError(t, err)
	require.Len(t, materializedViewLeafDescriptions(decoded), 2)

	desc.Branches[1].Description.BranchID = 1
	_, err = decodeIncrementalDescription(encodeMaterializedViewIncrementalDescription(t, desc))
	require.ErrorContains(t, err, "duplicate materialized view UNION ALL branch identity")
	desc.Branches[1].Description.BranchID = 2
	desc.Branches[1].Description.GroupKeyColumn = "__other"
	_, err = decodeIncrementalDescription(encodeMaterializedViewIncrementalDescription(t, desc))
	require.ErrorContains(t, err, "invalid materialized view UNION ALL branch specification")
}

func TestDecodeIncrementalDescriptionRejectsIncompleteOperators(t *testing.T) {
	base := incrementalDescription{
		Version: 2, SourceAlias: "e", SourceColumns: []string{"service", "value"},
		Groups:         []incrementalGroup{{Expression: "e.service", OutputColumn: "service"}},
		RowCountColumn: "__rows", StateColumns: []string{"__rows"},
	}
	encode := func(desc incrementalDescription) string {
		b, err := json.Marshal(desc)
		require.NoError(t, err)
		return base64.StdEncoding.EncodeToString(b)
	}
	tests := []struct {
		name    string
		mutate  func(*incrementalDescription)
		wantErr string
	}{
		{name: "invalid json", mutate: nil, wantErr: "invalid materialized view incremental specification"},
		{name: "incomplete description", mutate: func(d *incrementalDescription) { d.SourceAlias = "" }, wantErr: "incomplete materialized view incremental specification"},
		{name: "incomplete group", mutate: func(d *incrementalDescription) { d.Groups[0].Expression = "" }, wantErr: "invalid materialized view incremental group"},
		{name: "count column input", mutate: func(d *incrementalDescription) { d.Aggregates = []incrementalAggregate{{Kind: "count_column"}} }, wantErr: "incremental COUNT requires an input"},
		{name: "sum state", mutate: func(d *incrementalDescription) {
			d.Aggregates = []incrementalAggregate{{Kind: "sum", InputExpression: "e.value"}}
		}, wantErr: "incremental SUM requires input and state"},
		{name: "sum group key state", mutate: func(d *incrementalDescription) {
			d.GroupKeyColumn = "__key"
			d.Aggregates = []incrementalAggregate{{Kind: "sum", InputExpression: "e.value", StateCountColumn: "__count"}}
		}, wantErr: "requires sum state"},
		{name: "avg state", mutate: func(d *incrementalDescription) {
			d.Aggregates = []incrementalAggregate{{Kind: "avg", InputExpression: "e.value"}}
		}, wantErr: "incremental AVG requires input and state"},
		{name: "min input", mutate: func(d *incrementalDescription) { d.Aggregates = []incrementalAggregate{{Kind: "min"}} }, wantErr: "incremental MIN requires an input"},
		{name: "max input", mutate: func(d *incrementalDescription) { d.Aggregates = []incrementalAggregate{{Kind: "max"}} }, wantErr: "incremental MAX requires an input"},
		{name: "distinct state", mutate: func(d *incrementalDescription) {
			d.Aggregates = []incrementalAggregate{{Kind: "count_distinct", InputExpression: "e.value", StateIndex: 1}}
		}, wantErr: "requires versioned auxiliary state"},
		{name: "unknown aggregate", mutate: func(d *incrementalDescription) { d.Aggregates = []incrementalAggregate{{Kind: "median"}} }, wantErr: "is not supported"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			encoded := base64.StdEncoding.EncodeToString([]byte("{"))
			if tc.mutate != nil {
				desc := base
				desc.Groups = append([]incrementalGroup(nil), base.Groups...)
				tc.mutate(&desc)
				encoded = encode(desc)
			}
			_, err := decodeIncrementalDescription(encoded)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestMaterializedViewDeltaDescriptionPredicates(t *testing.T) {
	require.False(t, materializedViewDeltaCanUpsert(nil))
	require.False(t, materializedViewHasDistinctState(nil))
	require.False(t, materializedViewNeedsAffectedGroups(nil))
	require.False(t, materializedViewHasAuxiliaryState(nil))

	desc := &incrementalDescription{StateTable: "state", Groups: []incrementalGroup{{OutputColumn: "g"}}}
	require.False(t, materializedViewDeltaCanUpsert(desc))
	require.False(t, materializedViewHasDistinctState(desc))
	require.False(t, materializedViewNeedsAffectedGroups(desc))

	desc.GroupKeyColumn = "__key"
	desc.Aggregates = []incrementalAggregate{{Kind: "min"}}
	require.True(t, materializedViewDeltaCanUpsert(desc))
	require.True(t, materializedViewNeedsAffectedGroups(desc))
	require.True(t, materializedViewHasAuxiliaryState(desc))

	desc.Aggregates = []incrementalAggregate{{Kind: "count_distinct"}}
	require.True(t, materializedViewHasDistinctState(desc))
	require.True(t, materializedViewHasAuxiliaryState(desc))

	union := &incrementalDescription{
		Strategy: "union-all", GroupKeyColumn: "__key",
		Branches: []incrementalBranch{{Description: desc}, {Description: desc}},
	}
	require.True(t, materializedViewDeltaCanUpsert(union))
}

func TestMaterializedViewDeltaSourceCTEValidation(t *testing.T) {
	desc := &incrementalDescription{
		SourceAlias: "e", SourceColumns: []string{"service"},
		Groups:         []incrementalGroup{{Expression: "e.service", OutputColumn: "service"}},
		Aggregates:     []incrementalAggregate{{Kind: "count_star", OutputColumn: "rows"}},
		RowCountColumn: "__rows",
	}
	varcharType := types.T_varchar.ToType()
	_, err := materializedViewDeltaSourceCTE(t.Context(), desc, []*types.Type{&varcharType}, []materializedViewSignedRow{{
		values: map[string]any{}, sign: 1,
	}})
	require.ErrorContains(t, err, "missing column")

	for _, typ := range []*types.Type{
		ptrType(types.T_time.ToTypeWithScale(3)),
		ptrType(types.T_datetime.ToTypeWithScale(4)),
		ptrType(types.T_timestamp.ToTypeWithScale(5)),
		ptrType(types.T_decimal64.ToType()),
	} {
		require.NotEmpty(t, materializedViewDeltaSQLType(typ))
	}
}

func ptrType(typ types.Type) *types.Type { return &typ }

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

func TestMaterializedViewAdvancedDeltaSQLIsReparseable(t *testing.T) {
	desc := &incrementalDescription{
		Version: 2, Strategy: "hybrid-state", SourceAlias: "e",
		SourceColumns: []string{"duration", "service", "trace_id"},
		Groups:        []incrementalGroup{{Expression: "e.service", OutputColumn: "service", NotNullable: true}},
		Aggregates: []incrementalAggregate{
			{Kind: "min", InputExpression: "e.duration", OutputColumn: "min_duration"},
			{Kind: "max", InputExpression: "e.duration", OutputColumn: "max_duration"},
			{Kind: "count_distinct", InputExpression: "e.trace_id", OutputColumn: "traces", StateIndex: 3},
		},
		GroupKeyColumn: "__group_key", RowCountColumn: "__row_count",
		StateColumns: []string{"__row_count", "__group_key"}, StateTable: "__state",
	}
	intType, varcharType := types.T_int64.ToType(), types.T_varchar.ToType()
	rows := []materializedViewSignedRow{
		{values: map[string]any{"duration": int64(10), "service": []byte("api"), "trace_id": []byte("t1")}, sign: 1},
		{values: map[string]any{"duration": int64(5), "service": []byte("api"), "trace_id": []byte("t1")}, sign: -1},
	}
	typesByColumn := []*types.Type{&intType, &varcharType, &varcharType}
	cte, err := materializedViewDeltaCTE(t.Context(), desc, typesByColumn, rows)
	require.NoError(t, err)
	require.Contains(t, cte, "min(CASE WHEN __mo_sign > 0")
	require.Contains(t, cte, "max(CASE WHEN __mo_sign > 0")
	columns, values := materializedViewDeltaInsertProjection(desc, "d")
	upsert := fmt.Sprintf("%s INSERT INTO %s (%s) SELECT %s FROM delta AS d ON DUPLICATE KEY UPDATE %s",
		cte, sqlquote.QualifiedIdent("obs", "mv"), strings.Join(columns, ","), strings.Join(values, ","), strings.Join(materializedViewDeltaUpsertSets(desc), ","))

	distinctCTE, err := materializedViewDistinctDeltaCTE(t.Context(), desc, desc.Aggregates[2], typesByColumn, rows)
	require.NoError(t, err)
	distinctStatements := materializedViewDistinctDeltaStatements(
		desc, desc.Aggregates[2], distinctCTE,
		sqlquote.QualifiedIdent("obs", "__state"), sqlquote.QualifiedIdent("obs", "mv"))
	require.NotContains(t, strings.Join(distinctStatements, "\n"), "value_sum_delta")
	statements := append([]string{upsert}, distinctStatements...)
	for _, kind := range []string{"sum_distinct", "avg_distinct"} {
		distinctDesc := *desc
		distinctDesc.Aggregates = []incrementalAggregate{{
			Kind: kind, InputExpression: "e.duration", OutputColumn: "distinct_value",
			StateIndex: 1, StateSumColumn: "__distinct_sum", StateCountColumn: "__distinct_count",
		}}
		distinctDesc.StateColumns = []string{"__row_count", "__group_key", "__distinct_sum", "__distinct_count"}
		distinctCTE, err := materializedViewDistinctDeltaCTE(t.Context(), &distinctDesc, distinctDesc.Aggregates[0], typesByColumn, rows)
		require.NoError(t, err)
		distinctStatements := materializedViewDistinctDeltaStatements(
			&distinctDesc, distinctDesc.Aggregates[0], distinctCTE,
			sqlquote.QualifiedIdent("obs", "__state"), sqlquote.QualifiedIdent("obs", "mv"))
		require.Contains(t, strings.Join(distinctStatements, "\n"), "value_sum_delta")
		columns, values := materializedViewDeltaInsertProjection(&distinctDesc, "d")
		require.Equal(t, len(columns), len(values))
		upsert := fmt.Sprintf("%s INSERT INTO %s (%s) SELECT %s FROM delta AS d ON DUPLICATE KEY UPDATE %s",
			cte, sqlquote.QualifiedIdent("obs", "mv"), strings.Join(columns, ","), strings.Join(values, ","), strings.Join(materializedViewDeltaUpsertSets(&distinctDesc), ","))
		for _, sql := range append([]string{upsert}, distinctStatements...) {
			stmt, parseErr := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
			require.NoError(t, parseErr, sql)
			stmt.Free()
		}
	}
	statements = append(statements,
		"CREATE TABLE IF NOT EXISTS `obs`.`__state` (aggregate_index INT NOT NULL, group_key VARBINARY(65535) NOT NULL, value_key VARBINARY(65535) NOT NULL, ref_count BIGINT NOT NULL, PRIMARY KEY (aggregate_index, group_key, value_key)) COMMENT = 'matrixone materialized view state'",
		"DELETE t FROM `obs`.`mv` AS t JOIN `obs`.`__state` AS s ON t.`__group_key` = s.group_key WHERE s.aggregate_index = 0",
	)
	for _, sql := range statements {
		stmt, parseErr := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, parseErr, sql)
		stmt.Free()
	}
}

func TestMaterializedViewDeltaExecutionPaths(t *testing.T) {
	service := "materialized-view-delta-execution-test"
	rt := moruntime.NewRuntime(metadata.ServiceType_CN, service, zap.NewNop())
	moruntime.SetupServiceBasedRuntime(service, rt)
	var sqls []string
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		sqls = append(sqls, sql)
		return executor.Result{}, nil
	}))

	desc := &incrementalDescription{
		Version: 2, Strategy: "hybrid-state", SourceAlias: "e",
		SourceColumns: []string{"duration", "service", "trace_id"},
		Filter:        "e.duration >= 0",
		Groups:        []incrementalGroup{{Expression: "e.service", OutputColumn: "service", NotNullable: true}},
		Aggregates: []incrementalAggregate{
			{Kind: "count_star", OutputColumn: "requests"},
			{Kind: "min", InputExpression: "e.duration", OutputColumn: "min_duration"},
			{Kind: "max", InputExpression: "e.duration", OutputColumn: "max_duration"},
			{Kind: "count_distinct", InputExpression: "e.trace_id", OutputColumn: "traces", StateIndex: 4},
		},
		GroupKeyColumn: "__group_key", RowCountColumn: "__row_count",
		StateColumns: []string{"__row_count", "__group_key"}, StateTable: "__state",
	}
	info := &ConsumerInfo{
		DBName: "db", TableName: "mv",
		Columns:   []string{"service", "requests", "min_duration", "max_duration", "traces"},
		SourceSQL: "events",
		RefreshSQL: "SELECT e.service AS service, count(*) AS requests, min(e.duration) AS min_duration, " +
			"max(e.duration) AS max_duration, count(distinct e.trace_id) AS traces, " +
			"count(*) AS __row_count, serial_full(e.service) AS __group_key FROM events AS e " +
			"WHERE e.duration >= 0 GROUP BY e.service",
		SrcTables: []TableInfo{{DBName: "db", TableName: "events"}},
	}
	intType, varcharType := types.T_int64.ToType(), types.T_varchar.ToType()
	sourceTypes := []*types.Type{&intType, &varcharType, &varcharType}
	rows := []materializedViewSignedRow{
		{values: map[string]any{"duration": int64(10), "service": []byte("api"), "trace_id": []byte("t1")}, sign: -1},
		{values: map[string]any{"duration": int64(20), "service": []byte("api"), "trace_id": []byte("t2")}, sign: 1},
	}

	require.NoError(t, applyMaterializedViewDeltaRows(t.Context(), service, nil, info, desc, sourceTypes, rows))
	require.NoError(t, ensureMaterializedViewStateTable(t.Context(), service, nil, info, desc))
	require.NoError(t, resetMaterializedViewAffectedGroups(t.Context(), service, nil, info, desc))
	require.NoError(t, recordMaterializedViewAffectedGroups(t.Context(), service, nil, info, desc, sourceTypes, rows))
	require.NoError(t, recomputeMaterializedViewAffectedGroups(t.Context(), service, nil, info, desc, types.BuildTS(100, 7)))
	require.NoError(t, applyMaterializedViewDistinctDeltas(t.Context(), service, nil, info, desc, sourceTypes, rows))
	require.NoError(t, rebuildMaterializedViewDistinctState(t.Context(), service, nil, info, desc, types.BuildTS(100, 7)))

	legacy := *desc
	legacy.GroupKeyColumn = ""
	legacy.StateTable = ""
	legacy.Aggregates = []incrementalAggregate{{Kind: "count_star", OutputColumn: "requests"}}
	legacy.StateColumns = []string{"__row_count"}
	require.NoError(t, applyMaterializedViewDeltaRows(t.Context(), service, nil, info, &legacy, sourceTypes, rows))
	require.NoError(t, applyMaterializedViewDeltaRows(t.Context(), service, nil, info, &legacy, sourceTypes, rows[1:]))
	require.NoError(t, applyMaterializedViewDeltaRows(t.Context(), service, nil, info, &legacy, sourceTypes, nil))
	require.NoError(t, ensureMaterializedViewStateTable(t.Context(), service, nil, info, &legacy))
	require.NoError(t, resetMaterializedViewAffectedGroups(t.Context(), service, nil, info, &legacy))
	require.NoError(t, recordMaterializedViewAffectedGroups(t.Context(), service, nil, info, &legacy, sourceTypes, rows))
	require.NoError(t, recomputeMaterializedViewAffectedGroups(t.Context(), service, nil, info, &legacy, types.BuildTS(100, 7)))
	require.NoError(t, applyMaterializedViewDistinctDeltas(t.Context(), service, nil, info, &legacy, sourceTypes, rows))
	require.NoError(t, rebuildMaterializedViewDistinctState(t.Context(), service, nil, info, &legacy, types.BuildTS(100, 7)))
	require.GreaterOrEqual(t, len(sqls), 20)
}

func TestMaterializedViewDeltaExecutionFailureBoundaries(t *testing.T) {
	desc := &incrementalDescription{
		Version: 2, SourceAlias: "e", SourceColumns: []string{"service"},
		Groups:         []incrementalGroup{{Expression: "e.service", OutputColumn: "service", NotNullable: true}},
		Aggregates:     []incrementalAggregate{{Kind: "count_star", OutputColumn: "requests"}},
		GroupKeyColumn: "__group_key", RowCountColumn: "__row_count",
		StateColumns: []string{"__row_count", "__group_key"},
	}
	legacy := *desc
	legacy.GroupKeyColumn = ""
	legacy.StateColumns = []string{"__row_count"}
	info := &ConsumerInfo{DBName: "db", TableName: "mv"}
	varcharType := types.T_varchar.ToType()
	rows := []materializedViewSignedRow{
		{values: map[string]any{"service": []byte("api")}, sign: -1},
		{values: map[string]any{"service": []byte("api")}, sign: 1},
	}
	sourceErr := errors.New("delta statement failed")

	for _, tc := range []struct {
		name   string
		desc   *incrementalDescription
		failAt int
	}{
		{name: "legacy update", desc: &legacy, failAt: 1},
		{name: "legacy insert", desc: &legacy, failAt: 2},
		{name: "legacy delete cleanup", desc: &legacy, failAt: 3},
		{name: "upsert negative delta", desc: desc, failAt: 1},
		{name: "upsert delete cleanup", desc: desc, failAt: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := "materialized-view-delta-failure-" + strings.ReplaceAll(tc.name, " ", "-")
			rt := moruntime.NewRuntime(metadata.ServiceType_CN, service, zap.NewNop())
			moruntime.SetupServiceBasedRuntime(service, rt)
			calls := 0
			rt.SetGlobalVariables(moruntime.InternalSQLExecutor, executor.NewMemExecutor(func(string) (executor.Result, error) {
				calls++
				if calls == tc.failAt {
					return executor.Result{}, sourceErr
				}
				return executor.Result{}, nil
			}))
			err := applyMaterializedViewDeltaRows(t.Context(), service, nil, info, tc.desc, []*types.Type{&varcharType}, rows)
			require.ErrorIs(t, err, sourceErr)
			require.Equal(t, tc.failAt, calls)
		})
	}
}
