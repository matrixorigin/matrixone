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

package mongodb

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestQuoteSQLStringRoundTrip(t *testing.T) {
	for _, value := range []string{
		"plain",
		`path\segment`,
		`trailing\`,
		"single'quote",
		`raw\'); select 1; -- `,
		`backslash\nsequence`,
	} {
		t.Run(value, func(t *testing.T) {
			stmt, err := mysql.ParseOne(t.Context(), "select "+quoteSQLString(value), 1)
			require.NoError(t, err)
			defer stmt.Free()

			selectStmt, ok := stmt.(*tree.Select)
			require.True(t, ok)
			clause, ok := selectStmt.Select.(*tree.SelectClause)
			require.True(t, ok)
			require.Len(t, clause.Exprs, 1)
			literal, ok := clause.Exprs[0].Expr.(*tree.NumVal)
			require.True(t, ok)
			require.Equal(t, value, literal.String())
		})
	}
}

func TestCreateSQLEnvelopeRoundTripAndRejectsParallelMVP(t *testing.T) {
	ctx := context.Background()
	mapping := TableMapping{
		Connection: "telemetry production",
		Database:   "telemetry",
		Collection: "measurements/hour",
		Columns:    []ColumnMapping{{Name: "device_id", Path: "meta.device_id", TypeID: int32(types.T_varchar)}},
	}
	raw := BuildCreateSQLEnvelope(mapping)
	require.NotContains(t, raw, "password")
	env, found, err := ParseCreateSQLEnvelope(ctx, raw)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 2, env.Version)
	require.Equal(t, CreateSQLKindMongoDB, env.Kind)
	require.Equal(t, mapping.Connection, env.Connection)
	require.Equal(t, int32(1), env.MaxParallelism)
	require.Equal(t, mapping.Columns, env.Columns)

	_, found, err = ParseCreateSQLEnvelope(ctx, "create external table x (a int)")
	require.NoError(t, err)
	require.False(t, found)

	for _, injected := range []string{
		`{"filepath":"` + raw + `"}`,
		"create external table x (a int) infile " + raw,
		`{"filepath":"MO_MONGODB: version=1; connection=admin */"}`,
	} {
		_, found, err = ParseCreateSQLEnvelope(ctx, injected)
		require.NoError(t, err)
		require.False(t, found, injected)
	}

	legacy := strings.Replace(raw, "version=2; kind=mongodb_table;", "version=1;", 1)
	env, found, err = ParseCreateSQLEnvelope(ctx, legacy)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 1, env.Version)
	require.Equal(t, CreateSQLKindMongoDB, env.Kind)

	wrongKind := strings.Replace(raw, "kind=mongodb_table", "kind=generic_external", 1)
	_, found, err = ParseCreateSQLEnvelope(ctx, wrongKind)
	require.True(t, found)
	require.ErrorContains(t, err, "kind")

	bad := BuildCreateSQLEnvelope(TableMapping{
		Connection: "c", Database: "d", Collection: "x", MaxParallelism: 2,
		Columns: mapping.Columns,
	})
	_, found, err = ParseCreateSQLEnvelope(ctx, bad)
	require.True(t, found)
	require.Error(t, err)
}

func TestMappingSnapshotMatchesPlan(t *testing.T) {
	mapping := TableMapping{
		TableID: 10, MappingID: 11, ConnectionID: 22, Version: 3,
		Database: "telemetry", Collection: "raw", MaxParallelism: 1,
		Columns: []ColumnMapping{{Name: "value", Path: "reading.value", TypeID: int32(types.T_float64), Conversion: ConversionStrict}},
	}
	scan := &planpb.MongoScan{
		TableId: 10, MappingId: 11, MappingVersion: 3, ConnectionId: 22,
		Database: "telemetry", Collection: "raw", MaxParallelism: 1,
		Columns: ColumnsToPlan(mapping.Columns),
	}
	require.True(t, MappingDefinitionMatchesPlan(mapping, scan))
	require.True(t, MappingSnapshotMatchesPlan(mapping, scan))
	mapping.Columns = append(mapping.Columns, ColumnMapping{Name: "quality", Path: "quality", TypeID: int32(types.T_varchar), Conversion: ConversionStrict})
	require.False(t, MappingDefinitionMatchesPlan(mapping, scan), "compile compares the full rel_createsql definition")
	require.True(t, MappingSnapshotMatchesPlan(mapping, scan), "execution accepts a verified projected subset")

	drifted := mapping
	drifted.Collection = "redirected"
	require.False(t, MappingDefinitionMatchesPlan(drifted, scan))
	require.False(t, MappingSnapshotMatchesPlan(drifted, scan))

	drifted = mapping
	drifted.Columns = drifted.Columns[:1]
	drifted.Version++
	require.True(t, MappingDefinitionMatchesPlan(drifted, scan))
	require.False(t, MappingSnapshotMatchesPlan(drifted, scan))
}

func TestParseTableMappingSpecIgnoresInternalHiddenColumns(t *testing.T) {
	ctx := context.Background()
	defs := tree.TableDefs{
		&tree.ColumnTableDef{
			Name: tree.NewUnresolvedColName("value"),
			Attributes: []tree.ColumnAttribute{
				tree.NewAttributeMongoDBPath("reading.value"),
			},
		},
	}
	param := tree.NewMongoDBTableParam(tree.MongoDBOptions{
		tree.NewMongoDBOption("connection", "source"),
		tree.NewMongoDBOption("database", "telemetry"),
		tree.NewMongoDBOption("collection", "raw"),
	})
	tableDef := &planpb.TableDef{Cols: []*planpb.ColDef{
		{Name: "value", Typ: planpb.Type{Id: int32(types.T_float64), NotNullable: true}},
		{Name: "__mo_fake_pk_col", Hidden: true, Typ: planpb.Type{Id: int32(types.T_uint64)}},
	}}
	spec, err := ParseTableMappingSpec(ctx, param, defs, tableDef)
	require.NoError(t, err)
	require.Equal(t, []ColumnMapping{{
		Name: "value", Path: "reading.value", TypeID: int32(types.T_float64), NotNullable: true, Conversion: ConversionStrict,
	}}, spec.Mapping.Columns)
}

func TestPredicateTranslationAndProjection(t *testing.T) {
	ctx := context.Background()
	p := &Predicate{Op: PredicateAnd, Children: []*Predicate{
		{Op: PredicateGreaterEqual, Path: "meta.ts", Value: int64(10)},
		{Op: PredicateIsNull, Path: "reading.value"},
	}}
	filter, err := PredicateToBSON(ctx, p)
	require.NoError(t, err)
	encoded, err := bson.MarshalExtJSON(filter, false, false)
	require.NoError(t, err)
	require.JSONEq(t, `{"$and":[{"meta.ts":{"$gte":10}},{"reading.value":null}]}`, string(encoded))

	notNull, err := PredicateToBSON(ctx, &Predicate{Op: PredicateIsNotNull, Path: "x"})
	require.NoError(t, err)
	encoded, err = bson.MarshalExtJSON(notNull, false, false)
	require.NoError(t, err)
	require.JSONEq(t, `{"$and":[{"x":{"$exists":true}},{"x":{"$ne":null}}]}`, string(encoded))

	projection := ProjectionDocument([]ColumnMapping{{Path: "a"}, {Path: "a"}, {Path: "nested.b"}})
	require.Equal(t, bson.D{{Key: "a", Value: 1}, {Key: "nested.b", Value: 1}, {Key: "_id", Value: 0}}, projection)
	require.Error(t, (&Predicate{Op: PredicateEqual, Path: "$where", Value: 1}).Validate(ctx))
}

func TestPredicateOperatorsValidationAndProjectionParents(t *testing.T) {
	ctx := t.Context()
	require.NoError(t, (*Predicate)(nil).Validate(ctx))
	empty, err := PredicateToBSON(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, empty)
	require.ErrorContains(t, (&Predicate{Op: PredicateAnd}).Validate(ctx), "requires children")
	require.Error(t, (&Predicate{Op: PredicateAnd, Children: []*Predicate{{Op: PredicateInvalid, Path: "x"}}}).Validate(ctx))
	require.ErrorContains(t, (&Predicate{Op: PredicateIn, Path: "x"}).Validate(ctx), "cannot be empty")
	require.ErrorContains(t, (&Predicate{Op: PredicateInvalid, Path: "x"}).Validate(ctx), "unsupported")

	for _, tc := range []struct {
		name string
		op   PredicateOp
	}{
		{name: "equal", op: PredicateEqual},
		{name: "not equal", op: PredicateNotEqual},
		{name: "less", op: PredicateLess},
		{name: "less equal", op: PredicateLessEqual},
		{name: "greater", op: PredicateGreater},
		{name: "greater equal", op: PredicateGreaterEqual},
		{name: "is null", op: PredicateIsNull},
		{name: "is not null", op: PredicateIsNotNull},
	} {
		t.Run(tc.name, func(t *testing.T) {
			document, err := PredicateToBSON(t.Context(), &Predicate{Op: tc.op, Path: "reading.value", Value: int64(10)})
			require.NoError(t, err)
			require.NotEmpty(t, document)
		})
	}
	in, err := PredicateToBSON(ctx, &Predicate{Op: PredicateIn, Path: "reading.value", Values: []any{int64(1), int64(2)}})
	require.NoError(t, err)
	require.NotEmpty(t, in)

	for _, path := range []string{"", ".value", "value.", "a..b", "$value", "a.*", "a[0]", "a\x00b"} {
		require.Error(t, ValidateBSONPath(ctx, path), path)
	}
	require.NoError(t, ValidateBSONPath(ctx, "metadata.reading.value"))

	projection := ProjectionDocument([]ColumnMapping{
		{Path: "payload.value"}, {Path: "payload.quality"}, {Path: "payload"}, {Path: "_id.hex"},
	})
	require.Equal(t, bson.D{{Key: "payload", Value: 1}, {Key: "_id.hex", Value: 1}}, projection)
}

func TestParseTableMappingSpecRejectsInvalidOptionsAndColumnContracts(t *testing.T) {
	ctx := t.Context()
	validOptions := func(extra ...*tree.MongoDBOption) *tree.MongoDBTableParam {
		options := tree.MongoDBOptions{
			tree.NewMongoDBOption("connection", "source"),
			tree.NewMongoDBOption("database", "telemetry"),
			tree.NewMongoDBOption("collection", "raw"),
		}
		options = append(options, extra...)
		return tree.NewMongoDBTableParam(options)
	}
	validDefs := func(attributes ...tree.ColumnAttribute) tree.TableDefs {
		return tree.TableDefs{&tree.ColumnTableDef{Name: tree.NewUnresolvedColName("value"), Attributes: attributes}}
	}
	validTable := func() *planpb.TableDef {
		return &planpb.TableDef{Cols: []*planpb.ColDef{{Name: "value", Typ: planpb.Type{Id: int32(types.T_int64)}}}}
	}

	for _, tc := range []struct {
		name  string
		param *tree.MongoDBTableParam
		defs  tree.TableDefs
		table *planpb.TableDef
	}{
		{name: "missing param", defs: validDefs(), table: validTable()},
		{name: "missing table", param: validOptions(), defs: validDefs()},
		{name: "empty option", param: tree.NewMongoDBTableParam(tree.MongoDBOptions{tree.NewMongoDBOption("", "value")}), defs: validDefs(), table: validTable()},
		{name: "duplicate option", param: validOptions(tree.NewMongoDBOption("DATABASE", "other")), defs: validDefs(), table: validTable()},
		{name: "unsupported option", param: validOptions(tree.NewMongoDBOption("password", "secret")), defs: validDefs(), table: validTable()},
		{name: "missing required", param: tree.NewMongoDBTableParam(tree.MongoDBOptions{tree.NewMongoDBOption("connection", "source")}), defs: validDefs(), table: validTable()},
		{name: "invalid namespace", param: tree.NewMongoDBTableParam(tree.MongoDBOptions{
			tree.NewMongoDBOption("connection", "source"),
			tree.NewMongoDBOption("database", "telemetry"),
			tree.NewMongoDBOption("collection", "bad\x00name"),
		}), defs: validDefs(), table: validTable()},
		{name: "schema mode", param: validOptions(tree.NewMongoDBOption("schema_mode", "infer")), defs: validDefs(), table: validTable()},
		{name: "conversion mode", param: validOptions(tree.NewMongoDBOption("conversion_mode", "lossy")), defs: validDefs(), table: validTable()},
		{name: "parallel parse", param: validOptions(tree.NewMongoDBOption("max_parallelism", "many")), defs: validDefs(), table: validTable()},
		{name: "parallel unsupported", param: validOptions(tree.NewMongoDBOption("max_parallelism", "2")), defs: validDefs(), table: validTable()},
		{name: "missing columns", param: validOptions(), table: validTable()},
		{name: "column count", param: validOptions(), defs: validDefs(), table: &planpb.TableDef{Cols: []*planpb.ColDef{{Name: "value", Typ: planpb.Type{Id: int32(types.T_int64)}}, {Name: "other", Typ: planpb.Type{Id: int32(types.T_int64)}}}}},
		{name: "column order", param: validOptions(), defs: validDefs(), table: &planpb.TableDef{Cols: []*planpb.ColDef{{Name: "other", Typ: planpb.Type{Id: int32(types.T_int64)}}}}},
		{name: "generated column", param: validOptions(), defs: validDefs(), table: &planpb.TableDef{Cols: []*planpb.ColDef{{Name: "value", Typ: planpb.Type{Id: int32(types.T_int64)}, GeneratedCol: &planpb.GeneratedCol{}}}}},
		{name: "duplicate path", param: validOptions(), defs: validDefs(tree.NewAttributeMongoDBPath("a"), tree.NewAttributeMongoDBPath("b")), table: validTable()},
		{name: "duplicate conversion", param: validOptions(), defs: validDefs(tree.NewAttributeMongoDBConvert("strict"), tree.NewAttributeMongoDBConvert("try_null")), table: validTable()},
		{name: "invalid column conversion", param: validOptions(), defs: validDefs(tree.NewAttributeMongoDBConvert("lossy")), table: validTable()},
		{name: "invalid column path", param: validOptions(), defs: validDefs(tree.NewAttributeMongoDBPath("$where")), table: validTable()},
		{name: "non-null default", param: validOptions(), defs: validDefs(tree.NewAttributeDefault(tree.NewNumVal("fallback", "fallback", false, tree.P_char))), table: validTable()},
		{name: "unsupported type", param: validOptions(), defs: validDefs(), table: &planpb.TableDef{Cols: []*planpb.ColDef{{Name: "value", Typ: planpb.Type{Id: int32(types.T_array_float32)}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTableMappingSpec(ctx, tc.param, tc.defs, tc.table)
			require.Error(t, err)
		})
	}

	spec, err := ParseTableMappingSpec(ctx, validOptions(tree.NewMongoDBOption("max_parallelism", "1")), validDefs(
		tree.NewAttributeMongoDBPath("reading.value"), tree.NewAttributeMongoDBConvert(" TRY_NULL "),
	), validTable())
	require.NoError(t, err)
	require.Equal(t, ConversionTryNull, spec.Mapping.Columns[0].Conversion)

	spec, err = ParseTableMappingSpec(ctx, validOptions(), validDefs(
		tree.NewAttributeDefault(&tree.ParenExpr{Expr: tree.NewNumVal("null", "null", false, tree.P_null)}),
	), validTable())
	require.NoError(t, err)
	require.Len(t, spec.Mapping.Columns, 1)
}

func TestParseTableMappingSpecDefaultContractFromSQL(t *testing.T) {
	for _, tc := range []struct {
		name          string
		defaultClause string
		wantError     bool
	}{
		{name: "string", defaultClause: "DEFAULT 'fallback'", wantError: true},
		{name: "number", defaultClause: "DEFAULT 42", wantError: true},
		{name: "expression", defaultClause: "DEFAULT (uuid())", wantError: true},
		{name: "null", defaultClause: "DEFAULT NULL"},
		{name: "parenthesized null", defaultClause: "DEFAULT (NULL)"},
		{name: "unspecified"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := fmt.Sprintf(`CREATE EXTERNAL TABLE mongo_default (
				v VARCHAR(64) %s MONGODB_PATH 'payload.value'
			) ENGINE=MONGODB WITH (
				'connection'='source', 'database'='telemetry', 'collection'='samples'
			)`, tc.defaultClause)
			stmt, err := mysql.ParseOne(t.Context(), sql, 1)
			require.NoError(t, err)
			defer stmt.Free()

			create, ok := stmt.(*tree.CreateTable)
			require.True(t, ok)
			_, err = ParseTableMappingSpec(t.Context(), create.MongoDBParam, create.Defs, &planpb.TableDef{
				Cols: []*planpb.ColDef{{Name: "v", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 64}}},
			})
			if tc.wantError {
				require.ErrorContains(t, err, "do not support non-NULL DEFAULT values")
				require.ErrorContains(t, err, "column v")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestProjectColumnsByNameKeepsCompactResidualLayout(t *testing.T) {
	columns := []ColumnMapping{
		{Name: "device_id", Path: "metadata.device_id"},
		{Name: "site_id", Path: "metadata.site_id"},
		{Name: "ts", Path: "event.ts"},
	}
	projected, err := ProjectColumnsByName(t.Context(), columns, []string{"ts", "device_id"})
	require.NoError(t, err)
	require.Equal(t, []ColumnMapping{columns[2], columns[0]}, projected)

	_, err = ProjectColumnsByName(t.Context(), columns, []string{"missing"})
	require.ErrorContains(t, err, "has no mapping")
	_, err = ProjectColumnsByName(t.Context(), append(columns, columns[0]), []string{"device_id"})
	require.ErrorContains(t, err, "duplicate")
}

func TestPlanPredicatePushdownKeepsResidualAndStrictFallback(t *testing.T) {
	columnExpr := &planpb.Expr{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
	literalExpr := &planpb.Expr{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 42}}}}
	filter := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{ObjName: ">="}, Args: []*planpb.Expr{columnExpr, literalExpr},
	}}}
	tryColumns := []*planpb.MongoColumnMapping{{Path: "reading.value", ConversionMode: ConversionTryNull, MoType: planpb.Type{Id: int32(types.T_int64)}}}
	pushed, residual := PushdownPlanFilters(context.Background(), []*planpb.Expr{filter}, tryColumns)
	require.NotNil(t, pushed)
	require.Equal(t, planpb.MongoPredicateOp_MONGO_PREDICATE_GREATER_EQUAL, pushed.Op)
	require.Equal(t, "reading.value", pushed.Path)
	require.NotEmpty(t, residual)

	strictColumns := []*planpb.MongoColumnMapping{{Path: "reading.value", ConversionMode: ConversionStrict, MoType: planpb.Type{Id: int32(types.T_float64)}}}
	pushed, residual = PushdownPlanFilters(context.Background(), []*planpb.Expr{filter}, strictColumns)
	require.Nil(t, pushed, "strict conversion must not hide malformed source values")
	require.NotEmpty(t, residual)

	isNull := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{ObjName: "is_null"}, Args: []*planpb.Expr{columnExpr},
	}}}
	pushed, residual = PushdownPlanFilters(context.Background(), []*planpb.Expr{isNull}, tryColumns)
	require.Nil(t, pushed, "IS NULL must retain malformed try_null values as candidates")
	require.NotEmpty(t, residual)

	isNotNull := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{ObjName: "is_not_null"}, Args: []*planpb.Expr{columnExpr},
	}}}
	pushed, residual = PushdownPlanFilters(context.Background(), []*planpb.Expr{isNotNull}, tryColumns)
	require.NotNil(t, pushed)
	require.Equal(t, planpb.MongoPredicateOp_MONGO_PREDICATE_IS_NOT_NULL, pushed.Op)
	require.NotEmpty(t, residual)

	pushed, residual = PushdownPlanFilters(context.Background(), []*planpb.Expr{isNotNull}, strictColumns)
	require.Nil(t, pushed, "strict IS NOT NULL must not hide malformed nested values")
	require.NotEmpty(t, residual)

	stringColumns := []*planpb.MongoColumnMapping{{Path: "reading.value", ConversionMode: ConversionTryNull, MoType: planpb.Type{Id: int32(types.T_varchar)}}}
	pushed, residual = PushdownPlanFilters(context.Background(), []*planpb.Expr{filter}, stringColumns)
	require.Nil(t, pushed, "string comparison collation has not been proven equivalent")
	require.NotEmpty(t, residual)

	floatColumns := []*planpb.MongoColumnMapping{{Path: "reading.value", ConversionMode: ConversionTryNull, MoType: planpb.Type{Id: int32(types.T_float32)}}}
	pushed, residual = PushdownPlanFilters(context.Background(), []*planpb.Expr{filter}, floatColumns)
	require.Nil(t, pushed, "lossy BSON-to-float conversion can make a raw-value predicate narrower than the SQL residual")
	require.NotEmpty(t, residual)
}

func TestStrictNestedIsNotNullKeepsMalformedIntermediateCandidate(t *testing.T) {
	columnExpr := &planpb.Expr{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
	filter := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{ObjName: "is_not_null"}, Args: []*planpb.Expr{columnExpr},
	}}}
	columns := []*planpb.MongoColumnMapping{{
		Path: "payload.value", ConversionMode: ConversionStrict,
		MoType: planpb.Type{Id: int32(types.T_int64)},
	}}

	pushed, residual := PushdownPlanFilters(t.Context(), []*planpb.Expr{filter}, columns)
	require.Nil(t, pushed)
	require.NotEmpty(t, residual)

	converter, err := NewConverter(t.Context(), []ColumnMapping{{
		Name: "value", Path: "payload.value", TypeID: int32(types.T_int64), Conversion: ConversionStrict,
	}}, 1024)
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	bat := converter.NewBatch()
	defer bat.Clean(mp)

	malformed, err := bson.Marshal(bson.D{{Key: "payload", Value: "not-a-document"}})
	require.NoError(t, err)
	require.ErrorContains(t, converter.AppendDocument(t.Context(), bat, malformed, mp), "cannot be converted")
	require.Zero(t, bat.RowCount())
}

func TestPlanPredicatePushesTryNullBSONDateTimeWithSafeRounding(t *testing.T) {
	columnExpr := &planpb.Expr{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
	columns := []*planpb.MongoColumnMapping{{
		Path: "ts", ConversionMode: ConversionTryNull,
		MoType: planpb.Type{Id: int32(types.T_datetime), Scale: 6},
	}}
	literalDatetime := types.DatetimeFromUnixWithNsec(time.UTC, 10, 123456000)
	literalExpr := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_datetime), Scale: 6},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: int64(literalDatetime)}}},
	}
	filter := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{ObjName: ">="}, Args: []*planpb.Expr{columnExpr, literalExpr},
	}}}
	pushed, residual := PushdownPlanFilters(t.Context(), []*planpb.Expr{filter}, columns)
	require.NotNil(t, pushed)
	require.NotEmpty(t, residual)
	predicate, err := PredicateFromPlan(t.Context(), pushed)
	require.NoError(t, err)
	require.Equal(t, bson.DateTime(10124), predicate.Value,
		">= must round its BSON millisecond candidate outward")

	castLiteral := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_datetime), Scale: 3},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: "cast"},
			Args: []*planpb.Expr{{
				Typ:  planpb.Type{Id: int32(types.T_varchar)},
				Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "2026-07-27 10:55:00.123"}}},
			}},
		}},
	}
	filter.GetF().Args[1] = castLiteral
	pushed, _ = PushdownPlanFilters(t.Context(), []*planpb.Expr{filter}, columns)
	require.NotNil(t, pushed, "a deterministic DATETIME string cast is a safe BSON candidate")

	timestampColumns := ColumnsToPlan([]ColumnMapping{{
		Path: "ts", TypeID: int32(types.T_timestamp), Scale: 3, Conversion: ConversionTryNull,
	}})
	castLiteral.Typ.Id = int32(types.T_timestamp)
	pushed, _ = PushdownPlanFilters(t.Context(), []*planpb.Expr{filter}, timestampColumns)
	require.Nil(t, pushed, "a TIMESTAMP string cast depends on the session time zone")
	castLiteral.Typ.Id = int32(types.T_datetime)

	filter.GetF().Func.ObjName = "="
	filter.GetF().Args[1] = literalExpr
	pushed, _ = PushdownPlanFilters(t.Context(), []*planpb.Expr{filter}, columns)
	require.Nil(t, pushed, "a sub-millisecond equality has no exact BSON DateTime candidate")

	strictColumns := ColumnsToPlan([]ColumnMapping{{
		Path: "ts", TypeID: int32(types.T_datetime), Scale: 6, Conversion: ConversionStrict,
	}})
	filter.GetF().Func.ObjName = ">="
	pushed, _ = PushdownPlanFilters(t.Context(), []*planpb.Expr{filter}, strictColumns)
	require.Nil(t, pushed, "strict temporal conversion must not hide malformed values")
}

func TestPlanPredicateKeepsSubMillisecondScaleTemporalMappingsResidual(t *testing.T) {
	columnExpr := &planpb.Expr{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
	literalDatetime := types.DatetimeFromUnixWithNsec(time.UTC, 10, 0)
	literalExpr := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_datetime)},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: int64(literalDatetime)}}},
	}
	comparison := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{ObjName: "="}, Args: []*planpb.Expr{columnExpr, literalExpr},
	}}}
	in := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{ObjName: "in"}, Args: []*planpb.Expr{columnExpr, {
			Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{literalExpr}}},
		}},
	}}}

	for _, target := range []types.T{types.T_datetime, types.T_timestamp} {
		for scale := int32(0); scale < 3; scale++ {
			columns := []*planpb.MongoColumnMapping{{
				Path: "ts", ConversionMode: ConversionTryNull,
				MoType: planpb.Type{Id: int32(target), Scale: scale},
			}}
			for _, filter := range []*planpb.Expr{comparison, in} {
				pushed, residual := PushdownPlanFilters(t.Context(), []*planpb.Expr{filter}, columns)
				require.Nil(t, pushed, "%s(%d)", target, scale)
				require.NotEmpty(t, residual)
			}
		}
	}
}

func TestTemporalCandidateRoundingBeforeUnixEpoch(t *testing.T) {
	require.Equal(t, int64(-2), floorDiv(-1001, 1000))
	require.Equal(t, int64(-1), ceilDiv(-1001, 1000))
	require.Equal(t, int64(-1), floorDiv(-999, 1000))
	require.Equal(t, int64(0), ceilDiv(-999, 1000))
}

func TestPlanPredicateHelperVariants(t *testing.T) {
	for _, tc := range []struct {
		name string
		op   PredicateOp
		want PredicateOp
	}{
		{name: "less", op: PredicateLess, want: PredicateGreater},
		{name: "less equal", op: PredicateLessEqual, want: PredicateGreaterEqual},
		{name: "greater", op: PredicateGreater, want: PredicateLess},
		{name: "greater equal", op: PredicateGreaterEqual, want: PredicateLessEqual},
		{name: "equal", op: PredicateEqual, want: PredicateEqual},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, reversePredicateOp(tc.op)) })
	}

	for _, tc := range []struct {
		name string
		want PredicateOp
		ok   bool
	}{
		{name: "=", want: PredicateEqual, ok: true},
		{name: "!=", want: PredicateNotEqual, ok: true},
		{name: "<>", want: PredicateNotEqual, ok: true},
		{name: "<", want: PredicateLess, ok: true},
		{name: "<=", want: PredicateLessEqual, ok: true},
		{name: ">", want: PredicateGreater, ok: true},
		{name: ">=", want: PredicateGreaterEqual, ok: true},
		{name: "like", want: PredicateInvalid},
	} {
		t.Run("operator "+tc.name, func(t *testing.T) {
			got, ok := comparisonPredicateOp(tc.name)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.want, got)
		})
	}

	literals := []struct {
		name    string
		literal *planpb.Literal
		want    any
		ok      bool
	}{
		{name: "nil"},
		{name: "null", literal: &planpb.Literal{Isnull: true}, ok: true},
		{name: "bool", literal: &planpb.Literal{Value: &planpb.Literal_Bval{Bval: true}}, want: true, ok: true},
		{name: "int8", literal: &planpb.Literal{Value: &planpb.Literal_I8Val{I8Val: 8}}, want: int32(8), ok: true},
		{name: "int16", literal: &planpb.Literal{Value: &planpb.Literal_I16Val{I16Val: 16}}, want: int32(16), ok: true},
		{name: "int32", literal: &planpb.Literal{Value: &planpb.Literal_I32Val{I32Val: 32}}, want: int32(32), ok: true},
		{name: "int64", literal: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 64}}, want: int64(64), ok: true},
		{name: "uint8", literal: &planpb.Literal{Value: &planpb.Literal_U8Val{U8Val: 8}}, want: int32(8), ok: true},
		{name: "uint16", literal: &planpb.Literal{Value: &planpb.Literal_U16Val{U16Val: 16}}, want: int32(16), ok: true},
		{name: "uint32", literal: &planpb.Literal{Value: &planpb.Literal_U32Val{U32Val: 32}}, want: int64(32), ok: true},
		{name: "uint64", literal: &planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: 64}}, want: int64(64), ok: true},
		{name: "uint64 overflow", literal: &planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: math.MaxUint64}}},
		{name: "float32", literal: &planpb.Literal{Value: &planpb.Literal_Fval{Fval: 1.5}}, want: float32(1.5), ok: true},
		{name: "float64", literal: &planpb.Literal{Value: &planpb.Literal_Dval{Dval: 2.5}}, want: 2.5, ok: true},
		{name: "string", literal: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "value"}}, want: "value", ok: true},
		{name: "unsupported", literal: &planpb.Literal{Value: &planpb.Literal_Dateval{Dateval: 1}}},
	}
	for _, tc := range literals {
		t.Run("literal "+tc.name, func(t *testing.T) {
			expr := &planpb.Expr{}
			if tc.literal != nil {
				expr.Expr = &planpb.Expr_Lit{Lit: tc.literal}
			}
			got, ok := scalarPlanLiteral(expr)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.want, got)
		})
	}

	for _, tc := range []struct {
		op    PredicateOp
		micro int64
		want  int64
		ok    bool
	}{
		{op: PredicateEqual, micro: 2000, want: 2, ok: true},
		{op: PredicateNotEqual, micro: 2001},
		{op: PredicateGreaterEqual, micro: 2001, want: 3, ok: true},
		{op: PredicateLess, micro: 2001, want: 3, ok: true},
		{op: PredicateGreater, micro: 2001, want: 2, ok: true},
		{op: PredicateLessEqual, micro: 2001, want: 2, ok: true},
		{op: PredicateIn, micro: 2000},
	} {
		got, ok := temporalCandidateMilliseconds(tc.micro, tc.op)
		require.Equal(t, tc.ok, ok)
		require.Equal(t, tc.want, got)
	}
}

func TestPlanPredicateCompoundInAndReversedComparisons(t *testing.T) {
	column := &planpb.Expr{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
	badColumn := &planpb.Expr{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 9}}}
	literal := func(value int64) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: value}}}}
	}
	fn := func(name string, args ...*planpb.Expr) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: name}, Args: args}}}
	}
	columns := []*planpb.MongoColumnMapping{{
		Path: "reading.value", ConversionMode: ConversionTryNull, MoType: planpb.Type{Id: int32(types.T_int64)},
	}}

	reversed, ok := planExprToPredicate(fn("<", literal(10), column), columns)
	require.True(t, ok)
	require.Equal(t, PredicateGreater, reversed.Op)
	require.Equal(t, int64(10), reversed.Value)

	and, ok := planExprToPredicate(fn("and", fn(">", column, literal(1)), fn("<=", column, literal(9))), columns)
	require.True(t, ok)
	require.Equal(t, PredicateAnd, and.Op)
	require.Len(t, and.Children, 2)
	_, ok = planExprToPredicate(fn("and", fn(">", column, literal(1)), fn("=", badColumn, literal(9))), columns)
	require.False(t, ok)

	inList := &planpb.Expr{Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{literal(1), literal(2)}}}}
	in, ok := planExprToPredicate(fn("in", column, inList), columns)
	require.True(t, ok)
	require.Equal(t, []any{int64(1), int64(2)}, in.Values)
	for _, invalid := range []*planpb.Expr{
		fn("in", badColumn, inList),
		fn("in", column, &planpb.Expr{Expr: &planpb.Expr_List{List: &planpb.ExprList{}}}),
		fn("in", column, &planpb.Expr{Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}}}}}}),
		fn("unknown", column, literal(1)),
		fn("=", column),
		{},
	} {
		_, ok = planExprToPredicate(invalid, columns)
		require.False(t, ok)
	}

	isNotNull, ok := planExprToPredicate(fn("isnotnull", column), columns)
	require.True(t, ok)
	require.Equal(t, PredicateIsNotNull, isNotNull.Op)
	_, ok = planExprToPredicate(fn("isnull", column), columns)
	require.False(t, ok)
	_, ok = planExprToPredicate(fn("is_not_null", badColumn), columns)
	require.False(t, ok)

	pushed, residual := PushdownPlanFilters(t.Context(), []*planpb.Expr{fn(">", column, literal(1)), fn("unknown")}, columns)
	require.NotNil(t, pushed)
	require.Equal(t, "mo-residual:ff", residual)
	require.Equal(t, "", residualDigest(nil))
}

func TestSourceLimiterCancellationReleaseAndKeyRecycling(t *testing.T) {
	limiter := NewSourceLimiter(1)
	release, err := limiter.Acquire(context.Background(), 7, 9)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = limiter.Acquire(ctx, 7, 9)
	require.ErrorIs(t, err, context.Canceled)
	release()
	release()
	limiter.mu.Lock()
	require.Empty(t, limiter.sources)
	limiter.mu.Unlock()

	// Recreating a previously idle key must still enforce the same limit.
	release, err = limiter.Acquire(context.Background(), 7, 9)
	require.NoError(t, err)
	release()
	limiter.mu.Lock()
	require.Empty(t, limiter.sources)
	limiter.mu.Unlock()
}

func TestConverterTypesMissingTryNullAndRollback(t *testing.T) {
	ctx := context.Background()
	mp, err := mpool.NewMPool("mongodb-converter-test", 0, mpool.NoFixed)
	require.NoError(t, err)
	columns := []ColumnMapping{
		{Name: "id", Path: "_id", TypeID: int32(types.T_varchar), Width: 24},
		{Name: "device_id", Path: "meta.device_id", TypeID: int32(types.T_varchar), Width: 32},
		{Name: "count", Path: "count", TypeID: int32(types.T_int32)},
		{Name: "value", Path: "value", TypeID: int32(types.T_float64)},
		{Name: "when", Path: "when", TypeID: int32(types.T_datetime), Scale: 3},
		{Name: "optional", Path: "optional", TypeID: int32(types.T_int64)},
		{Name: "try_value", Path: "bad", TypeID: int32(types.T_int64), Conversion: ConversionTryNull},
	}
	converter, err := NewConverter(ctx, columns, 1024)
	require.NoError(t, err)
	bat := converter.NewBatch()
	t.Cleanup(func() {
		bat.Clean(mp)
		require.Zero(t, mp.CurrNB())
	})

	objectID := bson.NewObjectID()
	raw, err := bson.Marshal(bson.D{
		{Key: "_id", Value: objectID},
		{Key: "meta", Value: bson.D{{Key: "device_id", Value: "device-001"}}},
		{Key: "count", Value: int64(7)},
		{Key: "value", Value: 2.675},
		{Key: "when", Value: time.Date(2026, 7, 27, 8, 9, 10, 123000000, time.UTC)},
		{Key: "optional", Value: nil},
		{Key: "bad", Value: "not-an-int"},
	})
	require.NoError(t, err)
	require.NoError(t, converter.AppendDocument(ctx, bat, raw, mp))
	require.Equal(t, 1, bat.RowCount())
	require.Equal(t, objectID.Hex(), string(bat.Vecs[0].GetBytesAt(0)))
	require.Equal(t, "device-001", string(bat.Vecs[1].GetBytesAt(0)))
	require.Equal(t, int32(7), vector.GetFixedAtWithTypeCheck[int32](bat.Vecs[2], 0))
	require.Equal(t, 2.675, vector.GetFixedAtWithTypeCheck[float64](bat.Vecs[3], 0))
	require.True(t, bat.Vecs[5].GetNulls().Contains(0))
	require.True(t, bat.Vecs[6].GetNulls().Contains(0))

	// A strict error after earlier columns were appended must not leave a
	// half-row in any vector.
	badRaw, err := bson.Marshal(bson.D{
		{Key: "_id", Value: objectID},
		{Key: "meta", Value: bson.D{{Key: "device_id", Value: "device-002"}}},
		{Key: "count", Value: int64(1) << 40},
	})
	require.NoError(t, err)
	require.Error(t, converter.AppendDocument(ctx, bat, badRaw, mp))
	require.Equal(t, 1, bat.RowCount())
	for _, vec := range bat.Vecs {
		require.Equal(t, 1, vec.Length())
	}
}

func TestConverterRejectsNullForNotNullMapping(t *testing.T) {
	columns := []ColumnMapping{{
		Name: "value", Path: "payload.value", TypeID: int32(types.T_int64),
		NotNullable: true, Conversion: ConversionTryNull,
	}}
	require.Equal(t, columns, ColumnsFromPlan(ColumnsToPlan(columns)))

	converter, err := NewConverter(t.Context(), columns, 1024)
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	bat := converter.NewBatch()
	t.Cleanup(func() {
		bat.Clean(mp)
		require.Zero(t, mp.CurrNB())
	})

	for _, tc := range []struct {
		name string
		doc  bson.D
	}{
		{name: "missing", doc: bson.D{}},
		{name: "explicit BSON null", doc: bson.D{{Key: "payload", Value: bson.D{{Key: "value", Value: nil}}}}},
		{name: "BSON undefined", doc: bson.D{{Key: "payload", Value: bson.D{{Key: "value", Value: bson.Undefined{}}}}}},
		{name: "try_null conversion failure", doc: bson.D{{Key: "payload", Value: bson.D{{Key: "value", Value: "not-an-int"}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, marshalErr := bson.Marshal(tc.doc)
			require.NoError(t, marshalErr)
			require.ErrorContains(t, converter.AppendDocument(t.Context(), bat, raw, mp), "NOT NULL column value")
			require.Zero(t, bat.RowCount())
			require.Zero(t, bat.Vecs[0].Length())
		})
	}
}

func TestConverterRejectsOversizeAndUnsupportedType(t *testing.T) {
	ctx := context.Background()
	_, err := NewConverter(ctx, []ColumnMapping{{Name: "x", Path: "x", TypeID: int32(types.T_array_float32)}}, 1)
	require.Error(t, err)
	converter, err := NewConverter(ctx, []ColumnMapping{{Name: "x", Path: "x", TypeID: int32(types.T_varchar)}}, 8)
	require.NoError(t, err)
	mp, err := mpool.NewMPool("mongodb-oversize-test", 0, mpool.NoFixed)
	require.NoError(t, err)
	bat := converter.NewBatch()
	raw, err := bson.Marshal(bson.D{{Key: "x", Value: "larger-than-eight"}})
	require.NoError(t, err)
	require.Error(t, converter.AppendDocument(ctx, bat, raw, mp))
	require.Zero(t, bat.RowCount())
	bat.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func TestConverterTemporalRangeScaleAndTryNull(t *testing.T) {
	mp := mpool.MustNewZero()
	invalidValues := []bson.DateTime{
		bson.DateTime(math.MinInt64),
		bson.DateTime(time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()),
		bson.DateTime(math.MaxInt64),
	}
	for _, conversion := range []string{ConversionStrict, ConversionTryNull} {
		converter, err := NewConverter(t.Context(), []ColumnMapping{{
			Name: "ts", TypeID: int32(types.T_timestamp), Scale: 0, Conversion: conversion,
		}}, 1024)
		require.NoError(t, err)
		bat := converter.NewBatch()
		for _, invalid := range invalidValues {
			raw, marshalErr := bson.Marshal(bson.D{{Key: "ts", Value: invalid}})
			require.NoError(t, marshalErr)
			err = converter.AppendDocument(t.Context(), bat, raw, mp)
			if conversion == ConversionStrict {
				require.ErrorContains(t, err, "cannot be converted")
				require.Zero(t, bat.RowCount())
			} else {
				require.NoError(t, err)
				require.True(t, bat.Vecs[0].IsNull(uint64(bat.RowCount()-1)))
			}
		}
		bat.Clean(mp)
	}

	converter, err := NewConverter(t.Context(), []ColumnMapping{{
		Name: "ts", TypeID: int32(types.T_timestamp), Scale: 0, Conversion: ConversionStrict,
	}}, 1024)
	require.NoError(t, err)
	bat := converter.NewBatch()
	instant := time.Date(2026, 7, 29, 10, 11, 12, 100*int(time.Millisecond), time.UTC)
	raw, err := bson.Marshal(bson.D{{Key: "ts", Value: instant}})
	require.NoError(t, err)
	require.NoError(t, converter.AppendDocument(t.Context(), bat, raw, mp))
	want, err := types.ParseTimestamp(time.UTC, "2026-07-29 10:11:12", 0)
	require.NoError(t, err)
	require.Equal(t, want, vector.GetFixedAtNoTypeCheck[types.Timestamp](bat.Vecs[0], 0))
	bat.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func TestConverterEnforcesDecodedVectorBudgetIncrementally(t *testing.T) {
	const (
		columnCount = 64
		valueBytes  = 256 << 10
		budget      = 1 << 20
	)
	columns := make([]ColumnMapping, columnCount)
	for i := range columns {
		columns[i] = ColumnMapping{
			Name: fmt.Sprintf("copy_%d", i), Path: "payload",
			TypeID: int32(types.T_blob), Conversion: ConversionTryNull,
		}
	}
	converter, err := NewConverter(t.Context(), columns, valueBytes+1024)
	require.NoError(t, err)
	raw, err := bson.Marshal(bson.D{{Key: "payload", Value: bson.Binary{Data: make([]byte, valueBytes)}}})
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	bat := converter.NewBatch()
	err = converter.AppendDocumentWithBudget(t.Context(), bat, raw, mp, budget)
	require.True(t, IsDecodedBatchBudgetExceeded(err), "unexpected conversion result: %v", err)
	require.Zero(t, bat.RowCount())
	require.Zero(t, converter.conversionAttempts, "a deferred row must not be counted before it commits")
	require.LessOrEqual(t, bat.Size(), budget)
	require.Less(t, bat.Allocated(), 2*budget, "conversion must stop before duplicating the value into every mapped column")
	bat.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func TestConverterVarcharWidthCountsUnicodeCharacters(t *testing.T) {
	converter, err := NewConverter(t.Context(), []ColumnMapping{
		{Name: "value", Path: "value", TypeID: int32(types.T_varchar), Width: 2},
	}, 1024)
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	bat := converter.NewBatch()

	raw, err := bson.Marshal(bson.D{{Key: "value", Value: "你好"}})
	require.NoError(t, err)
	require.NoError(t, converter.AppendDocument(t.Context(), bat, raw, mp))
	require.Equal(t, "你好", string(bat.Vecs[0].GetBytesAt(0)))

	raw, err = bson.Marshal(bson.D{{Key: "value", Value: "你好世"}})
	require.NoError(t, err)
	require.Error(t, converter.AppendDocument(t.Context(), bat, raw, mp))
	require.Equal(t, 1, bat.RowCount())
	bat.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func TestConverterNestedPathDistinguishesMissingAndInvalidTraversal(t *testing.T) {
	mp := mpool.MustNewZero()
	strict, err := NewConverter(t.Context(), []ColumnMapping{
		{Name: "value", Path: "reading.value", TypeID: int32(types.T_int64), Conversion: ConversionStrict},
	}, 1024)
	require.NoError(t, err)
	bat := strict.NewBatch()

	missing, err := bson.Marshal(bson.D{{Key: "other", Value: 1}})
	require.NoError(t, err)
	require.NoError(t, strict.AppendDocument(t.Context(), bat, missing, mp))
	require.True(t, bat.Vecs[0].IsNull(0))

	for _, malformed := range []any{"scalar-parent", bson.A{bson.D{{Key: "value", Value: 1}}}} {
		raw, marshalErr := bson.Marshal(bson.D{{Key: "reading", Value: malformed}})
		require.NoError(t, marshalErr)
		require.Error(t, strict.AppendDocument(t.Context(), bat, raw, mp))
		require.Equal(t, 1, bat.RowCount())
	}
	bat.Clean(mp)
	require.Zero(t, mp.CurrNB())

	tryNull, err := NewConverter(t.Context(), []ColumnMapping{
		{Name: "value", Path: "reading.value", TypeID: int32(types.T_int64), Conversion: ConversionTryNull},
	}, 1024)
	require.NoError(t, err)
	bat = tryNull.NewBatch()
	raw, err := bson.Marshal(bson.D{{Key: "reading", Value: "scalar-parent"}})
	require.NoError(t, err)
	require.NoError(t, tryNull.AppendDocument(t.Context(), bat, raw, mp))
	require.True(t, bat.Vecs[0].IsNull(0))
	require.Equal(t, int64(1), tryNull.ConversionErrors())
	bat.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func TestConverterMpoolFailureLeavesNoPartialRow(t *testing.T) {
	ctx := context.Background()
	converter, err := NewConverter(ctx, []ColumnMapping{
		{Name: "first", Path: "first", TypeID: int32(types.T_varchar)},
		{Name: "second", Path: "second", TypeID: int32(types.T_varchar)},
	}, 2<<20)
	require.NoError(t, err)
	mp, err := mpool.NewMPool("mongodb-oom-test", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	bat := converter.NewBatch()
	for _, vec := range bat.Vecs {
		vec.SetOffHeap(true)
	}
	raw, err := bson.Marshal(bson.D{
		{Key: "first", Value: strings.Repeat("a", 700<<10)},
		{Key: "second", Value: strings.Repeat("b", 700<<10)},
	})
	require.NoError(t, err)
	require.Error(t, converter.AppendDocument(ctx, bat, raw, mp))
	require.Zero(t, bat.RowCount())
	for _, vec := range bat.Vecs {
		require.Zero(t, vec.Length())
	}
	bat.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func TestConverterTryNullDoesNotSwallowMpoolFailure(t *testing.T) {
	ctx := context.Background()
	converter, err := NewConverter(ctx, []ColumnMapping{
		{Name: "value", Path: "value", TypeID: int32(types.T_varchar), Conversion: ConversionTryNull},
	}, 2<<20)
	require.NoError(t, err)
	mp, err := mpool.NewMPool("mongodb-try-null-oom-test", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	bat := converter.NewBatch()
	bat.Vecs[0].SetOffHeap(true)
	raw, err := bson.Marshal(bson.D{{Key: "value", Value: strings.Repeat("x", 1100<<10)}})
	require.NoError(t, err)

	require.Error(t, converter.AppendDocument(ctx, bat, raw, mp))
	require.Zero(t, bat.RowCount())
	require.Zero(t, bat.Vecs[0].Length())
	bat.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func TestConverterTryNullStatementErrorLimits(t *testing.T) {
	columns := []ColumnMapping{
		{Name: "value", Path: "value", TypeID: int32(types.T_int64), Conversion: ConversionTryNull},
	}
	converter, err := NewConverter(t.Context(), columns, 1024)
	require.NoError(t, err)
	converter.SetConversionErrorLimits(1, 1)
	mp := mpool.MustNewZero()
	bat := converter.NewBatch()
	bad, err := bson.Marshal(bson.D{{Key: "value", Value: "not-an-int"}})
	require.NoError(t, err)
	require.NoError(t, converter.AppendDocument(t.Context(), bat, bad, mp))
	require.True(t, bat.Vecs[0].IsNull(0))
	require.Equal(t, int64(1), converter.ConversionErrors())
	require.ErrorContains(t, converter.AppendDocument(t.Context(), bat, bad, mp), "error limit")
	require.Equal(t, 1, bat.RowCount(), "the threshold-crossing row must be rolled back")
	bat.Clean(mp)
	require.Zero(t, mp.CurrNB())

	converter, err = NewConverter(t.Context(), columns, 1024)
	require.NoError(t, err)
	converter.SetConversionErrorLimits(1_000, 0.05)
	bat = converter.NewBatch()
	for range conversionErrorRateMinAttempts - 1 {
		require.NoError(t, converter.AppendDocument(t.Context(), bat, bad, mp))
	}
	require.ErrorContains(t, converter.AppendDocument(t.Context(), bat, bad, mp), "error limit")
	require.Equal(t, conversionErrorRateMinAttempts-1, bat.RowCount())
	bat.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func TestBSONDoubleToInt64RejectsTwoToThe63rd(t *testing.T) {
	raw, err := bson.Marshal(bson.D{{Key: "value", Value: float64(9223372036854775808)}})
	require.NoError(t, err)
	value := bson.Raw(raw).Lookup("value")
	converted, ok := bsonInt64(value)
	require.False(t, ok)
	require.Zero(t, converted)
}

func TestConverterDecimalBinaryAndInternalJSONEncoding(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	decimal, err := bson.ParseDecimal128("123.456")
	require.NoError(t, err)
	converter, err := NewConverter(ctx, []ColumnMapping{
		{Name: "decimal", Path: "decimal", TypeID: int32(types.T_decimal128), Width: 18, Scale: 3},
		{Name: "binary", Path: "binary", TypeID: int32(types.T_varbinary), Width: 16},
		{Name: "document", Path: "document", TypeID: int32(types.T_json)},
	}, 1024)
	require.NoError(t, err)
	raw, err := bson.Marshal(bson.D{
		{Key: "decimal", Value: decimal},
		{Key: "binary", Value: bson.Binary{Subtype: 0, Data: []byte{1, 2, 3}}},
		{Key: "document", Value: bson.D{{Key: "nested", Value: bson.A{1, "two"}}}},
	})
	require.NoError(t, err)
	bat := converter.NewBatch()
	require.NoError(t, converter.AppendDocument(ctx, bat, raw, mp))
	require.Equal(t, "123.456", vector.GetFixedAtNoTypeCheck[types.Decimal128](bat.Vecs[0], 0).Format(3))
	require.Equal(t, []byte{1, 2, 3}, bat.Vecs[1].GetBytesAt(0))
	require.JSONEq(t, `{"nested":[{"$numberInt":"1"},"two"]}`, types.DecodeJson(bat.Vecs[2].GetBytesAt(0)).String())
	bat.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func TestConverterJSONValues(t *testing.T) {
	decimal, err := bson.ParseDecimal128("123.456")
	require.NoError(t, err)
	objectID := bson.NewObjectID()
	instant := time.Date(2026, 8, 18, 9, 10, 11, 123000000, time.UTC)

	tests := []struct {
		name     string
		value    any
		missing  bool
		wantJSON string
		wantNull bool
	}{
		{name: "string", value: "text", wantJSON: `"text"`},
		{name: "empty string", value: "", wantJSON: `""`},
		{name: "int32", value: int32(32), wantJSON: `{"$numberInt":"32"}`},
		{name: "int64", value: int64(64), wantJSON: `{"$numberLong":"64"}`},
		{name: "double", value: 1.5, wantJSON: `{"$numberDouble":"1.5"}`},
		{name: "decimal128", value: decimal, wantJSON: `{"$numberDecimal":"123.456"}`},
		{name: "bool", value: true, wantJSON: `true`},
		{name: "date", value: instant, wantJSON: fmt.Sprintf(`{"$date":{"$numberLong":"%d"}}`, instant.UnixMilli())},
		{name: "binary", value: bson.Binary{Data: []byte{1, 2, 3}}, wantJSON: `{"$binary":{"base64":"AQID","subType":"00"}}`},
		{name: "objectID", value: objectID, wantJSON: fmt.Sprintf(`{"$oid":"%s"}`, objectID.Hex())},
		{name: "document", value: bson.D{{Key: "nested", Value: int32(1)}}, wantJSON: `{"nested":{"$numberInt":"1"}}`},
		{name: "array", value: bson.A{int32(1), "two"}, wantJSON: `[{"$numberInt":"1"},"two"]`},
		{name: "null", value: nil, wantNull: true},
		{name: "missing", missing: true, wantNull: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			converter, err := NewConverter(t.Context(), []ColumnMapping{{
				Name: "value", TypeID: int32(types.T_json),
			}}, 1024)
			require.NoError(t, err)
			mp := mpool.MustNewZero()
			bat := converter.NewBatch()
			t.Cleanup(func() {
				bat.Clean(mp)
				require.Zero(t, mp.CurrNB())
			})

			doc := bson.D{{Key: "value", Value: tc.value}}
			if tc.missing {
				doc = bson.D{{Key: "other", Value: int32(1)}}
			}
			raw, err := bson.Marshal(doc)
			require.NoError(t, err)
			require.NoError(t, converter.AppendDocument(t.Context(), bat, raw, mp))
			require.Equal(t, 1, bat.RowCount())
			if tc.wantNull {
				require.True(t, bat.Vecs[0].GetNulls().Contains(0))
				return
			}
			require.JSONEq(t, tc.wantJSON, types.DecodeJson(bat.Vecs[0].GetBytesAt(0)).String())
		})
	}
}

func TestConverterCoversSupportedScalarFamilies(t *testing.T) {
	mp := mpool.MustNewZero()
	decimal, err := bson.ParseDecimal128("12.345")
	require.NoError(t, err)
	objectID := bson.NewObjectID()
	when := time.Date(2026, 7, 29, 10, 11, 12, 345000000, time.UTC)
	columns := []ColumnMapping{
		{Name: "bool", TypeID: int32(types.T_bool)},
		{Name: "int8", TypeID: int32(types.T_int8)},
		{Name: "int16", TypeID: int32(types.T_int16)},
		{Name: "int64", TypeID: int32(types.T_int64)},
		{Name: "uint8", TypeID: int32(types.T_uint8)},
		{Name: "uint16", TypeID: int32(types.T_uint16)},
		{Name: "uint32", TypeID: int32(types.T_uint32)},
		{Name: "uint64", TypeID: int32(types.T_uint64)},
		{Name: "float32", TypeID: int32(types.T_float32)},
		{Name: "decimal64", TypeID: int32(types.T_decimal64), Width: 18, Scale: 2},
		{Name: "decimal128", TypeID: int32(types.T_decimal128), Width: 20, Scale: 3},
		{Name: "decimal256", TypeID: int32(types.T_decimal256), Width: 38, Scale: 3},
		{Name: "date", TypeID: int32(types.T_date)},
		{Name: "timestamp", TypeID: int32(types.T_timestamp)},
		{Name: "char", TypeID: int32(types.T_char), Width: 8},
		{Name: "text", TypeID: int32(types.T_text)},
		{Name: "binary", TypeID: int32(types.T_binary), Width: 8},
		{Name: "varbinary", TypeID: int32(types.T_varbinary), Width: 12},
		{Name: "blob", TypeID: int32(types.T_blob)},
	}
	converter, err := NewConverter(t.Context(), columns, 0)
	require.NoError(t, err)
	require.Equal(t, columns, converter.Columns())
	copyOfColumns := converter.Columns()
	copyOfColumns[0].Name = "changed"
	require.Equal(t, "bool", converter.Columns()[0].Name)
	converter.SetConversionErrorLimits(3, 0.25)
	require.Equal(t, int64(3), converter.maxConversionErrors)
	require.Equal(t, 0.25, converter.maxConversionErrorRate)
	converter.SetConversionErrorLimits(0, 2)
	require.Equal(t, DefaultRuntimeConfig().MaxConversionErrors, converter.maxConversionErrors)
	require.Equal(t, DefaultRuntimeConfig().MaxConversionErrorRate, converter.maxConversionErrorRate)

	raw, err := bson.Marshal(bson.D{
		{Key: "bool", Value: true},
		{Key: "int8", Value: int32(8)},
		{Key: "int16", Value: int32(16)},
		{Key: "int64", Value: float64(64)},
		{Key: "uint8", Value: int32(8)},
		{Key: "uint16", Value: int32(16)},
		{Key: "uint32", Value: int64(32)},
		{Key: "uint64", Value: int64(64)},
		{Key: "float32", Value: 1.5},
		{Key: "decimal64", Value: int32(12)},
		{Key: "decimal128", Value: int64(12345)},
		{Key: "decimal256", Value: decimal},
		{Key: "date", Value: when},
		{Key: "timestamp", Value: when},
		{Key: "char", Value: "text"},
		{Key: "text", Value: objectID},
		{Key: "binary", Value: bson.Binary{Data: []byte{1, 2}}},
		{Key: "varbinary", Value: objectID},
		{Key: "blob", Value: bson.Binary{Data: []byte{3, 4}}},
	})
	require.NoError(t, err)
	bat := converter.NewBatch()
	require.NoError(t, converter.AppendDocument(t.Context(), bat, raw, mp))
	require.Equal(t, 1, bat.RowCount())
	require.True(t, vector.GetFixedAtNoTypeCheck[bool](bat.Vecs[0], 0))
	require.Equal(t, int8(8), vector.GetFixedAtNoTypeCheck[int8](bat.Vecs[1], 0))
	require.Equal(t, uint64(64), vector.GetFixedAtNoTypeCheck[uint64](bat.Vecs[7], 0))
	require.Equal(t, objectID.Hex(), string(bat.Vecs[15].GetBytesAt(0)))
	require.Equal(t, objectID[:], bat.Vecs[17].GetBytesAt(0))
	bat.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func TestConverterRejectsInvalidMappingsAndScalarBounds(t *testing.T) {
	for _, tc := range []struct {
		name    string
		columns []ColumnMapping
	}{
		{name: "empty"},
		{name: "empty name", columns: []ColumnMapping{{TypeID: int32(types.T_int64)}}},
		{name: "duplicate", columns: []ColumnMapping{{Name: "Value", TypeID: int32(types.T_int64)}, {Name: "value", TypeID: int32(types.T_int64)}}},
		{name: "invalid path", columns: []ColumnMapping{{Name: "value", Path: "$where", TypeID: int32(types.T_int64)}}},
		{name: "invalid conversion", columns: []ColumnMapping{{Name: "value", TypeID: int32(types.T_int64), Conversion: "lossy"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewConverter(t.Context(), tc.columns, 1024)
			require.Error(t, err)
		})
	}

	mp := mpool.MustNewZero()
	tooWideDecimal, err := bson.ParseDecimal128("123456789")
	require.NoError(t, err)
	for _, tc := range []struct {
		name   string
		target types.T
		width  int32
		scale  int32
		value  any
	}{
		{name: "bool", target: types.T_bool, value: "true"},
		{name: "int8", target: types.T_int8, value: int32(math.MaxInt8 + 1)},
		{name: "int16", target: types.T_int16, value: int32(math.MaxInt16 + 1)},
		{name: "int64 fraction", target: types.T_int64, value: 1.5},
		{name: "uint negative", target: types.T_uint64, value: int32(-1)},
		{name: "uint8", target: types.T_uint8, value: int32(math.MaxUint8 + 1)},
		{name: "uint16", target: types.T_uint16, value: int64(math.MaxUint16 + 1)},
		{name: "uint32", target: types.T_uint32, value: int64(math.MaxUint32 + 1)},
		{name: "float32", target: types.T_float32, value: math.MaxFloat64},
		{name: "decimal type", target: types.T_decimal64, width: 18, scale: 2, value: "12.3"},
		{name: "decimal width", target: types.T_decimal64, width: 4, scale: 0, value: tooWideDecimal},
		{name: "date", target: types.T_date, value: "2026-07-29"},
		{name: "char", target: types.T_char, width: 8, value: int32(1)},
		{name: "binary", target: types.T_binary, width: 8, value: "bytes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			converter, err := NewConverter(t.Context(), []ColumnMapping{{
				Name: "value", TypeID: int32(tc.target), Width: tc.width, Scale: tc.scale,
			}}, 1024)
			require.NoError(t, err)
			raw, err := bson.Marshal(bson.D{{Key: "value", Value: tc.value}})
			require.NoError(t, err)
			bat := converter.NewBatch()
			require.Error(t, converter.AppendDocument(t.Context(), bat, raw, mp))
			require.Zero(t, bat.RowCount())
			bat.Clean(mp)
		})
	}
	require.Zero(t, mp.CurrNB())

	for _, value := range []any{math.NaN(), math.Inf(1), math.Inf(-1), float64(9223372036854775808)} {
		raw, err := bson.Marshal(bson.D{{Key: "value", Value: value}})
		require.NoError(t, err)
		_, ok := bsonInt64(bson.Raw(raw).Lookup("value"))
		require.False(t, ok)
	}
}

func TestConnectionValidationAndRedaction(t *testing.T) {
	ctx := context.Background()
	base := Connection{
		Name: "c", Hosts: "db.mongo.example:27017", CredentialSecretRef: "secret://mongodb/reader",
		AuthMechanism: "SCRAM-SHA-256", TLSMode: "required", ReadPreference: "secondaryPreferred", ReadConcern: "majority",
	}
	cfg := DefaultRuntimeConfig()
	cfg.AllowedHostSuffixes = []string{"mongo.example"}
	require.NoError(t, ValidateConnection(ctx, base, cfg))
	bad := base
	bad.Hosts = "mongodb://user:password@db:27017"
	require.Error(t, ValidateConnection(ctx, bad, cfg))
	bad = base
	bad.Hosts = "127.0.0.1:27017"
	require.Error(t, ValidateConnection(ctx, bad, cfg))
	bad = base
	bad.CredentialSecretRef = "reader:plaintext"
	require.Error(t, ValidateConnection(ctx, bad, cfg))
	bad = base
	bad.OptionsJSON = `{"password":"must-not-be-smuggled"}`
	require.Error(t, ValidateConnection(ctx, bad, cfg))
	bad.OptionsJSON = `{"direct":true} trailing`
	require.Error(t, ValidateConnection(ctx, bad, cfg))
	good := base
	good.OptionsJSON = `{"direct":true}`
	require.NoError(t, ValidateConnection(ctx, good, cfg))

	noAllowlist := base
	require.ErrorContains(t, ValidateConnection(ctx, noAllowlist, DefaultRuntimeConfig()), "suffix allowlist")
	ipLiteral := base
	ipLiteral.Hosts = "10.10.1.5:27017"
	require.ErrorContains(t, ValidateConnection(ctx, ipLiteral, DefaultRuntimeConfig()), "CIDR allowlist")
	loopback := base
	loopback.Hosts = "127.0.0.1:27017"
	loopbackConfig := DefaultRuntimeConfig()
	loopbackConfig.AllowLoopback = true
	require.NoError(t, ValidateConnection(ctx, loopback, loopbackConfig))
	unspecified := base
	unspecified.Hosts = "0.0.0.0:27017"
	require.ErrorContains(t, ValidateConnection(ctx, unspecified, loopbackConfig), "unspecified")

	redacted := Redact("mongodb://alice:very-secret@db.example:27017/x")
	require.NotContains(t, redacted, "alice")
	require.NotContains(t, redacted, "very-secret")
	require.Equal(t, "mongodb://db.example:27017/x", redacted)
	redacted = Redact("mongodb://alice:very-secret@db.example:27017/x?password=one&token=two&password=three")
	for _, forbidden := range []string{"alice", "very-secret", "one", "two", "three"} {
		require.NotContains(t, redacted, forbidden)
	}
	require.Equal(t, "mongodb://db.example:27017/x?password=******&token=******&password=******", redacted)
	redacted = Redact("server selection failed for mongodb+srv://bob:p%40ss@db.example/x token=raw-token")
	require.Equal(t, "server selection failed for mongodb+srv://db.example/x token=******", redacted)
}

func TestDefaultRuntimeConfigEnablesAllAccountsWithoutNetworkAccess(t *testing.T) {
	cfg := DefaultRuntimeConfig()
	require.True(t, cfg.EnabledFor(0))
	require.True(t, cfg.EnabledFor(7))
	require.False(t, cfg.EnablePerAccount)
	require.False(t, cfg.AllowLoopback)
	require.Empty(t, cfg.AllowedHostSuffixes)
	require.Empty(t, cfg.AllowedCIDRs)

	base := Connection{
		Name: "c", CredentialSecretRef: "secret://mongodb/reader",
		AuthMechanism: "SCRAM-SHA-256", TLSMode: "required",
		ReadPreference: "primary", ReadConcern: "majority",
	}
	for name, endpoint := range map[string]string{
		"hostname":       "db.mongo.example:27017",
		"literal IP":     "10.10.1.5:27017",
		"loopback":       "127.0.0.1:27017",
		"link local":     "169.254.10.20:27017",
		"cloud metadata": "100.100.100.200:27017",
	} {
		t.Run(name, func(t *testing.T) {
			connection := base
			connection.Hosts = endpoint
			require.Error(t, ValidateConnection(t.Context(), connection, cfg))
		})
	}
}

func TestDriverOptionsFailClosedOnIncompleteResolvedSecrets(t *testing.T) {
	connection := Connection{
		Name: "c", Hosts: "127.0.0.1:27017", CredentialSecretRef: "secret://env/MONGO",
		AuthMechanism: "SCRAM-SHA-256", TLSMode: "disabled", ReadPreference: "primary", ReadConcern: "majority",
	}
	cfg := DefaultRuntimeConfig()
	cfg.AllowLoopback = true
	_, err := buildClientOptions(t.Context(), connection, Credentials{}, cfg)
	require.ErrorContains(t, err, "incomplete identity")

	connection.TLSMode = "required"
	connection.TLSCASecretRef = "secret://env/MONGO_CA"
	_, err = buildClientOptions(t.Context(), connection, Credentials{Username: "reader", Password: "secret"}, cfg)
	require.ErrorContains(t, err, "empty material")
}

func TestDriverDisablesAdaptiveCursorRetries(t *testing.T) {
	connection := Connection{
		Name: "c", Hosts: "127.0.0.1:27017", CredentialSecretRef: "secret://env/MONGO",
		AuthMechanism: "SCRAM-SHA-256", TLSMode: "disabled", ReadPreference: "primary", ReadConcern: "majority",
	}
	cfg := DefaultRuntimeConfig()
	cfg.AllowLoopback = true
	clientOptions, err := buildClientOptions(
		t.Context(), connection, Credentials{Username: "reader", Password: "secret"}, cfg)
	require.NoError(t, err)
	require.NotNil(t, clientOptions.MaxAdaptiveRetries)
	require.Zero(t, *clientOptions.MaxAdaptiveRetries)
	require.NotNil(t, clientOptions.RetryReads)
	require.True(t, *clientOptions.RetryReads, "the initial find remains retryable")
}

func TestDriverSRVTLSModeIsExplicit(t *testing.T) {
	clientOptions := options.Client().SetTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12})
	err := configureTLS(t.Context(), clientOptions, "disabled", nil)
	require.NoError(t, err)
	require.Nil(t, clientOptions.TLSConfig, "tls_mode=disabled must override mongodb+srv's implicit TLS")

	err = configureTLS(t.Context(), clientOptions, "required", nil)
	require.NoError(t, err)
	require.NotNil(t, clientOptions.TLSConfig)
	require.Equal(t, uint16(tls.VersionTLS12), clientOptions.TLSConfig.MinVersion)
}

func TestEnvSecretResolverRejectsMalformedCredentials(t *testing.T) {
	ctx := context.Background()
	resolver := EnvSecretResolver{}
	t.Setenv("MO_MONGODB_ACCOUNT_1_TEST_CREDENTIAL", `{"username":"reader","password":"secret"}`)
	t.Setenv("MO_MONGODB_ACCOUNT_1_TEST_CA", "test-ca")
	credentials, err := resolver.ResolveMongoDBCredentials(
		ctx, 1, "secret://env/MO_MONGODB_ACCOUNT_1_TEST_CREDENTIAL", "secret://env/MO_MONGODB_ACCOUNT_1_TEST_CA")
	require.NoError(t, err)
	require.Equal(t, "reader", credentials.Username)
	require.Equal(t, "secret", credentials.Password)
	require.Equal(t, []byte("test-ca"), credentials.TLSCA)

	for name, raw := range map[string]string{
		"missing-password": `{"username":"reader"}`,
		"unknown-field":    `{"username":"reader","password":"secret","token":"leak"}`,
		"trailing-json":    `{"username":"reader","password":"secret"}{}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("MO_MONGODB_ACCOUNT_1_TEST_BAD_CREDENTIAL", raw)
			_, err := resolver.ResolveMongoDBCredentials(
				ctx, 1, "secret://env/MO_MONGODB_ACCOUNT_1_TEST_BAD_CREDENTIAL", "")
			require.Error(t, err)
		})
	}
	_, err = resolver.ResolveMongoDBCredentials(ctx, 1, "env://PLAINTEXT", "")
	require.Error(t, err)
	t.Setenv("MO_MONGODB_ACCOUNT_2_TEST_CREDENTIAL", `{"username":"other","password":"secret"}`)
	_, err = resolver.ResolveMongoDBCredentials(ctx, 1, "secret://env/MO_MONGODB_ACCOUNT_2_TEST_CREDENTIAL", "")
	require.Error(t, err, "one tenant must not resolve another tenant's environment-secret namespace")
}

type fakeHostResolver struct {
	addresses map[string][]net.IPAddr
	srv       []*net.SRV
}

type recordingDialer struct {
	addresses []string
}

func (d *recordingDialer) DialContext(_ context.Context, _, address string) (net.Conn, error) {
	d.addresses = append(d.addresses, address)
	return nil, nil
}

func (r fakeHostResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	return r.addresses[host], nil
}

func (r fakeHostResolver) LookupSRV(context.Context, string, string, string) (string, []*net.SRV, error) {
	return "", r.srv, nil
}

func TestResolvedEndpointPolicyRejectsDNSRebinding(t *testing.T) {
	ctx := context.Background()
	connection := Connection{
		Name: "c", Hosts: "db.mongo.example:27017", CredentialSecretRef: "secret://env/MONGO",
		AuthMechanism: "SCRAM-SHA-256", TLSMode: "required", ReadPreference: "secondaryPreferred", ReadConcern: "majority",
	}
	cfg := DefaultRuntimeConfig()
	cfg.AllowedHostSuffixes = []string{"mongo.example"}
	resolver := fakeHostResolver{addresses: map[string][]net.IPAddr{"db.mongo.example": {{IP: net.ParseIP("127.0.0.1")}}}}
	require.Error(t, ValidateResolvedEndpoints(ctx, connection, cfg, resolver))
	resolver.addresses["db.mongo.example"] = []net.IPAddr{{IP: net.ParseIP("10.10.1.5")}}
	cfg.AllowedCIDRs = []string{"10.10.0.0/16"}
	require.NoError(t, ValidateResolvedEndpoints(ctx, connection, cfg, resolver))

	srvConnection := connection
	srvConnection.Hosts = ""
	srvConnection.SRVHost = "cluster.mongo.example"
	resolver.srv = []*net.SRV{{Target: "attacker.invalid.", Port: 27017}}
	resolver.addresses["attacker.invalid"] = []net.IPAddr{{IP: net.ParseIP("10.10.1.5")}}
	require.ErrorContains(t, ValidateResolvedEndpoints(ctx, srvConnection, cfg, resolver), "hostname allowlist")
}

func TestEndpointPolicyRejectsCloudMetadataAddresses(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultRuntimeConfig()
	cfg.AllowLoopback = true
	for _, address := range []string{
		"169.254.169.254",
		"100.100.100.200",
		"fd00:ec2::254",
		"fd20:ce::254",
	} {
		t.Run(address, func(t *testing.T) {
			require.Error(t, validateHost(ctx, address, cfg))
		})
	}
}

func TestPolicyDialerValidatesDiscoveredMembersAndDialsResolvedIP(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultRuntimeConfig()
	cfg.AllowedHostSuffixes = []string{"mongo.example"}
	cfg.AllowedCIDRs = []string{"10.20.0.0/16"}
	resolver := fakeHostResolver{addresses: map[string][]net.IPAddr{
		"member.mongo.example": {{IP: net.ParseIP("10.20.1.7")}},
	}}
	base := &recordingDialer{}
	dialer := newPolicyDialer(cfg, resolver)
	dialer.base = base
	_, err := dialer.DialContext(ctx, "tcp", "member.mongo.example:27017")
	require.NoError(t, err)
	require.Equal(t, []string{"10.20.1.7:27017"}, base.addresses)

	resolver.addresses["member.mongo.example"] = []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}
	_, err = dialer.DialContext(ctx, "tcp", "member.mongo.example:27017")
	require.Error(t, err)
	require.Len(t, base.addresses, 1, "an out-of-policy rebinding must fail before socket creation")

	_, err = dialer.DialContext(ctx, "tcp", "attacker.invalid:27017")
	require.Error(t, err, "a discovered member must also satisfy the hostname allowlist")
}

func TestProjectionDocumentCollapsesParentChildPaths(t *testing.T) {
	projection := ProjectionDocument([]ColumnMapping{
		{Path: "payload.value"},
		{Path: "other"},
		{Path: "payload"},
		{Path: "payload.unit"},
	})
	require.Equal(t, bson.D{
		{Key: "other", Value: 1},
		{Key: "payload", Value: 1},
		{Key: "_id", Value: 0},
	}, projection)

	projection = ProjectionDocument([]ColumnMapping{{Path: "_id"}, {Path: "value"}})
	require.Equal(t, bson.D{{Key: "_id", Value: 1}, {Key: "value", Value: 1}}, projection)
}

type fakeFactory struct {
	mu      sync.Mutex
	clients []*fakeClient
}

type connectionResolverFunc func(context.Context, uint32, uint64, uint64) (Connection, error)

func (f connectionResolverFunc) ResolveMongoDBConnection(
	ctx context.Context, accountID uint32, connectionID uint64, version uint64,
) (Connection, error) {
	return f(ctx, accountID, connectionID, version)
}

func (f *fakeFactory) Connect(context.Context, Connection, Credentials, RuntimeConfig) (Client, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := &fakeClient{}
	f.clients = append(f.clients, c)
	return c, nil
}

type fakeClient struct {
	mu                   sync.Mutex
	disconnects          int
	disconnectContextErr error
}

func (*fakeClient) Collection(string, string) Collection { return nil }
func (*fakeClient) Ping(context.Context) error           { return nil }
func (c *fakeClient) Disconnect(ctx context.Context) error {
	c.mu.Lock()
	c.disconnects++
	c.disconnectContextErr = ctx.Err()
	c.mu.Unlock()
	return nil
}
func (c *fakeClient) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.disconnects
}

func TestClientPoolTenantIsolationRotationAndIdempotentRelease(t *testing.T) {
	ctx := context.Background()
	factory := &fakeFactory{}
	pool := NewClientPool(factory)
	base := Connection{AccountID: 1, ConnectionID: 9, Version: 1, CredentialSecretRef: "secret://v1"}
	l1, err := pool.Acquire(ctx, base, Credentials{}, RuntimeConfig{})
	require.NoError(t, err)
	l2, err := pool.Acquire(ctx, base, Credentials{}, RuntimeConfig{})
	require.NoError(t, err)
	require.Same(t, l1.Client(), l2.Client())

	tenant2 := base
	tenant2.AccountID = 2
	lTenant, err := pool.Acquire(ctx, tenant2, Credentials{}, RuntimeConfig{})
	require.NoError(t, err)
	require.NotSame(t, l1.Client(), lTenant.Client())

	rotated := base
	rotated.Version = 2
	rotated.CredentialSecretRef = "secret://v2"
	l3, err := pool.Acquire(ctx, rotated, Credentials{}, RuntimeConfig{})
	require.NoError(t, err)
	require.NotSame(t, l1.Client(), l3.Client())
	old := l1.Client().(*fakeClient)
	require.Zero(t, old.count())
	require.NoError(t, l1.Release(ctx))
	require.NoError(t, l1.Release(ctx))
	require.Zero(t, old.count())
	require.NoError(t, l2.Release(ctx))
	require.Equal(t, 1, old.count())
	require.NoError(t, l3.Release(ctx))
	require.NoError(t, lTenant.Release(ctx))
	// A released current generation stays reusable until rotation, eviction,
	// or CN shutdown, so the driver's socket pool survives statement boundaries.
	l4, err := pool.Acquire(ctx, rotated, Credentials{}, RuntimeConfig{})
	require.NoError(t, err)
	require.Same(t, l3.Client(), l4.Client())
	require.NoError(t, l4.Release(ctx))
	require.NoError(t, pool.Close(ctx))
}

type singleflightFactory struct {
	mu       sync.Mutex
	connects int
	started  chan struct{}
	release  chan struct{}
	err      error
}

func (f *singleflightFactory) Connect(context.Context, Connection, Credentials, RuntimeConfig) (Client, error) {
	f.mu.Lock()
	f.connects++
	f.mu.Unlock()
	f.started <- struct{}{}
	<-f.release
	if f.err != nil {
		return nil, f.err
	}
	return &fakeClient{}, nil
}

func TestClientPoolSingleflightsColdAcquisitionPerExactKey(t *testing.T) {
	const callers = 16
	factory := &singleflightFactory{
		started: make(chan struct{}, callers),
		release: make(chan struct{}),
	}
	pool := NewClientPool(factory)
	connection := Connection{AccountID: 1, ConnectionID: 9, Version: 1}
	type result struct {
		lease *ClientLease
		err   error
	}
	results := make(chan result, callers)
	for range callers {
		go func() {
			lease, err := pool.Acquire(t.Context(), connection, Credentials{}, RuntimeConfig{})
			results <- result{lease: lease, err: err}
		}()
	}
	<-factory.started
	for {
		pool.mu.Lock()
		flight := pool.flights[poolKey{accountID: 1, connectionID: 9, version: 1, identity: credentialIdentity(Credentials{})}]
		waiting := flight != nil && flight.waiters == callers-1
		pool.mu.Unlock()
		if waiting {
			break
		}
		runtime.Gosched()
	}
	close(factory.release)

	var first Client
	for range callers {
		acquired := <-results
		require.NoError(t, acquired.err)
		if first == nil {
			first = acquired.lease.Client()
		} else {
			require.Same(t, first, acquired.lease.Client())
		}
		require.NoError(t, acquired.lease.Release(t.Context()))
	}
	factory.mu.Lock()
	require.Equal(t, 1, factory.connects)
	factory.mu.Unlock()
	require.NoError(t, pool.Close(t.Context()))
}

func TestClientPoolSingleflightSharesColdAcquisitionFailure(t *testing.T) {
	const callers = 16
	connectErr := errors.New("injected connect failure")
	factory := &singleflightFactory{
		started: make(chan struct{}, callers),
		release: make(chan struct{}),
		err:     connectErr,
	}
	pool := NewClientPool(factory)
	results := make(chan error, callers)
	for range callers {
		go func() {
			_, err := pool.Acquire(t.Context(), Connection{AccountID: 1, ConnectionID: 9, Version: 1}, Credentials{}, RuntimeConfig{})
			results <- err
		}()
	}
	<-factory.started
	for {
		pool.mu.Lock()
		flight := pool.flights[poolKey{accountID: 1, connectionID: 9, version: 1, identity: credentialIdentity(Credentials{})}]
		waiting := flight != nil && flight.waiters == callers-1
		pool.mu.Unlock()
		if waiting {
			break
		}
		runtime.Gosched()
	}
	close(factory.release)
	for range callers {
		require.ErrorIs(t, <-results, connectErr)
	}
	factory.mu.Lock()
	require.Equal(t, 1, factory.connects)
	factory.mu.Unlock()
	require.NoError(t, pool.Close(t.Context()))
}

func TestClientPoolDetectsInPlaceSecretRotation(t *testing.T) {
	factory := &fakeFactory{}
	pool := NewClientPool(factory)
	connection := Connection{AccountID: 1, ConnectionID: 9, Version: 1, CredentialSecretRef: "secret://stable"}
	oldLease, err := pool.Acquire(t.Context(), connection, Credentials{Username: "reader", Password: "old"}, RuntimeConfig{})
	require.NoError(t, err)
	newLease, err := pool.Acquire(t.Context(), connection, Credentials{Username: "reader", Password: "new"}, RuntimeConfig{})
	require.NoError(t, err)
	require.NotSame(t, oldLease.Client(), newLease.Client())
	for key := range pool.entries {
		require.NotContains(t, key.identity, "old")
		require.NotContains(t, key.identity, "new")
		require.NotContains(t, key.identity, "reader")
	}
	require.NoError(t, oldLease.Release(t.Context()))
	require.Equal(t, 1, oldLease.Client().(*fakeClient).count())
	require.NoError(t, newLease.Release(t.Context()))
	reused, err := pool.Acquire(t.Context(), connection, Credentials{Username: "reader", Password: "new"}, RuntimeConfig{})
	require.NoError(t, err)
	require.Same(t, newLease.Client(), reused.Client())
	require.NoError(t, reused.Release(t.Context()))
	require.NoError(t, pool.Close(t.Context()))
}

func TestClientPoolBoundsIdleEntries(t *testing.T) {
	ctx := context.Background()
	factory := &fakeFactory{}
	pool := NewClientPool(factory, 1)
	first := Connection{AccountID: 1, ConnectionID: 1, Version: 1}
	lease1, err := pool.Acquire(ctx, first, Credentials{}, RuntimeConfig{})
	require.NoError(t, err)
	client1 := lease1.Client().(*fakeClient)
	require.NoError(t, lease1.Release(ctx))
	require.Zero(t, client1.count())

	second := first
	second.ConnectionID = 2
	lease2, err := pool.Acquire(ctx, second, Credentials{}, RuntimeConfig{})
	require.NoError(t, err)
	require.Equal(t, 1, client1.count())
	require.Len(t, pool.entries, 1)
	require.NoError(t, lease2.Release(ctx))
	require.Len(t, pool.entries, 1)
	require.NoError(t, pool.Close(ctx))
}

func TestClientPoolRetiresCommittedOldGeneration(t *testing.T) {
	factory := &fakeFactory{}
	pool := NewClientPool(factory)
	connection := Connection{AccountID: 1, ConnectionID: 9, Version: 3}

	idle, err := pool.Acquire(t.Context(), connection, Credentials{}, RuntimeConfig{})
	require.NoError(t, err)
	idleClient := idle.Client().(*fakeClient)
	require.NoError(t, idle.Release(t.Context()))
	require.NoError(t, pool.RetireBefore(1, 9, 4))
	require.Equal(t, 1, idleClient.count(), "an idle replaced generation must disconnect immediately")

	connection.Version = 4
	active, err := pool.Acquire(t.Context(), connection, Credentials{}, RuntimeConfig{})
	require.NoError(t, err)
	activeClient := active.Client().(*fakeClient)
	require.NoError(t, pool.RetireBefore(1, 9, 5))
	require.Zero(t, activeClient.count(), "an active generation must remain valid until its lease releases")
	require.NoError(t, active.Release(t.Context()))
	require.Equal(t, 1, activeClient.count())

	connection.Version = 5
	dropped, err := pool.Acquire(t.Context(), connection, Credentials{}, RuntimeConfig{})
	require.NoError(t, err)
	droppedClient := dropped.Client().(*fakeClient)
	require.NoError(t, dropped.Release(t.Context()))
	require.NoError(t, pool.RetireConnection(1, 9))
	require.Equal(t, 1, droppedClient.count())
	require.NoError(t, pool.Close(t.Context()))
}

func TestClientPoolLateGenerationObservesRetirementTombstone(t *testing.T) {
	for _, tc := range []struct {
		name    string
		retire  func(*ClientPool) error
		version uint64
		connID  uint64
	}{
		{
			name: "alter",
			retire: func(pool *ClientPool) error {
				return pool.RetireBefore(4, 8, 2)
			},
			version: 1,
			connID:  8,
		},
		{
			name: "drop",
			retire: func(pool *ClientPool) error {
				return pool.RetireConnection(4, 9)
			},
			version: 1,
			connID:  9,
		},
		{
			name: "drop account",
			retire: func(pool *ClientPool) error {
				return pool.RetireAccount(4)
			},
			version: 1,
			connID:  10,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allow := make(chan struct{})
			factory := &blockingFactory{
				started: make(chan uint64, 1),
				allow:   map[uint64]chan struct{}{tc.version: allow},
			}
			pool := NewClientPool(factory)
			type acquireResult struct {
				lease *ClientLease
				err   error
			}
			result := make(chan acquireResult, 1)
			go func() {
				lease, err := pool.Acquire(t.Context(), Connection{
					AccountID: 4, ConnectionID: tc.connID, Version: tc.version,
				}, Credentials{}, RuntimeConfig{})
				result <- acquireResult{lease: lease, err: err}
			}()
			require.Equal(t, tc.version, <-factory.started)
			require.NoError(t, tc.retire(pool))
			close(allow)
			acquired := <-result
			require.NoError(t, acquired.err)
			require.NotNil(t, acquired.lease)
			client := acquired.lease.Client().(*fakeClient)
			require.Zero(t, client.count(), "an already resolved statement may finish its lease")
			require.NoError(t, acquired.lease.Release(t.Context()))
			require.Equal(t, 1, client.count(), "a late retired client must not remain idle or reusable")
			pool.mu.Lock()
			require.Empty(t, pool.retirements, "retirement tombstones must be reclaimed after the race drains")
			require.Empty(t, pool.connecting)
			pool.mu.Unlock()
			require.NoError(t, pool.Close(t.Context()))
		})
	}
}

func TestValidatedClientPoolClosesPreAcquireGenerationRace(t *testing.T) {
	factory := &fakeFactory{}
	validator := connectionResolverFunc(func(ctx context.Context, _ uint32, _ uint64, _ uint64) (Connection, error) {
		return Connection{}, moerr.NewInvalidInput(ctx, "stale generation")
	})
	pool := NewValidatedClientPool(factory, validator)

	// With no pool Acquire in flight, retirement metadata can be reclaimed.
	// A statement that resolved v1 before this point but reaches Acquire only
	// afterward is still rejected by the second catalog validation, before any
	// socket pool is created.
	require.NoError(t, pool.RetireBefore(4, 8, 2))
	require.Empty(t, pool.retirements)
	_, err := pool.Acquire(t.Context(), Connection{
		AccountID: 4, ConnectionID: 8, Version: 1,
	}, Credentials{}, RuntimeConfig{})
	require.ErrorContains(t, err, "stale generation")
	require.Empty(t, factory.clients)
	require.Empty(t, pool.connecting)
	require.NoError(t, pool.Close(t.Context()))
}

func TestClientPoolCleanupDoesNotInheritCanceledCaller(t *testing.T) {
	factory := &fakeFactory{}
	pool := NewClientPool(factory)
	lease, err := pool.Acquire(context.Background(), Connection{AccountID: 1, ConnectionID: 1, Version: 1}, Credentials{}, RuntimeConfig{})
	require.NoError(t, err)
	require.NoError(t, lease.Release(context.Background()))
	client := lease.Client().(*fakeClient)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, pool.Close(ctx))
	client.mu.Lock()
	defer client.mu.Unlock()
	require.Equal(t, 1, client.disconnects)
	require.NoError(t, client.disconnectContextErr)
}

type failingFactory struct{}

func (failingFactory) Connect(context.Context, Connection, Credentials, RuntimeConfig) (Client, error) {
	return nil, errors.New("connect failed")
}

func TestClientPoolAcquireFailureDoesNotPublishEntry(t *testing.T) {
	pool := NewClientPool(failingFactory{})
	_, err := pool.Acquire(context.Background(), Connection{}, Credentials{}, RuntimeConfig{})
	require.Error(t, err)
	require.Empty(t, pool.entries)
}

type blockingFactory struct {
	started chan uint64
	allow   map[uint64]chan struct{}
}

func (f *blockingFactory) Connect(_ context.Context, connection Connection, _ Credentials, _ RuntimeConfig) (Client, error) {
	f.started <- connection.Version
	<-f.allow[connection.Version]
	return &fakeClient{}, nil
}

func TestClientPoolLateOldGenerationCannotDrainNewGeneration(t *testing.T) {
	allowV1 := make(chan struct{})
	defer func() {
		select {
		case <-allowV1:
		default:
			close(allowV1)
		}
	}()
	allowV2 := make(chan struct{})
	close(allowV2)
	factory := &blockingFactory{
		started: make(chan uint64, 2),
		allow:   map[uint64]chan struct{}{1: allowV1, 2: allowV2},
	}
	pool := NewClientPool(factory)
	type result struct {
		lease *ClientLease
		err   error
	}
	oldResult := make(chan result, 1)
	go func() {
		lease, err := pool.Acquire(t.Context(), Connection{AccountID: 1, ConnectionID: 9, Version: 1}, Credentials{}, RuntimeConfig{})
		oldResult <- result{lease: lease, err: err}
	}()
	require.Equal(t, uint64(1), <-factory.started)

	newLease, err := pool.Acquire(t.Context(), Connection{AccountID: 1, ConnectionID: 9, Version: 2}, Credentials{}, RuntimeConfig{})
	require.NoError(t, err)
	require.Equal(t, uint64(2), <-factory.started)
	close(allowV1)
	old := <-oldResult
	require.NoError(t, old.err)
	require.NotSame(t, old.lease.Client(), newLease.Client())

	reusedNew, err := pool.Acquire(t.Context(), Connection{AccountID: 1, ConnectionID: 9, Version: 2}, Credentials{}, RuntimeConfig{})
	require.NoError(t, err)
	require.Same(t, newLease.Client(), reusedNew.Client(), "a late old Connect must not retire the current generation")
	require.NoError(t, old.lease.Release(t.Context()))
	require.Equal(t, 1, old.lease.Client().(*fakeClient).count())
	require.NoError(t, newLease.Release(t.Context()))
	require.NoError(t, reusedNew.Release(t.Context()))
	require.NoError(t, pool.Close(t.Context()))
}
