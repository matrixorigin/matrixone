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

package plan

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	sqlkafka "github.com/matrixorigin/matrixone/pkg/sql/kafka"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// The regression fixtures here never hand-write a durable rel_createsql. Every
// JSON fixture is either produced by running a real CREATE EXTERNAL TABLE
// statement through the parser and the create plan (durableExternalCreateSQL
// returns exactly the string stored in the catalog SystemRelAttr_CreateSQL
// property), or by json.Marshal of the tree.ExternParam the create path
// serializes for the stage/S3 shapes (marshalExternParam). The shape reported
// in issue #31920 keeps the file in Option; the INFILE string shape keeps it
// in Filepath (mysql_sql.y load_param_opt); both must classify as a read-only
// file mapping.

// externalTableDef builds the planner-facing table definition whose Createsql
// is a durable catalog string produced above.
func externalTableDef(name, createSQL string) *TableDef {
	return &TableDef{
		Name:      name,
		TableType: catalog.SystemExternalRel,
		Createsql: createSQL,
		Cols: []*ColDef{
			{Name: "id", Typ: Type{Id: int32(types.T_int64)}},
		},
	}
}

// durableExternalCreateSQL plans a real CREATE EXTERNAL TABLE statement and
// returns the exact durable SystemRelAttr_CreateSQL property it stores.
func durableExternalCreateSQL(t *testing.T, ddl string) string {
	t.Helper()
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, ddl, 1)
	require.NoError(t, err)
	defer stmt.Free()

	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	p, err := BuildPlan(ctx, stmt, false, nil)
	require.NoError(t, err)
	for _, def := range p.GetDdl().GetCreateTable().GetTableDef().GetDefs() {
		for _, kv := range def.GetProperties().GetProperties() {
			if kv.GetKey() == catalog.SystemRelAttr_CreateSQL {
				return kv.GetValue()
			}
		}
	}
	t.Fatalf("create plan for %q stores no durable CreateSQL property", ddl)
	return ""
}

// marshalExternParam reproduces the durable JSON exactly the way the create
// path serializes an already-built parser ExternParam (json.Marshal of the
// whole stmt.Param), for shapes like stage/S3 whose full SQL spellings live in
// their own grammar branches.
func marshalExternParam(t *testing.T, param *tree.ExternParam) string {
	t.Helper()
	raw, err := json.Marshal(param)
	require.NoError(t, err)
	return string(raw)
}

type readonlyFileShape struct {
	name      string
	tableName string
	createSQL string
}

// durableReadOnlyFileShapes returns the legal read-only file-mapping durable
// shapes that reach TRUNCATE/ALTER admission: the reported
// INFILE{'filepath'=...} option-only dialect for csv/jsonline/parquet, the
// normalized INFILE string dialect, and the stage / S3 persistent shapes.
func durableReadOnlyFileShapes(t *testing.T) []readonlyFileShape {
	t.Helper()
	shapes := make([]readonlyFileShape, 0, 6)
	for _, format := range []string{"csv", "jsonline", "parquet"} {
		ddl := fmt.Sprintf(
			"create external table ro_%s (id int, s varchar(16)) infile{'filepath'='/data/ro_%s.flat','format'='%s'}",
			format, format, format)
		shapes = append(shapes, readonlyFileShape{
			name:      "infile-option/" + format,
			tableName: "ro_" + format,
			createSQL: durableExternalCreateSQL(t, ddl),
		})
	}
	// The parser normalizes INFILE '...' into Filepath plus csv defaults.
	shapes = append(shapes, readonlyFileShape{
		name:      "infile-string/csv",
		tableName: "ro_infile_string",
		createSQL: durableExternalCreateSQL(t,
			"create external table ro_infile_string (id int, s varchar(16)) infile '/data/ro_infile_string.flat'"),
	})
	// Stage mapping: the durable JSON carries StageName instead of a file path.
	shapes = append(shapes, readonlyFileShape{
		name:      "stage/url",
		tableName: "ro_stage",
		createSQL: marshalExternParam(t, &tree.ExternParam{ExParamConst: tree.ExParamConst{
			StageName: "ro_stage",
		}}),
	})
	// S3 mapping: the durable JSON keeps the s3 endpoints in Option next to
	// the scanned object filepath.
	shapes = append(shapes, readonlyFileShape{
		name:      "s3/url",
		tableName: "ro_s3",
		createSQL: marshalExternParam(t, &tree.ExternParam{ExParamConst: tree.ExParamConst{
			ScanType: tree.S3,
			Option: []string{
				"filepath", "bucket-key.flat",
				"endpoint", "http://127.0.0.1",
				"region", "r",
				"bucket", "b",
				"access_key_id", "k",
				"secret_access_key", "s",
				"format", "csv",
			},
		}}),
	})
	return shapes
}

// TestDurableCreatesqlKeepsReportedInfileShapeInOption pins the producer fact
// behind issue #31920: the plain external CREATE serializes stmt.Param as the
// durable CreateSQL without normalizing the INFILE{'filepath'=...} option into
// ExternParam.Filepath, so admission must read the effective file path exactly
// like the read side does.
func TestDurableCreatesqlKeepsReportedInfileShapeInOption(t *testing.T) {
	for _, format := range []string{"csv", "jsonline", "parquet"} {
		t.Run(format, func(t *testing.T) {
			ddl := fmt.Sprintf(
				"create external table ro_%s (id int, s varchar(16)) infile{'filepath'='/data/ro_%s.flat','format'='%s'}",
				format, format, format)
			raw := durableExternalCreateSQL(t, ddl)
			require.Contains(t, raw, `"Option"`, raw)

			param := &tree.ExternParam{}
			require.NoError(t, json.Unmarshal([]byte(raw), param))
			require.Empty(t, param.Filepath, "durable INFILE{} shape keeps the path only in Option")
			require.Equal(t, "/data/ro_"+format+".flat", GetFilePathFromParam(param))
		})
	}
}

// TestBuildTruncateReadOnlyFileExternalRejectedAcrossDurableShapes is the
// issue #31920 regression: every legal read-only file-mapping durable shape
// must fail TRUNCATE with the explicit read-only decision instead of the
// historical silent no-op plan.
func TestBuildTruncateReadOnlyFileExternalRejectedAcrossDurableShapes(t *testing.T) {
	for _, shape := range durableReadOnlyFileShapes(t) {
		t.Run(shape.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
				"truncate table "+shape.tableName, 1)
			require.NoError(t, err)
			defer stmt.Free()

			ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
			ctx.objects[shape.tableName] = &ObjectRef{SchemaName: "tpch", ObjName: shape.tableName}
			ctx.tables[shape.tableName] = externalTableDef(shape.tableName, shape.createSQL)

			p, err := BuildPlan(ctx, stmt, false, nil)
			require.Error(t, err,
				"TRUNCATE on the read-only file mapping %q must not produce plan %v", shape.name, p)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)
			require.Contains(t, err.Error(), "truncate read-only external table '"+shape.tableName+"'")
		})
	}
}

// TestBuildTruncateWritableFileExternalOptionShapeKeepsDedicatedError keeps
// the pre-existing writable admission in the option dialect: WRITE_FILE_PATTERN
// grants INSERT/LOAD but never TRUNCATE, with the historical message.
func TestBuildTruncateWritableFileExternalOptionShapeKeepsDedicatedError(t *testing.T) {
	createSQL := marshalExternParam(t, &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Filepath: "/data/wo.flat",
		Option: []string{
			"format", "csv",
			"write_file_pattern", "stage://bkt/out/wo-%U.csv",
		},
	}})
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "truncate table wo", 1)
	require.NoError(t, err)
	defer stmt.Free()

	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	ctx.objects["wo"] = &ObjectRef{SchemaName: "tpch", ObjName: "wo"}
	ctx.tables["wo"] = externalTableDef("wo", createSQL)

	_, err = BuildPlan(ctx, stmt, false, nil)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)
	require.Contains(t, err.Error(),
		"truncate writable external table 'wo'; its files in the stage are not managed by the table")
}

// newKafkaEnvelopeExternalTable returns a Kafka external mapping. Its
// rel_createsql is the Kafka comment envelope, not file-scan JSON; it must
// keep the historical generic no-op TRUNCATE contract.
func newKafkaEnvelopeExternalTable(name string) *TableDef {
	return &TableDef{
		Name:        name,
		TableType:   catalog.SystemExternalRel,
		FeatureFlag: features.KafkaExternal,
		Createsql: sqlkafka.BuildCreateSQLEnvelope(sqlkafka.Config{
			Brokers: "kafka.invalid:9092",
			Topic:   name,
			Group:   "probe",
			Format:  "csv",
		}),
		Cols: []*ColDef{
			{Name: "id", Typ: Type{Id: int32(types.T_int64)}},
		},
	}
}

// TestBuildTruncateKafkaExternalTableKeepsNoOpPlan pins the neighboring
// control: envelope-backed mappings that today fall through the generic
// external no-op must keep building a truncate plan. Kafka is the riskiest
// confusion candidate for a file classification, so it is asserted explicitly.
func TestBuildTruncateKafkaExternalTableKeepsNoOpPlan(t *testing.T) {
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "truncate table ext_kafka", 1)
	require.NoError(t, err)
	defer stmt.Free()

	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	ctx.objects["ext_kafka"] = &ObjectRef{SchemaName: "tpch", ObjName: "ext_kafka"}
	ctx.tables["ext_kafka"] = newKafkaEnvelopeExternalTable("ext_kafka")

	p, err := BuildPlan(ctx, stmt, false, nil)
	require.NoError(t, err)
	require.NotNil(t, p.GetDdl().GetTruncateTable())
}

// TestBuildAlterReadOnlyFileExternalReportsAlterContractAcrossShapes pins the
// second half of issue #31920 in the durable producer shape: ALTER on a
// read-only file mapping must fail with a diagnostic that names the
// unsupported ALTER, not the generic insert/update/delete DML error that the
// copy-based path emits.
func TestBuildAlterReadOnlyFileExternalReportsAlterContractAcrossShapes(t *testing.T) {
	for _, shape := range durableReadOnlyFileShapes(t) {
		for _, sql := range []string{
			"alter table " + shape.tableName + " add column extra int",
			"alter table " + shape.tableName + " modify column id varchar(16)",
			"alter table " + shape.tableName + " change column id id2 varchar(16)",
			"alter table " + shape.tableName + " rename to " + shape.tableName + "_x",
		} {
			name := shape.name + "/" + sql
			t.Run(name, func(t *testing.T) {
				stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
				require.NoError(t, err)
				defer stmt.Free()

				ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
				ctx.objects[shape.tableName] = &ObjectRef{SchemaName: "tpch", ObjName: shape.tableName}
				ctx.tables[shape.tableName] = externalTableDef(shape.tableName, shape.createSQL)

				_, err = BuildPlan(ctx, stmt, false, nil)
				require.Error(t, err)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)
				require.Contains(t, err.Error(), "ALTER TABLE on a read-only external table")
			})
		}
	}
}

// TestBuildAlterKafkaExternalTableKeepsExistingError keeps the existing
// envelope-backed ALTER admission untouched.
func TestBuildAlterKafkaExternalTableKeepsExistingError(t *testing.T) {
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "alter table ext_kafka add column extra int", 1)
	require.NoError(t, err)
	defer stmt.Free()

	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	ctx.objects["ext_kafka"] = &ObjectRef{SchemaName: "tpch", ObjName: "ext_kafka"}
	ctx.tables["ext_kafka"] = newKafkaEnvelopeExternalTable("ext_kafka")

	_, err = BuildPlan(ctx, stmt, false, nil)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)
	require.Contains(t, err.Error(),
		"ALTER TABLE on a kafka external table; drop and recreate the external table to change its schema")
}

// TestBuildAlterReadOnlyFileExternalDelegatesIndexSubactions keeps the
// read-only file front guard failing closed while constraint/index ALTER
// sub-actions keep the established cannot-create-index diagnostic instead of
// falling through to the data-copy route rejection (issue #31920).
func TestBuildAlterReadOnlyFileExternalDelegatesIndexSubactions(t *testing.T) {
	for _, shape := range durableReadOnlyFileShapes(t) {
		run := func(sql string, wantIndexErr bool, moErrCode uint16, fragment string) {
			name := shape.name + "/" + sql
			t.Run(name, func(t *testing.T) {
				stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
				require.NoError(t, err)
				defer stmt.Free()

				ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
				ctx.objects[shape.tableName] = &ObjectRef{SchemaName: "tpch", ObjName: shape.tableName}
				def := externalTableDef(shape.tableName, shape.createSQL)
				ctx.tables[shape.tableName] = def

				_, err = BuildPlan(ctx, stmt, false, nil)
				require.Error(t, err)
				require.True(t, moerr.IsMoErrCode(err, moErrCode), "%v", err)
				require.Contains(t, err.Error(), fragment)
				require.Same(t, def, ctx.tables[shape.tableName])
			})
		}
		for _, sql := range []string{
			"alter table " + shape.tableName + " add constraint primary key (id)",
			"alter table " + shape.tableName + " add unique (id)",
			"alter table " + shape.tableName + " add index idx_x (id)",
			"alter table " + shape.tableName + " add column extra2 int primary key",
			"alter table " + shape.tableName + " add column extra3 int unique",
			"alter table " + shape.tableName + " add column extra4 int primary key, rename column id to id4",
		} {
			run(sql, true, moerr.ErrInvalidInput, "cannot create index on external table")
		}
		run("alter table "+shape.tableName+" rename column id to idx", false,
			moerr.ErrNotSupported, "ALTER TABLE on a read-only external table")
	}
}
