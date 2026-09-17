// Copyright 2021 Matrix Origin
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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

type namedSnapshotViewContext struct {
	*rootSQLCompilerContext
	snapshot *Snapshot
}

type physicalOwnerViewContext struct {
	*rootSQLCompilerContext
	accountID uint32
}

func TestPersistedDecimalLiteralViewProtocolLifecycle(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	proc := ctx.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldProtocol, hadProtocol := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldReadFloor, hadReadFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor)
	oldAuthoringFloor, hadAuthoringFloor := rt.GetGlobalVariables(
		moruntime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if hadProtocol {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldProtocol)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		if hadReadFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, oldReadFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolFloor, current)
		}
		if hadAuthoringFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, oldAuthoringFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, current)
		}
	})

	const createSQL = "create view v_decimal_literal as select 12345678901234567890123456789012345678.1 as n"
	parseAndBuild := func(sql string) (*Plan, error) {
		root := &rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: sql}
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		return BuildPlan(root, stmt, false)
	}

	// A CN that can execute v81 but has not passed the v84 authoring barrier
	// must not publish a view whose literal would be rebound differently by an
	// older planner.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion81))
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, int64(defines.MORPCVersion81))
	_, err := parseAndBuild(createSQL)
	require.ErrorContains(t, err, "protocol version 84")

	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion84))
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, int64(defines.MORPCVersion84))
	created, err := parseAndBuild(createSQL)
	require.NoError(t, err)
	createdView := created.GetDdl().GetCreateView().GetTableDef()
	var createdData ViewData
	require.NoError(t, json.Unmarshal([]byte(createdView.GetViewSql().GetView()), &createdData))
	require.NotNil(t, createdData.RequiredProtocolVersion)
	require.Equal(t, int64(defines.MORPCVersion84), *createdData.RequiredProtocolVersion)

	// The compact IN-vector path carries the same aggregate marker. Keep this
	// as a planner-level check so the compatibility fence does not regress into
	// a slow structured-list fallback for ordinary decimal predicates.
	const createInSQL = "create view v_decimal_in as select n_name from nation where n_regionkey in (0.1, 0.2)"
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion81))
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, int64(defines.MORPCVersion81))
	_, err = parseAndBuild(createInSQL)
	require.ErrorContains(t, err, "protocol version 84")
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion84))
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, int64(defines.MORPCVersion84))
	createdIn, err := parseAndBuild(createInSQL)
	require.NoError(t, err)
	var createdInData ViewData
	require.NoError(t, json.Unmarshal([]byte(createdIn.GetDdl().GetCreateView().GetTableDef().GetViewSql().GetView()), &createdInData))
	require.NotNil(t, createdInData.RequiredProtocolVersion)
	require.Equal(t, int64(defines.MORPCVersion84), *createdInData.RequiredProtocolVersion)

	// The same SQL without a historical marker is a legacy view. Regeneration
	// and direct expansion both rebind it first, then discover and fence v84.
	var markerlessFields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(createdView.GetViewSql().GetView()), &markerlessFields))
	delete(markerlessFields, "required_protocol_version")
	markerlessBytes, err := json.Marshal(markerlessFields)
	require.NoError(t, err)
	markerless := string(markerlessBytes)

	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion81))
	_, err = RegenerateViewDefinition(ctx, markerless)
	require.ErrorContains(t, err, "protocol version 84")

	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion84))
	regenerated, err := RegenerateViewDefinition(ctx, markerless)
	require.NoError(t, err)
	var regeneratedData ViewData
	require.NoError(t, json.Unmarshal([]byte(regenerated.TableDef.GetViewSql().GetView()), &regeneratedData))
	require.NotNil(t, regeneratedData.RequiredProtocolVersion)
	require.Equal(t, int64(defines.MORPCVersion84), *regeneratedData.RequiredProtocolVersion)

	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion81))
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, true, false)
	bindCtx := NewBindContext(builder, nil)
	viewDef := &TableDef{ViewSql: &planpb.ViewDef{View: markerless}}
	_, err = builder.bindView(bindCtx, viewDef, nil, &ObjectRef{}, "tpch", "v_decimal_literal", nil)
	require.ErrorContains(t, err, "protocol version 84")

	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion84))
	builder = NewQueryBuilder(planpb.Query_SELECT, ctx, true, false)
	bindCtx = NewBindContext(builder, nil)
	_, err = builder.bindView(bindCtx, viewDef, nil, &ObjectRef{}, "tpch", "v_decimal_literal", nil)
	require.NoError(t, err)
}

func (c *physicalOwnerViewContext) ResolveViewDependencyAccount(
	*ObjectRef, *TableDef, *Snapshot,
) (uint32, error) {
	return c.accountID, nil
}

func (c *namedSnapshotViewContext) ResolveSnapshotWithSnapshotName(
	_ string,
) (*Snapshot, error) {
	return DeepCopySnapshot(c.snapshot), nil
}

func TestAuthoritativeViewGenerationCapturesDirectDependency(t *testing.T) {
	const rootSQL = "create view v as select n_nationkey from nation"
	ctx := &rootSQLCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		rootSQL:             rootSQL,
	}
	ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }
	ctx.tables["nation"].DbId = 7
	ctx.tables["nation"].TblId = 11
	ctx.tables["nation"].LogicalId = 13
	ctx.tables["nation"].Version = 17

	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	var data ViewData
	require.NoError(t, json.Unmarshal(
		[]byte(p.GetDdl().GetCreateView().GetTableDef().GetViewSql().GetView()), &data))
	require.Equal(t, []ViewDependency{{
		AccountID:           42,
		DatabaseID:          7,
		RelationID:          11,
		LogicalID:           13,
		DatabaseName:        "tpch",
		RelationName:        "nation",
		BindingDatabaseName: "tpch",
		BindingRelationName: "nation",
		RelationKind:        catalog.SystemOrdinaryRel,
		Version:             17,
		LowerCaseTableNames: 1,
	}}, data.Dependencies)
}

func TestViewDependencyPhysicalOwnerSurvivesRegeneration(t *testing.T) {
	const rootSQL = "create view v as select n_nationkey from nation"
	ctx := &physicalOwnerViewContext{
		rootSQLCompilerContext: &rootSQLCompilerContext{
			MockCompilerContext: NewMockCompilerContext(false),
			rootSQL:             rootSQL,
		},
		accountID: 0,
	}
	ctx.GetAccountIdFunc = func() (uint32, error) { return 7, nil }

	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()
	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	persisted := p.GetDdl().GetCreateView().GetTableDef().GetViewSql().GetView()

	var created ViewData
	require.NoError(t, json.Unmarshal([]byte(persisted), &created))
	require.Len(t, created.Dependencies, 1)
	require.Equal(t, uint32(0), created.Dependencies[0].AccountID)

	regenerated, err := RegenerateViewDefinition(ctx, persisted)
	require.NoError(t, err)
	require.Len(t, regenerated.Dependencies, 1)
	require.Equal(t, uint32(0), regenerated.Dependencies[0].AccountID)
}

func TestAuthoritativeViewGenerationCapturesViewNotItsSources(t *testing.T) {
	const rootSQL = "create view v2 as select n_nationkey from v1"
	ctx := &rootSQLCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		rootSQL:             rootSQL,
	}
	ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }
	viewJSON, err := json.Marshal(ViewData{
		Stmt:            "create view v1 as select n_nationkey from nation",
		DefaultDatabase: "tpch",
	})
	require.NoError(t, err)
	ctx.objects["v1"] = &planpb.ObjectRef{SchemaName: "tpch", ObjName: "v1", Obj: 101}
	ctx.tables["v1"] = &planpb.TableDef{
		DbId:      7,
		TblId:     101,
		LogicalId: 103,
		Version:   5,
		Name:      "v1",
		TableType: catalog.SystemViewRel,
		Cols:      DeepCopyColDefList(ctx.tables["nation"].Cols),
		ViewSql:   &planpb.ViewDef{View: string(viewJSON)},
	}

	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()
	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)

	var data ViewData
	require.NoError(t, json.Unmarshal(
		[]byte(p.GetDdl().GetCreateView().GetTableDef().GetViewSql().GetView()), &data))
	require.Len(t, data.Dependencies, 1)
	require.Equal(t, "v1", data.Dependencies[0].RelationName)
	require.Equal(t, uint64(101), data.Dependencies[0].RelationID)
	require.Equal(t, uint64(103), data.Dependencies[0].LogicalID)
}

func TestAuthoritativeViewGenerationCapturesSnapshotBinding(t *testing.T) {
	const rootSQL = "create view v as select n_nationkey from nation {snapshot = 'daily'}"
	snapshot := &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 123}}
	ctx := &namedSnapshotViewContext{
		rootSQLCompilerContext: &rootSQLCompilerContext{
			MockCompilerContext: NewMockCompilerContext(false), rootSQL: rootSQL,
		},
		snapshot: snapshot,
	}
	ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }

	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()
	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)

	var data ViewData
	require.NoError(t, json.Unmarshal(
		[]byte(p.GetDdl().GetCreateView().GetTableDef().GetViewSql().GetView()), &data))
	require.Len(t, data.Dependencies, 1)
	require.Equal(t, "daily", data.Dependencies[0].SnapshotName)
	require.Equal(t, snapshot, data.Dependencies[0].Snapshot)
}

func TestRegenerateViewDefinitionUsesAuthoritativeGeneratorAndPreservesJSON(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }
	ctx.tables["nation"].DbId = 7
	ctx.tables["nation"].TblId = 11
	ctx.tables["nation"].LogicalId = 13
	ctx.tables["nation"].Cols[1].Typ.Width = 60
	persisted := `{"Stmt":"create view v as select n_name from nation",` +
		`"DefaultDatabase":"tpch","security_type":"DEFINER",` +
		`"required_protocol_version":72,` +
		`"future_field":{"keep":true}}`

	regenerated, err := RegenerateViewDefinition(ctx, persisted)
	require.NoError(t, err)
	require.Len(t, regenerated.TableDef.Cols, 1)
	require.Equal(t, int32(60), regenerated.TableDef.Cols[0].Typ.Width)
	require.Len(t, regenerated.Dependencies, 1)

	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(regenerated.TableDef.ViewSql.View), &fields))
	require.JSONEq(t, `{"keep":true}`, string(fields["future_field"]))
	require.JSONEq(t, `"create view v as select n_name from nation"`, string(fields["Stmt"]))
	require.JSONEq(t, `72`, string(fields["required_protocol_version"]))
	require.Contains(t, fields, "dependencies")
	require.Contains(t, fields, "lower_case_table_names")
}

func TestRegenerateViewDefinitionUsesReadFloorDuringAuthoringBarrier(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	proc := ctx.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldProtocol, hadProtocol := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldReadFloor, hadReadFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor)
	oldAuthoringFloor, hadAuthoringFloor := rt.GetGlobalVariables(
		moruntime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if hadProtocol {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldProtocol)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		if hadReadFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, oldReadFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolFloor, current)
		}
		if hadAuthoringFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, oldAuthoringFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, current)
		}
	})

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	// During phase one, existing persisted definitions must be readable while
	// new protocol-bearing definitions remain blocked until catalog fencing.
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion72))
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, int64(0))
	persisted := `{"Stmt":"create view v as select inet_ntoa(1)",` +
		`"DefaultDatabase":"tpch","required_protocol_version":72}`

	regenerated, err := RegenerateViewDefinition(ctx, persisted)
	require.NoError(t, err)
	require.NotNil(t, regenerated)
	require.NotNil(t, regenerated.TableDef)
	require.NotNil(t, regenerated.TableDef.ViewSql)

	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"create view v as select inet_ntoa(1)", 1)
	require.NoError(t, err)
	defer stmt.Free()
	_, err = BuildPlan(ctx, stmt, false)
	require.ErrorContains(t, err, "protocol version 72")
}

func TestPersistedStringNumericViewProtocolLifecycle(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	proc := ctx.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldProtocol, hadProtocol := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldReadFloor, hadReadFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor)
	oldAuthoringFloor, hadAuthoringFloor := rt.GetGlobalVariables(
		moruntime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if hadProtocol {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldProtocol)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		if hadReadFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, oldReadFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolFloor, current)
		}
		if hadAuthoringFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, oldAuthoringFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, current)
		}
	})

	const createSQL = "create view v_string_numeric as select char_length(n_name) as n from nation"
	const alterSQL = "alter view v_string_numeric as select char_length(n_name) as n from nation"
	parse := func(sql string) tree.Statement {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		return stmt
	}
	build := func(sql string, authoringFloor int64) (*Plan, error) {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, authoringFloor)
		root := &rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: sql}
		stmt := parse(sql)
		defer stmt.Free()
		return BuildPlan(root, stmt, false)
	}

	// CREATE and ALTER both pass through the authoring gate. A CN that has
	// only admitted the previous v79 contract must not publish v80 metadata.
	_, err := build(createSQL, defines.MORPCVersion79)
	require.ErrorContains(t, err, "protocol version 80")
	created, err := build(createSQL, defines.MORPCVersion80)
	require.NoError(t, err)
	createdView := created.GetDdl().GetCreateView().GetTableDef()
	var createdData ViewData
	require.NoError(t, json.Unmarshal([]byte(createdView.GetViewSql().GetView()), &createdData))
	require.NotNil(t, createdData.RequiredProtocolVersion)
	require.Equal(t, int64(defines.MORPCVersion80), *createdData.RequiredProtocolVersion)

	// Make the successfully authored definition visible to ALTER VIEW through
	// the mock catalog, then exercise the same write-side fence on replacement.
	ctx.tables["v_string_numeric"] = DeepCopyTableDef(createdView, true)
	ctx.objects["v_string_numeric"] = &planpb.ObjectRef{
		SchemaName: "tpch", ObjName: "v_string_numeric",
	}
	_, err = build(alterSQL, defines.MORPCVersion79)
	require.ErrorContains(t, err, "protocol version 80")
	altered, err := build(alterSQL, defines.MORPCVersion80)
	require.NoError(t, err)
	var alteredData ViewData
	require.NoError(t, json.Unmarshal(
		[]byte(altered.GetDdl().GetAlterView().GetTableDef().GetViewSql().GetView()), &alteredData))
	require.NotNil(t, alteredData.RequiredProtocolVersion)
	require.Equal(t, int64(defines.MORPCVersion80), *alteredData.RequiredProtocolVersion)

	// Regeneration is the read-side boundary. Start with a lower historical
	// marker and an unknown field: generation must raise the marker to the
	// newly required v80 floor while preserving unrelated JSON metadata.
	var persistedFields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(createdView.GetViewSql().GetView()), &persistedFields))
	persistedFields["required_protocol_version"] = json.RawMessage("79")
	persistedFields["future_field"] = json.RawMessage(`{"keep":true}`)
	persistedBytes, err := json.Marshal(persistedFields)
	require.NoError(t, err)
	persisted := string(persistedBytes)

	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, defines.MORPCVersion79)
	_, err = RegenerateViewDefinition(ctx, persisted)
	require.ErrorContains(t, err, "protocol version 80")

	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, defines.MORPCVersion80)
	regenerated, err := RegenerateViewDefinition(ctx, persisted)
	require.NoError(t, err)
	var regeneratedFields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(
		[]byte(regenerated.TableDef.GetViewSql().GetView()), &regeneratedFields))
	require.JSONEq(t, `80`, string(regeneratedFields["required_protocol_version"]))
	require.JSONEq(t, `{"keep":true}`, string(regeneratedFields["future_field"]))
}

func TestPersistedBinarySliceViewProtocolAdmission(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	table := ctx.tables["nation"]
	name2col := make(map[string]int32, len(table.Cols)+1)
	for pos, col := range table.Cols {
		name2col[col.Name] = int32(pos)
	}
	binaryPos := int32(len(table.Cols))
	table.Cols = append(table.Cols, &planpb.ColDef{
		ColId:      uint64(binaryPos),
		Name:       "n_binary",
		OriginName: "n_binary",
		Typ: planpb.Type{
			Id:      int32(types.T_varbinary),
			Width:   128,
			Charset: uint32(types.CharsetBinary),
		},
	})
	name2col["n_binary"] = binaryPos
	table.Name2ColIndex = name2col

	proc := ctx.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldProtocol, hadProtocol := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldReadFloor, hadReadFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor)
	oldAuthoringFloor, hadAuthoringFloor := rt.GetGlobalVariables(
		moruntime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if hadProtocol {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldProtocol)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		if hadReadFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, oldReadFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolFloor, current)
		}
		if hadAuthoringFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, oldAuthoringFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, current)
		}
	})

	const createSQL = "create view v_binary_slice as select left(n_binary, 1) as x, substring(n_name, 1, 3) as txt from nation"
	build := func(floor int64) (*Plan, error) {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, floor)
		rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, floor)
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, createSQL, 1)
		if err != nil {
			return nil, err
		}
		defer stmt.Free()
		return BuildPlan(&rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: createSQL}, stmt, false)
	}

	_, err := build(defines.MORPCVersion85)
	require.ErrorContains(t, err, "protocol version 86")

	created, err := build(defines.MORPCVersion86)
	require.NoError(t, err)
	createdView := created.GetDdl().GetCreateView().GetTableDef()
	require.Len(t, createdView.GetCols(), 2)
	createdColType := createdView.GetCols()[0].Typ
	require.Equal(t, int32(types.T_varbinary), createdColType.Id)
	require.Equal(t, int32(1), createdColType.Width)
	require.Equal(t, int32(types.T_varchar), createdView.Cols[1].Typ.Id)
	require.Equal(t, int32(3), createdView.Cols[1].Typ.Width)
	var viewData ViewData
	require.NoError(t, json.Unmarshal(
		[]byte(createdView.GetViewSql().GetView()), &viewData))
	require.NotNil(t, viewData.RequiredProtocolVersion)
	require.Equal(t, int64(defines.MORPCVersion86), *viewData.RequiredProtocolVersion)

	regenerated, err := RegenerateViewDefinition(ctx, createdView.GetViewSql().GetView())
	require.NoError(t, err)
	require.Len(t, regenerated.TableDef.Cols, 2)
	regeneratedColType := regenerated.TableDef.Cols[0].Typ
	require.Equal(t, int32(types.T_varbinary), regeneratedColType.Id)
	require.Equal(t, int32(1), regeneratedColType.Width)
	require.Equal(t, int32(types.T_varchar), regenerated.TableDef.Cols[1].Typ.Id)
	require.Equal(t, int32(3), regenerated.TableDef.Cols[1].Typ.Width)
	var regeneratedData ViewData
	require.NoError(t, json.Unmarshal(
		[]byte(regenerated.TableDef.GetViewSql().GetView()), &regeneratedData))
	require.NotNil(t, regeneratedData.RequiredProtocolVersion)
	require.Equal(t, int64(defines.MORPCVersion86), *regeneratedData.RequiredProtocolVersion)
}

func TestPersistedSpatialDistanceViewProtocolLifecycle(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	proc := ctx.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldProtocol, hadProtocol := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldReadFloor, hadReadFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor)
	oldAuthoringFloor, hadAuthoringFloor := rt.GetGlobalVariables(
		moruntime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if hadProtocol {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldProtocol)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		if hadReadFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, oldReadFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolFloor, current)
		}
		if hadAuthoringFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, oldAuthoringFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, current)
		}
	})

	const createSQL = "create view v_spatial_distance as select st_frechetdistance(" +
		"st_geomfromtext('LINESTRING(0 0, 1 0)', 4326), " +
		"st_geomfromtext('LINESTRING(0 1, 1 1)', 4326)) as d"
	build := func() (*Plan, error) {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		root := &rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: createSQL}
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, createSQL, 1)
		require.NoError(t, err)
		defer stmt.Free()
		return BuildPlan(root, stmt, false)
	}

	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion83))
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, int64(defines.MORPCVersion83))
	_, err := build()
	require.ErrorContains(t, err, "protocol version 86")

	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion86))
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, int64(defines.MORPCVersion86))
	created, err := build()
	require.NoError(t, err)
	createdView := created.GetDdl().GetCreateView().GetTableDef()
	var data ViewData
	require.NoError(t, json.Unmarshal([]byte(createdView.GetViewSql().GetView()), &data))
	require.NotNil(t, data.RequiredProtocolVersion)
	require.Equal(t, int64(defines.MORPCVersion86), *data.RequiredProtocolVersion)

	// Rebinding a markerless historical definition must rediscover the spatial
	// requirement from the optimized plan rather than trust a missing marker.
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(createdView.GetViewSql().GetView()), &fields))
	delete(fields, "required_protocol_version")
	markerless, err := json.Marshal(fields)
	require.NoError(t, err)
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion83))
	_, err = RegenerateViewDefinition(ctx, string(markerless))
	require.ErrorContains(t, err, "protocol version 86")
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion86))
	regenerated, err := RegenerateViewDefinition(ctx, string(markerless))
	require.NoError(t, err)
	var regeneratedData ViewData
	require.NoError(t, json.Unmarshal(
		[]byte(regenerated.TableDef.GetViewSql().GetView()), &regeneratedData))
	require.Equal(t, int64(defines.MORPCVersion86), *regeneratedData.RequiredProtocolVersion)
}

func TestPersistedIPFunctionRequirementSurvivesViewFilterFold(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	proc := ctx.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldProtocol, hadProtocol := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldReadFloor, hadReadFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor)
	oldAuthoringFloor, hadAuthoringFloor := rt.GetGlobalVariables(
		moruntime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if hadProtocol {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldProtocol)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		if hadReadFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, oldReadFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolFloor, current)
		}
		if hadAuthoringFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, oldAuthoringFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, current)
		}
	})

	const createSQL = `create view v_ip_filter as select n_name from nation
where inet_ntoa(cast('"2"' as json)) = '0.0.0.2'`
	build := func() (*Plan, error) {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		root := &rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: createSQL}
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, createSQL, 1)
		require.NoError(t, err)
		defer stmt.Free()
		return BuildPlan(root, stmt, false)
	}

	// The filter is constant-foldable, but its INET_NTOA overload is a v85
	// persisted-expression contract. A pre-v85 writer must still reject the
	// view instead of persisting only the folded NULL/literal result.
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion84))
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, int64(defines.MORPCVersion84))
	_, err := build()
	require.ErrorContains(t, err, "protocol version 85")

	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion85))
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, int64(defines.MORPCVersion85))
	created, err := build()
	require.NoError(t, err)
	var data ViewData
	require.NoError(t, json.Unmarshal(
		[]byte(created.GetDdl().GetCreateView().GetTableDef().GetViewSql().GetView()), &data))
	require.NotNil(t, data.RequiredProtocolVersion)
	require.Equal(t, int64(defines.MORPCVersion85), *data.RequiredProtocolVersion)
}

func TestRegenerateViewDefinitionPersistsExpandedStar(t *testing.T) {
	for _, rootSQL := range []string{
		"create view v as select * from nation",
		"create view v (k, name, rkey, comment) as select * from nation",
	} {
		t.Run(rootSQL, func(t *testing.T) {
			ctx := NewMockCompilerContext(false)
			ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }
			persisted, err := json.Marshal(map[string]any{
				"Stmt": rootSQL, "DefaultDatabase": "tpch", "future_field": map[string]bool{"keep": true},
			})
			require.NoError(t, err)

			first, err := RegenerateViewDefinition(ctx, string(persisted))
			require.NoError(t, err)
			require.Len(t, first.TableDef.Cols, 4)
			var firstData ViewData
			require.NoError(t, json.Unmarshal([]byte(first.TableDef.ViewSql.View), &firstData))
			require.NotContains(t, firstData.Stmt, "*")

			ctx.tables["nation"].Cols = append(ctx.tables["nation"].Cols, &planpb.ColDef{
				Name:       "n_extra",
				OriginName: "n_extra",
				Typ:        planpb.Type{Id: int32(types.T_int32)},
				Default:    &planpb.Default{NullAbility: true},
			})
			second, err := RegenerateViewDefinition(ctx, first.TableDef.ViewSql.View)
			require.NoError(t, err)
			require.Len(t, second.TableDef.Cols, 4)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(second.TableDef.ViewSql.View), &fields))
			require.JSONEq(t, `{"keep":true}`, string(fields["future_field"]))
		})
	}
}

func TestAuthoritativeViewGenerationCapturesLimitZeroStarDependency(t *testing.T) {
	const rootSQL = "create view v as select * from nation limit 0"
	ctx := &rootSQLCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		rootSQL:             rootSQL,
	}
	ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }

	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()
	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)

	var data ViewData
	require.NoError(t, json.Unmarshal(
		[]byte(p.GetDdl().GetCreateView().GetTableDef().GetViewSql().GetView()), &data))
	require.Len(t, data.Dependencies, 1)
	require.Equal(t, "nation", data.Dependencies[0].RelationName)
}

func TestReplaceRegeneratedViewDependenciesPreservesViewData(t *testing.T) {
	regenerated := &RegeneratedViewDefinition{
		TableDef: &planpb.TableDef{ViewSql: &planpb.ViewDef{View: `{
			"stmt":"create view v as select a from t",
			"default_database":"db",
			"lower_case_table_names":1,
			"future_field":{"keep":true},
			"dependencies":[]}`}},
	}
	dependencies := []ViewDependency{{
		AccountID: 1, DatabaseID: 2, RelationID: 4, LogicalID: 3,
		DatabaseName: "db", RelationName: "t",
	}}
	require.NoError(t, ReplaceRegeneratedViewDependencies(regenerated, dependencies))
	require.Equal(t, dependencies, regenerated.Dependencies)

	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(regenerated.TableDef.ViewSql.View), &fields))
	require.JSONEq(t, `{"keep":true}`, string(fields["future_field"]))
	require.JSONEq(t, `1`, string(fields["lower_case_table_names"]))
	require.JSONEq(t, `[{"account_id":1,"database_id":2,"relation_id":4,"logical_id":3,"database_name":"db","relation_name":"t","relation_kind":"","version":0}]`, string(fields["dependencies"]))
}

func TestViewDependencyIdentityKeepsDistinctBindingEnvironments(t *testing.T) {
	base := ViewDependency{
		AccountID: 1, DatabaseID: 2, RelationID: 3,
		DatabaseName: "PhysicalDB", RelationName: "PhysicalTable",
		BindingDatabaseName: "sub_one", BindingRelationName: "PhysicalTable",
		SubscriptionName: "sub_one", LowerCaseTableNames: 1,
	}
	secondSubscription := base
	secondSubscription.BindingDatabaseName = "sub_two"
	secondSubscription.SubscriptionName = "sub_two"
	require.NotEqual(t, viewDependencyKey(base), viewDependencyKey(secondSubscription))

	snapshot := base
	snapshot.SubscriptionName = ""
	snapshot.SnapshotName = "daily"
	snapshot.Snapshot = &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 123}}
	require.NotEqual(t, viewDependencyKey(base), viewDependencyKey(snapshot))

	caseVariant := base
	caseVariant.BindingDatabaseName = "SUB_ONE"
	require.Equal(t, viewDependencyKey(base), viewDependencyKey(caseVariant))
	caseVariant.LowerCaseTableNames = 0
	require.NotEqual(t, viewDependencyKey(base), viewDependencyKey(caseVariant))
}

func TestViewDependencyCaptureScopeAndIdentityFallbacks(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	ctx.GetAccountIdFunc = func() (uint32, error) { return 7, nil }
	capture := newViewDependencyCaptureContext(ctx)

	capture.enterNestedView()
	_, _, err := capture.Resolve("tpch", "nation", nil)
	require.NoError(t, err)
	require.Empty(t, capture.dependencies())
	capture.leaveNestedView()
	require.Panics(t, capture.leaveNestedView)

	obj := &planpb.ObjectRef{PubInfo: &planpb.PubInfo{TenantId: 11}}
	tableDef := &planpb.TableDef{DbId: 2, TblId: 3, LogicalId: 4, DbName: "physical_db", Name: "physical_t"}
	require.NoError(t, capture.record(obj, tableDef, nil, "", ""))
	dependencies := capture.dependencies()
	require.Len(t, dependencies, 1)
	require.Equal(t, uint32(11), dependencies[0].AccountID)
	require.Equal(t, "physical_db", dependencies[0].DatabaseName)
	require.Equal(t, "physical_t", dependencies[0].RelationName)
	require.Equal(t, uint32(11), dependencies[0].PublisherAccount)

	snapshot := &Snapshot{
		TS:     &timestamp.Timestamp{PhysicalTime: 456},
		Tenant: &planpb.SnapshotTenant{TenantID: 13},
	}
	capture.snapshotNames[snapshot.String()] = "daily"
	require.NoError(t, capture.record(
		&planpb.ObjectRef{SchemaName: "snapshot_db", ObjName: "snapshot_t"},
		tableDef, snapshot, "snapshot_db", "snapshot_t"))
	dependencies = capture.dependencies()
	require.Len(t, dependencies, 2)
	require.Equal(t, uint32(13), dependencies[1].AccountID)
	require.Equal(t, "daily", dependencies[1].SnapshotName)
	require.NotSame(t, snapshot, dependencies[1].Snapshot)

	expected := errors.New("account unavailable")
	ctx.GetAccountIdFunc = func() (uint32, error) { return 0, expected }
	require.ErrorIs(t, capture.record(&planpb.ObjectRef{}, tableDef, nil, "", ""), expected)
}

func TestViewDependencyCaptureResolveByID(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	ctx.GetAccountIdFunc = func() (uint32, error) { return 7, nil }
	capture := newViewDependencyCaptureContext(ctx)
	tableID := ctx.tables["nation"].TblId
	obj, tableDef, err := capture.ResolveById(tableID, nil)
	require.NoError(t, err)
	require.NotNil(t, obj)
	require.NotNil(t, tableDef)
	require.Len(t, capture.dependencies(), 1)
}

func TestRegenerateViewDefinitionRejectsInvalidPersistedDefinitions(t *testing.T) {
	ctx := NewMockCompilerContext(false)

	_, err := RegenerateViewDefinition(ctx, `{`)
	require.Error(t, err)
	_, err = RegenerateViewDefinition(ctx, `{"Stmt":"select ("}`)
	require.Error(t, err)
	_, err = RegenerateViewDefinition(ctx, `{"Stmt":"select 1; select 2"}`)
	require.Error(t, err)
	_, err = RegenerateViewDefinition(ctx, `{"Stmt":"select 1"}`)
	require.Error(t, err)
	futureVersion := defines.MORPCLatestVersion + 1
	_, err = RegenerateViewDefinition(ctx,
		fmt.Sprintf(`{"Stmt":"select (","required_protocol_version":%d}`, futureVersion))
	require.ErrorContains(t, err, fmt.Sprintf("protocol version %d", futureVersion))
	_, err = RegenerateViewDefinition(ctx,
		`{"Stmt":"select (","required_protocol_version":-1}`)
	require.ErrorContains(t, err, "must not be negative")

	for _, regenerated := range []*RegeneratedViewDefinition{
		nil,
		{},
		{TableDef: &planpb.TableDef{}},
	} {
		require.Error(t, ReplaceRegeneratedViewDependencies(regenerated, nil))
	}
	require.Error(t, ReplaceRegeneratedViewDependencies(&RegeneratedViewDefinition{
		TableDef: &planpb.TableDef{ViewSql: &planpb.ViewDef{View: `{`}},
	}, nil))
}

func TestRegenerateAlterViewUsesPersistedParserEnvironment(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }
	mode := ""
	lowerCaseTableNames := int64(0)
	persisted, err := json.Marshal(ViewData{
		Stmt:                "alter view v (renamed) as select n_name from nation",
		DefaultDatabase:     "tpch",
		SQLMode:             &mode,
		LowerCaseTableNames: &lowerCaseTableNames,
	})
	require.NoError(t, err)
	regenerated, err := RegenerateViewDefinition(ctx, string(persisted))
	require.NoError(t, err)
	require.Equal(t, "renamed", regenerated.TableDef.Cols[0].Name)
}
