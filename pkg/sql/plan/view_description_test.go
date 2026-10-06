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
	"context"
	"encoding/json"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

func TestViewMetadataUdfRebindFlagSurvivesPlanCopyAndWire(t *testing.T) {
	query := &planpb.Query{ViewMetadataDependsOnUdf: true}
	require.True(t, DeepCopyQuery(query).GetViewMetadataDependsOnUdf())
	data, err := query.Marshal()
	require.NoError(t, err)
	var decoded planpb.Query
	require.NoError(t, decoded.Unmarshal(data))
	require.True(t, decoded.GetViewMetadataDependsOnUdf())
}

func TestDescribeViewColumnsUsesCurrentDefinition(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }
	const definition = `{"Stmt":"create view v as select n_name as label, n_nationkey as k from nation","DefaultDatabase":"tpch"}`
	before, err := DescribeViewColumns(ctx, definition)
	require.NoError(t, err)
	ctx.tables["nation"].Cols[1].Typ.Width = 60
	source := ctx.tables["nation"].Cols[0]
	source.Typ.Id = int32(types.T_int64)
	source.Default = &planpb.Default{Expr: makePlan2Int32ConstExprWithType(7), OriginString: "7"}
	after, err := DescribeViewColumns(ctx, definition)
	require.NoError(t, err)
	require.Equal(t, "label", after[0].Name)
	require.Equal(t, int32(60), after[0].Typ.Width)
	require.NotEqual(t, before[0].Typ.Width, after[0].Typ.Width)
	require.Equal(t, int32(types.T_int64), after[1].Typ.Id)
	require.Equal(t, "7", after[1].Default.OriginString)
	regenerated, err := RegenerateViewDefinition(ctx, definition)
	require.NoError(t, err)
	require.Equal(t, regenerated.TableDef.Cols, after)
	after[1].Default.OriginString = "changed"
	require.Equal(t, "7", source.Default.OriginString, "caller owns description, not source catalog metadata")
	again, err := DescribeViewColumns(ctx, definition)
	require.NoError(t, err)
	require.Equal(t, "7", again[1].Default.OriginString)
}

// The definition writer reads the current authoring security setting when it
// assembles ViewData. A metadata reader must not enter that publication phase.
type descriptionPublicationContext struct {
	*MockCompilerContext
	publicationReads int
}

func (c *descriptionPublicationContext) ResolveVariable(name string, system, global bool) (any, error) {
	if name == "view_security_type" {
		c.publicationReads++
	}
	return c.MockCompilerContext.ResolveVariable(name, system, global)
}

func TestDescribeViewColumnsDoesNotAssembleDefinition(t *testing.T) {
	ctx := &descriptionPublicationContext{MockCompilerContext: NewMockCompilerContext(false, newPlanTestProcess(t))}
	ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }
	const definition = `{"Stmt":"create view v (label) as select n_name from nation","DefaultDatabase":"tpch","security_type":"INVOKER","future_field":{"keep":true}}`
	cols, err := DescribeViewColumns(ctx, definition)
	require.NoError(t, err)
	require.Len(t, cols, 1)
	require.Equal(t, "label", cols[0].Name)
	require.Equal(t, "label", cols[0].OriginName)
	require.Zero(t, ctx.publicationReads, "description must stop at output inference")

	regenerated, err := RegenerateViewDefinition(ctx, definition)
	require.NoError(t, err)
	require.Equal(t, 1, ctx.publicationReads, "the definition owner still assembles ViewData")
	require.Equal(t, cols, regenerated.TableDef.Cols)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(regenerated.TableDef.ViewSql.View), &fields))
	require.JSONEq(t, `"INVOKER"`, string(fields["security_type"]))
	require.JSONEq(t, `{"keep":true}`, string(fields["future_field"]))
}

func TestDescribeViewColumnsSharedInferenceContract(t *testing.T) {
	for _, tc := range []struct {
		name          string
		selectSQL     string
		nullable      bool
		defaultOrigin string
	}{
		{name: "direct", selectSQL: "select n_nationkey from nation", defaultOrigin: "7"},
		{name: "nested", selectSQL: "select k from inner_v", defaultOrigin: "7"},
		{name: "null extended", selectSQL: "select n.n_nationkey from region r left join nation n on r.r_regionkey=n.n_regionkey", nullable: true, defaultOrigin: "7"},
		{name: "expression clears default", selectSQL: "select n_nationkey + 1 from nation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
			ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }
			source := ctx.tables["nation"].Cols[0]
			source.Typ.NotNullable = true
			source.Default = &planpb.Default{NullAbility: false, Expr: makePlan2Int32ConstExprWithType(7), OriginString: "7"}
			ctx.objects["inner_v"] = &ObjectRef{SchemaName: "tpch", ObjName: "inner_v", Obj: 101}
			ctx.tables["inner_v"] = &TableDef{
				Name: "inner_v", TblId: 101, TableType: catalog.SystemViewRel,
				ViewSql: &planpb.ViewDef{View: `{"Stmt":"create view inner_v (k) as select n_nationkey from nation","DefaultDatabase":"tpch"}`},
				Cols:    []*ColDef{{Name: "k", Typ: planpb.Type{Id: int32(types.T_int32)}}},
			}
			beforeSource := proto.Clone(ctx.tables["nation"])
			beforeView := proto.Clone(ctx.tables["inner_v"])
			definition, err := json.Marshal(ViewData{Stmt: "create view v (result) as " + tc.selectSQL, DefaultDatabase: "tpch"})
			require.NoError(t, err)
			cols, err := DescribeViewColumns(ctx, string(definition))
			require.NoError(t, err)
			require.Len(t, cols, 1)
			require.Equal(t, "result", cols[0].OriginName)
			require.Equal(t, tc.nullable, !cols[0].Typ.NotNullable)
			require.Equal(t, tc.nullable, cols[0].Default.NullAbility)
			require.Equal(t, tc.defaultOrigin, cols[0].Default.OriginString)
			if tc.defaultOrigin != "" {
				require.Equal(t, int32(types.T_int32), cols[0].Typ.Id)
				require.Equal(t, int32(7), cols[0].Default.Expr.GetLit().GetI32Val())
			}
			regenerated, err := RegenerateViewDefinition(ctx, string(definition))
			require.NoError(t, err)
			require.Equal(t, cols, regenerated.TableDef.Cols)
			require.Equal(t, beforeSource, ctx.tables["nation"])
			require.Equal(t, beforeView, ctx.tables["inner_v"])
			cols[0].Default.OriginString = "changed by consumer"
			require.Equal(t, beforeSource, ctx.tables["nation"])
			require.Equal(t, beforeView, ctx.tables["inner_v"])
		})
	}
}

func TestDescribeViewColumnsFailureDoesNotCache(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }
	const definition = `{"Stmt":"create view v as select n_name from nation","DefaultDatabase":"tpch"}`
	source := ctx.tables["nation"]
	delete(ctx.tables, "nation")
	cols, err := DescribeViewColumns(ctx, definition)
	require.Error(t, err)
	require.Nil(t, cols)
	ctx.tables["nation"] = source
	cols, err = DescribeViewColumns(ctx, definition)
	require.NoError(t, err)
	require.Len(t, cols, 1)
}

type subscriptionViewDatabaseContext struct {
	*MockCompilerContext
	sub *SubscriptionMeta
}

func (c *subscriptionViewDatabaseContext) GetQueryingSubscription() *SubscriptionMeta {
	return c.sub
}

func TestSubscriptionViewDatabaseLookupUsesPublisherSnapshot(t *testing.T) {
	const publisher uint32 = 23
	subscriberSnapshot := &Snapshot{
		TS:     &timestamp.Timestamp{PhysicalTime: 50},
		Tenant: &planpb.SnapshotTenant{TenantID: 17},
	}
	mock := NewMockCompilerContext(false, newPlanTestProcess(t))
	ctx := &subscriptionViewDatabaseContext{MockCompilerContext: mock,
		sub: &SubscriptionMeta{AccountId: int32(publisher)}}
	mock.GetDatabaseIdFunc = func(name string, snapshot *Snapshot) (uint64, error) {
		require.Equal(t, "source", name)
		require.Equal(t, publisher, snapshot.Tenant.TenantID)
		require.Equal(t, subscriberSnapshot.TS, snapshot.TS)
		return 42, nil
	}
	checker := &viewRegenerationContext{CompilerContext: ctx}
	exists, err := checker.CheckViewDatabase("source", subscriberSnapshot)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, uint32(17), subscriberSnapshot.Tenant.TenantID, "caller snapshot must remain unchanged")

	ctx.sub = nil
	mock.GetDatabaseIdFunc = func(_ string, snapshot *Snapshot) (uint64, error) {
		require.Same(t, subscriberSnapshot, snapshot)
		return 0, moerr.NewBadDB(t.Context(), "source")
	}
	exists, err = checker.CheckViewDatabase("source", subscriberSnapshot)
	require.NoError(t, err)
	require.False(t, exists, "a non-subscription lookup must not borrow a publisher")
}

func TestDescribeViewColumnsPreservesDatabaseLookupFailure(t *testing.T) {
	const definition = `{"Stmt":"create view v as select n_name from tpch.nation","DefaultDatabase":"tpch"}`
	for _, tc := range []struct {
		name        string
		lookupError error
		missing     bool
	}{
		{name: "storage failure", lookupError: moerr.NewInternalError(t.Context(), "catalog storage unavailable")},
		{name: "cancelled", lookupError: context.Canceled},
		{name: "genuinely missing", lookupError: moerr.GetOkExpectedEOB(), missing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
			ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }
			ctx.DatabaseExistsFunc = func(string, *Snapshot) bool {
				t.Fatal("View regeneration must not use a bool-only database lookup")
				return false
			}
			ctx.GetDatabaseIdFunc = func(name string, _ *Snapshot) (uint64, error) {
				require.Equal(t, "tpch", name)
				return 0, tc.lookupError
			}
			cols, err := DescribeViewColumns(ctx, definition)
			require.Nil(t, cols)
			if tc.missing {
				require.Error(t, err)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadDB), err)
			} else {
				require.ErrorIs(t, err, tc.lookupError)
			}
		})
	}
}

func TestDescribeViewColumnsRejectsInvalidInputAndCancellation(t *testing.T) {
	for _, definition := range []string{`{`, `{"Stmt":"select 1"}`, `{"Stmt":"create view v as select 1; select 2"}`} {
		ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
		cols, err := DescribeViewColumns(ctx, definition)
		require.Error(t, err)
		require.Nil(t, cols)
	}
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	canceled, cancel := context.WithCancel(ctx.GetContext())
	cancel()
	ctx.SetContext(canceled)
	cols, err := DescribeViewColumns(ctx, `{"Stmt":"create view v as select n_name from nation","DefaultDatabase":"tpch"}`)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, cols)
}
