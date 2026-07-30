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

package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestClassifyUpdatePlannerError(t *testing.T) {
	ctx := context.Background()
	baseErr := moerr.NewUnsupportedDML(ctx, "multi-table update")
	typedErr := newLegacyUpdatePlannerRouteError(
		updateRouteReasonMultiTarget,
		baseErr,
	)
	require.Equal(t, baseErr.Error(), typedErr.Error())
	require.ErrorIs(t, typedErr, baseErr)

	tests := []struct {
		name       string
		err        error
		wantRoute  updatePlannerRoute
		wantReason updatePlannerRouteReason
		wantErr    error
	}{
		{
			name:       "typed legacy route",
			err:        typedErr,
			wantRoute:  updatePlannerLegacy,
			wantReason: updateRouteReasonMultiTarget,
			wantErr:    baseErr,
		},
		{
			name:       "raw unsupported dml is unknown",
			err:        moerr.NewUnsupportedDML(ctx, "new shared helper route"),
			wantRoute:  updatePlannerUnknown,
			wantReason: updateRouteReasonUnknown,
		},
		{
			name:       "ordinary binder error is rejected",
			err:        moerr.NewInternalError(ctx, "binder failed"),
			wantRoute:  updatePlannerRejected,
			wantReason: updateRouteReasonBinderError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			route, reason, gotErr := classifyUpdatePlannerError(test.err)
			require.Equal(t, test.wantRoute, route)
			require.Equal(t, test.wantReason, reason)
			if test.wantErr != nil {
				require.Same(t, test.wantErr, gotErr)
			} else {
				require.Same(t, test.err, gotErr)
			}
		})
	}
}

func TestClassifyUpdateTableResolutionError(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	stmt, err := parsers.ParseOne(
		ctx.GetContext(),
		dialect.MYSQL,
		"UPDATE nation SET n_name = 'x'",
		1,
	)
	require.NoError(t, err)
	defer stmt.Free()
	updateStmt := stmt.(*tree.Update)

	tests := []struct {
		name       string
		err        error
		wantRoute  updatePlannerRoute
		wantReason updatePlannerRouteReason
		wantText   string
	}{
		{
			name: "foreign key enters temporary legacy allowlist",
			err: moerr.NewUnsupportedDML(
				ctx.GetContext(),
				foreignKeyUnsupportedDMLCause,
			),
			wantRoute:  updatePlannerLegacy,
			wantReason: updateRouteReasonForeignKey,
			wantText:   foreignKeyUnsupportedDMLMsg,
		},
		{
			name: "iceberg uses specialized planner",
			err: moerr.NewUnsupportedDML(
				ctx.GetContext(),
				icebergRowLevelDMLUnsupportedCause,
			),
			wantRoute:  updatePlannerSpecialized,
			wantReason: updateRouteReasonIceberg,
			wantText:   icebergRowLevelDMLUnsupportedMsg,
		},
		{
			name: "writable external update is rejected",
			err: moerr.NewUnsupportedDML(
				ctx.GetContext(),
				externalTableUnsupportedDMLCause,
			),
			wantRoute:  updatePlannerRejected,
			wantReason: updateRouteReasonExternalTable,
			wantText:   "invalid input: cannot insert/update/delete from external table",
		},
		{
			name: "unsupported table form is rejected",
			err: moerr.NewUnsupportedDML(
				ctx.GetContext(),
				"unsupported table type",
			),
			wantRoute:  updatePlannerRejected,
			wantReason: updateRouteReasonTableForm,
			wantText:   "unsupported DML: unsupported table type",
		},
		{
			name: "empty table name is rejected",
			err: moerr.NewUnsupportedDML(
				ctx.GetContext(),
				"empty table name",
			),
			wantRoute:  updatePlannerRejected,
			wantReason: updateRouteReasonEmptyTableName,
			wantText:   "unsupported DML: empty table name",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			classifiedErr := classifyUpdateTableResolutionError(ctx, updateStmt, test.err)
			route, reason, gotErr := classifyUpdatePlannerError(classifiedErr)
			require.Equal(t, test.wantRoute, route)
			require.Equal(t, test.wantReason, reason)
			require.Equal(t, test.wantText, gotErr.Error())
		})
	}
}

func TestBindUpdateProducesTypedPlannerRoutes(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		prepare    func(*MockOptimizer)
		wantRoute  updatePlannerRoute
		wantReason updatePlannerRouteReason
	}{
		{
			name:       "multi target",
			sql:        "UPDATE emp, dept SET emp.sal = 1, dept.loc = 'x'",
			wantRoute:  updatePlannerLegacy,
			wantReason: updateRouteReasonMultiTarget,
		},
		{
			name: "foreign key metadata",
			sql:  "UPDATE nation SET n_name = 'x'",
			prepare: func(mock *MockOptimizer) {
				mock.ctxt.tables["nation"].Fkeys = []*planpb.ForeignKeyDef{{Name: "fk"}}
			},
			wantRoute:  updatePlannerLegacy,
			wantReason: updateRouteReasonForeignKey,
		},
		{
			name: "irregular index column",
			sql:  "UPDATE nation SET n_comment = 'x'",
			prepare: func(mock *MockOptimizer) {
				mock.ctxt.tables["nation"].Indexes = []*planpb.IndexDef{{
					IndexName: "idx",
					IndexAlgo: catalog.MoIndexIvfFlatAlgo.ToString(),
					Parts:     []string{"n_comment"},
				}}
			},
			wantRoute:  updatePlannerLegacy,
			wantReason: updateRouteReasonIrregularIndex,
		},
		{
			name: "pub sub key",
			sql:  "UPDATE nation SET n_nationkey = 2",
			prepare: func(mock *MockOptimizer) {
				mock.ctxt.tables["nation"].Name = catalog.MO_PUBS
			},
			wantRoute:  updatePlannerLegacy,
			wantReason: updateRouteReasonPubSubKey,
		},
		{
			name: "set auto increment",
			sql:  "UPDATE nation SET n_nationkey = 2",
			prepare: func(mock *MockOptimizer) {
				col := mock.ctxt.tables["nation"].Cols[0]
				col.Typ.Id = int32(types.T_uint64)
				col.Typ.Enumvalues = "one,two"
				col.Typ.AutoIncr = true
			},
			wantRoute:  updatePlannerLegacy,
			wantReason: updateRouteReasonAutoIncrement,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			if test.prepare != nil {
				test.prepare(mock)
			}

			stmt, err := parsers.ParseOne(
				mock.CurrentContext().GetContext(),
				dialect.MYSQL,
				test.sql,
				1,
			)
			require.NoError(t, err)
			defer stmt.Free()

			builder := NewQueryBuilder(planpb.Query_UPDATE, mock.CurrentContext(), false, true)
			bindCtx := NewBindContext(builder, nil)
			_, err = builder.bindUpdate(stmt.(*tree.Update), bindCtx)
			require.Error(t, err)

			route, reason, _ := classifyUpdatePlannerError(err)
			require.Equal(t, test.wantRoute, route, "bind error: %v", err)
			require.Equal(t, test.wantReason, reason, "bind error: %v", err)
		})
	}
}
