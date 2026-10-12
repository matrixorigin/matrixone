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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

// Main's canonical metadata owner admits the already implemented UCA400
// contract while the staged 0900 and versioned physical format remain closed.
func TestCollationMetadataIntegrationUsesCanonicalAdmission(t *testing.T) {
	for _, identity := range []uint32{0, 1, 2, 3} {
		typ := planpb.Type{Id: int32(types.T_varchar), Charset: identity}
		require.NoError(t, planpb.RequireLegacyCollations(typ))
	}
	for _, identity := range []uint32{9, 10} {
		typ := planpb.Type{Id: int32(types.T_varchar), Charset: identity, CollationVersion: 1}
		require.NoError(t, planpb.RequireLegacyCollations(typ))
		require.NoError(t, planpb.RequireLegacyCollations(&planpb.TableDef{Cols: []*planpb.ColDef{{Typ: typ}}}))
	}
	for _, identity := range []uint32{4, 5, 260} {
		typ := planpb.Type{Id: int32(types.T_varchar), Charset: identity, CollationVersion: 1}
		require.Error(t, planpb.RequireLegacyCollations(typ))
		require.Error(t, planpb.RequireLegacyCollations(&planpb.Expr{Typ: typ}))
	}
	require.Error(t, planpb.RequireLegacyCollations(planpb.Type{Id: int32(types.T_varchar), Charset: 3, CollationVersion: 2}))
	for _, format := range []uint32{1, 256} {
		require.Error(t, planpb.RequireLegacyCollations(&planpb.TableDef{DefaultCharset: 3, KeyFormat: format}))
	}
}

// The retired prototype function must fail while binding, before folding can
// erase its identity. This does not claim SQL COLLATE syntax preserves metadata.
func TestRetiredNativeCollationKeyRejectsBeforeFolding(t *testing.T) {
	for _, sql := range []string{
		"select length(internal_collation_key('Alpha', cast(4 as unsigned)))",
		"select length(internal_collation_key(n_name, cast(4 as unsigned))) from nation",
	} {
		t.Run(sql, func(t *testing.T) {
			ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			_, err = BuildPlan(ctx, stmt, false)
			require.Error(t, err)
			require.Contains(t, err.Error(), "internal_collation_key")
		})
	}
}

func TestNativeCollationMetadataRejectsFoldedProjection(t *testing.T) {
	for _, sql := range []string{
		"select 1 from nation",
		"select length(n_name) from nation",
	} {
		t.Run(sql, func(t *testing.T) {
			ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
			col := FindColumn(ctx.tables["nation"].Cols, "n_name")
			require.NotNil(t, col)
			col.Typ.Charset, col.Typ.CollationVersion = 4, 1
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			_, err = BuildPlan(ctx, stmt, false)
			require.ErrorContains(t, err, "disabled")
		})
	}
}
