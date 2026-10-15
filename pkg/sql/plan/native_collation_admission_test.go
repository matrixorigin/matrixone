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
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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

func TestCollationCatalogAdmissionPreservesAllowedDomainsAndReuse(t *testing.T) {
	contracts := []struct {
		name                          string
		identity, revision, keyFormat uint32
		allowed                       bool
	}{
		{"legacy0", 0, 0, 0, true},
		{"binary1", 1, 0, 0, true},
		{"legacy_bin2", 2, 0, 0, true},
		{"legacy_ci3", 3, 0, 0, true},
		{"unicode9", 9, 1, 0, true},
		{"unicode10", 10, 1, 0, true},
		{"native4", 4, 1, 0, false},
		{"native5", 5, 1, 0, false},
		{"wide_identity", 260, 1, 0, false},
		{"unknown_revision", 3, 2, 0, false},
		{"native_format", 3, 0, 1, false},
		{"wide_format", 3, 0, 256, false},
	}
	for _, contract := range contracts {
		for _, sql := range []string{"select 1 from nation", "select * from nation limit 0"} {
			t.Run(contract.name+"/"+sql, func(t *testing.T) {
				ctx := &viewMetadataConsumerContext{MockCompilerContext: NewMockCompilerContext(false, newPlanTestProcess(t))}
				table := ctx.tables["nation"]
				col := FindColumn(table.Cols, "n_name")
				require.NotNil(t, col)
				col.Typ.Charset, col.Typ.CollationVersion = contract.identity, contract.revision
				table.KeyFormat = contract.keyFormat
				stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
				require.NoError(t, err)
				_, err = BuildPlan(ctx, stmt, false)
				stmt.Free()
				if contract.allowed {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
				if sql == "select * from nation limit 0" {
					require.Zero(t, ctx.fastCalls, "LIMIT 0 must use complete catalog metadata")
				}
				// The admission visitor is per call; an error must not poison
				// the same compiler context's subsequent legacy query.
				col.Typ.Charset, col.Typ.CollationVersion, table.KeyFormat = 3, 0, 0
				stmt, err = parsers.ParseOne(t.Context(), dialect.MYSQL, "select 1 from nation", 1)
				require.NoError(t, err)
				defer stmt.Free()
				_, err = BuildPlan(ctx, stmt, false)
				require.NoError(t, err)
			})
		}
	}
}

func TestCollationCatalogAdmissionPreservesSnapshotScopeErrors(t *testing.T) {
	for _, contract := range []struct {
		name               string
		identity, revision uint32
		objectID           uint64
		errorText          string
	}{
		{"valid_legacy", 3, 0, 7, ""},
		{"valid_unicode", 9, 1, 7, ""},
		{"valid_scope_native", 4, 1, 7, "disabled"},
		{"invalid_scope_legacy", 3, 0, 8, "does not belong"},
		{"invalid_scope_native", 4, 1, 8, "does not belong"},
	} {
		t.Run(contract.name, func(t *testing.T) {
			ctx := &namedSnapshotCompilerContext{
				MockCompilerContext: NewMockCompilerContext(false, newPlanTestProcess(t)),
				snapshot: &Snapshot{
					TS: &timestamp.Timestamp{PhysicalTime: 42},
					ExtraInfo: &planpb.SnapshotExtraInfo{
						Name: "snapshot", Level: tree.SNAPSHOTLEVELTABLE.String(), ObjId: contract.objectID,
					},
				},
			}
			table := ctx.tables["nation"]
			table.TblId, table.LogicalId = 7, 7
			col := FindColumn(table.Cols, "n_name")
			require.NotNil(t, col)
			col.Typ.Charset, col.Typ.CollationVersion = contract.identity, contract.revision
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "select 1 from nation {snapshot = 'snapshot'}", 1)
			require.NoError(t, err)
			defer stmt.Free()
			_, err = BuildPlan(ctx, stmt, false)
			if contract.errorText == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, contract.errorText)
			}
		})
	}
}
