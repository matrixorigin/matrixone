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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// The fulltext2 alter hooks delegate to the shared func-vars (assigned by
// pkg/sql/plan at init). These tests stub those vars so the plugin package is
// tested in isolation, and assert each hook routes to the right helper with the
// fulltext2 algo — the gap that broke rename/drop of an INCLUDE column (#27964).

func TestHandleAlterRenameColumnDelegatesWithFulltext2Algo(t *testing.T) {
	old := planplugin.RenameIncludedColumnsForAlgo
	defer func() { planplugin.RenameIncludedColumnsForAlgo = old }()

	var gotAlgo, gotOld, gotNew string
	planplugin.RenameIncludedColumnsForAlgo = func(_ *planpb.TableDef, algo, oldColName, newColName string) ([]string, error) {
		gotAlgo, gotOld, gotNew = algo, oldColName, newColName
		return []string{"update mo_indexes ..."}, nil
	}

	sqls, err := Hooks{}.HandleAlterRenameColumn(&planpb.TableDef{}, "status", "state")
	require.NoError(t, err)
	require.Equal(t, []string{"update mo_indexes ..."}, sqls)
	require.Equal(t, catalog.MoIndexFullText2Algo.ToString(), gotAlgo)
	require.Equal(t, "status", gotOld)
	require.Equal(t, "state", gotNew)
}

func TestRenameColumnRequiresIndexRebuildDelegatesIncludeHook(t *testing.T) {
	old := planplugin.IncludedColumnAffected
	defer func() { planplugin.IncludedColumnAffected = old }()

	var gotCol string
	planplugin.IncludedColumnAffected = func(_ *planpb.IndexDef, colName string) (bool, error) {
		gotCol = colName
		return colName == "status", nil
	}

	// Renaming an INCLUDE column must require a rebuild (triggers reindex).
	needsRebuild, err := Hooks{}.RenameColumnRequiresIndexRebuild(&planpb.TableDef{}, &planpb.IndexDef{}, "status")
	require.NoError(t, err)
	require.True(t, needsRebuild)
	require.Equal(t, "status", gotCol)

	// A non-INCLUDE column does not force a rebuild.
	needsRebuild, err = Hooks{}.RenameColumnRequiresIndexRebuild(&planpb.TableDef{}, &planpb.IndexDef{}, "body")
	require.NoError(t, err)
	require.False(t, needsRebuild)
}

func TestHandleAlterDropColumnDelegatesIncludeHook(t *testing.T) {
	old := planplugin.IncludedColumnAffected
	defer func() { planplugin.IncludedColumnAffected = old }()

	planplugin.IncludedColumnAffected = func(_ *planpb.IndexDef, colName string) (bool, error) {
		return colName == "status", nil
	}

	// Dropping an INCLUDE column is "affected" → caller drops the index.
	affected, err := Hooks{}.HandleAlterDropColumn(&planpb.TableDef{}, &planpb.IndexDef{}, "status")
	require.NoError(t, err)
	require.True(t, affected)

	affected, err = Hooks{}.HandleAlterDropColumn(&planpb.TableDef{}, &planpb.IndexDef{}, "body")
	require.NoError(t, err)
	require.False(t, affected)
}

func TestUpdateColumnRequiresIndexRewriteDelegatesIncludeHook(t *testing.T) {
	old := planplugin.IncludedColumnAffected
	defer func() { planplugin.IncludedColumnAffected = old }()

	planplugin.IncludedColumnAffected = func(_ *planpb.IndexDef, colName string) (bool, error) {
		return colName == "status", nil
	}

	needsRewrite, err := Hooks{}.UpdateColumnRequiresIndexRewrite(&planpb.TableDef{}, &planpb.IndexDef{}, "status")
	require.NoError(t, err)
	require.True(t, needsRewrite)

	needsRewrite, err = Hooks{}.UpdateColumnRequiresIndexRewrite(&planpb.TableDef{}, &planpb.IndexDef{}, "body")
	require.NoError(t, err)
	require.False(t, needsRewrite)
}

// Compile-time assurance the plugin's Hooks satisfies every alter interface the
// SQL layer type-asserts (the missing wiring that caused #27964).
func TestFulltext2HooksImplementAlterInterfaces(t *testing.T) {
	var _ planplugin.AlterColumnHooks = Hooks{}
	var _ planplugin.RenameColumnRebuildHook = Hooks{}
	var _ planplugin.UpdateColumnRewriteHook = Hooks{}
}
