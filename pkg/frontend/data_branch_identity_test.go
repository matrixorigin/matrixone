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

package frontend

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestPrepareLogicalRestoreDatabaseIdentity(t *testing.T) {
	parent := context.WithValue(context.Background(), defines.DatTypKey{}, catalog.SystemDBTypeDataBranch)
	for _, databaseType := range []string{"", catalog.SystemDBTypeDataBranch, catalog.SystemDBTypeSubscription, "unknown"} {
		t.Run(databaseType, func(t *testing.T) {
			ctx := prepareLogicalRestoreDatabase(parent, logicalRestoreDatabaseDefinition{databaseType: databaseType})
			want := ""
			if databaseType == catalog.SystemDBTypeDataBranch {
				want = databaseType
			}
			require.Equal(t, want, ctx.Value(defines.DatTypKey{}))
			require.Equal(t, catalog.SystemDBTypeDataBranch, parent.Value(defines.DatTypKey{}))
		})
	}
}

func TestNewLogicalRestoreDatabaseDefinitionRequiresTypeColumn(t *testing.T) {
	for _, cols := range [][]string{nil, {"db"}, {"db", "create database db"}} {
		_, err := newLogicalRestoreDatabaseDefinition(context.Background(), "db", cols)
		require.Error(t, err)
	}
	definition, err := newLogicalRestoreDatabaseDefinition(context.Background(), "db", []string{"db", "create database db", catalog.SystemDBTypeDataBranch})
	require.NoError(t, err)
	require.Equal(t, "create database db", definition.createSQL)
	require.Equal(t, catalog.SystemDBTypeDataBranch, definition.databaseType)
}

func TestPreflightLogicalRestoreDatabasesStopsBeforeTargetWork(t *testing.T) {
	primary := errors.New("source identity read failed")
	var loaded []string
	err := preflightLogicalRestoreDatabases(context.Background(), []string{catalog.MO_CATALOG, "first", "bad", "later"}, func(name string) (logicalRestoreDatabaseDefinition, error) {
		loaded = append(loaded, name)
		if name == "bad" {
			return logicalRestoreDatabaseDefinition{}, primary
		}
		return logicalRestoreDatabaseDefinition{databaseType: catalog.SystemDBTypeDataBranch}, nil
	})
	require.ErrorIs(t, err, primary)
	require.Equal(t, []string{"first", "bad"}, loaded)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = preflightLogicalRestoreDatabases(ctx, []string{"db"}, func(string) (logicalRestoreDatabaseDefinition, error) {
		t.Fatal("cancelled preflight must not read or mutate a catalog")
		return logicalRestoreDatabaseDefinition{}, nil
	})
	require.Error(t, err)
}

func TestInstallDataBranchCloneContextRestoresRequestContext(t *testing.T) {
	original := context.Background()
	execCtx := &ExecCtx{reqCtx: original}
	func() {
		restore := installDataBranchCloneContext(execCtx, tree.NormalCloneLevelDatabase, catalog.SystemDBTypeDataBranch)
		defer restore()
		require.Equal(t, catalog.SystemDBTypeDataBranch, execCtx.reqCtx.Value(defines.DatTypKey{}))
		require.Equal(t, tree.NormalCloneLevelDatabase, execCtx.reqCtx.Value(tree.CloneLevelCtxKey{}))
		require.Equal(t, true, execCtx.reqCtx.Value(dataBranchCloneLockCtxKey{}))
	}()
	require.Equal(t, original, execCtx.reqCtx)
	require.Nil(t, execCtx.reqCtx.Value(defines.DatTypKey{}))
	require.Nil(t, execCtx.reqCtx.Value(dataBranchCloneLockCtxKey{}))

	restore := installDataBranchCloneContext(execCtx, tree.NormalCloneLevelTable, "")
	require.Empty(t, execCtx.reqCtx.Value(defines.DatTypKey{}))
	restore()
	require.Equal(t, original, execCtx.reqCtx)
}
