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

package frontend

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func setProtocolVersionForTest(t testing.TB, service string, version int64) {
	t.Helper()
	rt := runtime.ServiceRuntime(service)
	previous, hadPrevious := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	rt.SetGlobalVariables(runtime.MOProtocolVersion, version)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(runtime.MOProtocolVersion, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(runtime.MOProtocolVersion, version)
		}
	})
}

func TestDataBranchDatabaseIdentityCapability(t *testing.T) {
	ctx := context.Background()
	require.False(t, dataBranchDatabaseIdentitySupported(defines.MORPCVersion63))
	require.True(t, dataBranchDatabaseIdentitySupported(defines.MORPCVersion64))
	require.ErrorContains(t,
		requireDataBranchDatabaseIdentity(ctx, defines.MORPCVersion63),
		"requires MORPC protocol version 64",
	)
	require.NoError(t, requireDataBranchDatabaseIdentity(ctx, defines.MORPCVersion64))
}

func TestDataBranchCreateDatabaseRejectsBeforeExecutorBelowCapability(t *testing.T) {
	ses := newValidateSession(t)
	setProtocolVersionForTest(t, ses.proc.GetService(), defines.MORPCVersion63)

	_, err := dataBranchCreateDatabase(
		&ExecCtx{reqCtx: context.Background()},
		ses,
		&tree.DataBranchCreateDatabase{},
	)
	require.ErrorContains(t, err, "requires MORPC protocol version 64")
}

func TestPrepareLogicalRestoreDatabaseCapability(t *testing.T) {
	ctx := context.Background()
	ordinary := logicalRestoreDatabaseDefinition{createSQL: "create database ordinary"}
	marked := logicalRestoreDatabaseDefinition{
		createSQL:    "create database branch_db",
		databaseType: catalog.SystemDBTypeDataBranch,
	}

	for _, version := range []int64{defines.MORPCVersion63, defines.MORPCVersion64} {
		prepared, err := prepareLogicalRestoreDatabase(ctx, "ordinary", ordinary, version)
		require.NoError(t, err)
		require.Empty(t, prepared.Value(defines.DatTypKey{}))
	}

	prepared, err := prepareLogicalRestoreDatabase(ctx, "branch_db", marked, defines.MORPCVersion63)
	require.ErrorContains(t, err, "restoring data-branch database 'branch_db' requires MORPC protocol version 64")
	require.Nil(t, prepared)

	prepared, err = prepareLogicalRestoreDatabase(ctx, "branch_db", marked, defines.MORPCVersion64)
	require.NoError(t, err)
	require.Equal(t, catalog.SystemDBTypeDataBranch, prepared.Value(defines.DatTypKey{}))
}

func TestNewLogicalRestoreDatabaseDefinitionRequiresTypeColumn(t *testing.T) {
	_, err := newLogicalRestoreDatabaseDefinition(
		context.Background(), "db1", []string{"db1", "create database db1"},
	)
	require.Error(t, err)

	definition, err := newLogicalRestoreDatabaseDefinition(
		context.Background(), "db1",
		[]string{"db1", "create database db1", catalog.SystemDBTypeDataBranch},
	)
	require.NoError(t, err)
	require.Equal(t, "create database db1", definition.createSQL)
	require.Equal(t, catalog.SystemDBTypeDataBranch, definition.databaseType)
}

func TestPreflightLogicalRestoreDatabases(t *testing.T) {
	ctx := context.Background()
	dbNames := []string{moCatalog, "ordinary", "branch_db", "not_reached"}
	var loaded []string
	load := func(dbName string) (logicalRestoreDatabaseDefinition, error) {
		loaded = append(loaded, dbName)
		definition := logicalRestoreDatabaseDefinition{createSQL: "create database " + dbName}
		if dbName == "branch_db" {
			definition.databaseType = catalog.SystemDBTypeDataBranch
		}
		return definition, nil
	}

	err := preflightLogicalRestoreDatabases(ctx, dbNames, defines.MORPCVersion63, load)
	require.ErrorContains(t, err, "restoring data-branch database 'branch_db' requires MORPC protocol version 64")
	require.Equal(t, []string{"ordinary", "branch_db"}, loaded)

	loaded = nil
	require.NoError(t, preflightLogicalRestoreDatabases(ctx, dbNames, defines.MORPCVersion64, load))
	require.Empty(t, loaded)
}
