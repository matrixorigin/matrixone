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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
)

func dataBranchDatabaseIdentitySupported(protocolVersion int64) bool {
	return protocolVersion >= defines.MORPCVersion62
}

func requireDataBranchDatabaseIdentity(ctx context.Context, protocolVersion int64) error {
	if dataBranchDatabaseIdentitySupported(protocolVersion) {
		return nil
	}
	return moerr.NewInternalErrorf(
		ctx,
		"DATA BRANCH CREATE DATABASE requires MORPC protocol version %d",
		defines.MORPCVersion62,
	)
}

func dataBranchDatabaseIdentityActive(databaseType string, protocolVersion int64) bool {
	return databaseType == catalog.SystemDBTypeDataBranch &&
		dataBranchDatabaseIdentitySupported(protocolVersion)
}

type logicalRestoreDatabaseDefinition struct {
	createSQL    string
	databaseType string
}

type logicalRestoreDatabaseDefinitionLoader func(string) (logicalRestoreDatabaseDefinition, error)

func newLogicalRestoreDatabaseDefinition(ctx context.Context, dbName string, cols []string) (logicalRestoreDatabaseDefinition, error) {
	if len(cols) < 3 {
		return logicalRestoreDatabaseDefinition{}, moerr.NewBadDB(ctx, dbName)
	}
	return logicalRestoreDatabaseDefinition{
		createSQL:    cols[1],
		databaseType: cols[2],
	}, nil
}

// prepareLogicalRestoreDatabase validates persistent identity before any
// destructive restore work and returns the context used by internal CREATE
// DATABASE. Only the catalog-owned data-branch marker is propagated.
func prepareLogicalRestoreDatabase(
	ctx context.Context,
	dbName string,
	definition logicalRestoreDatabaseDefinition,
	protocolVersion int64,
) (context.Context, error) {
	if definition.databaseType != catalog.SystemDBTypeDataBranch {
		return ctx, nil
	}
	if !dataBranchDatabaseIdentitySupported(protocolVersion) {
		return nil, moerr.NewInternalErrorf(
			ctx,
			"restoring data-branch database '%s' requires MORPC protocol version %d",
			dbName,
			defines.MORPCVersion62,
		)
	}
	return context.WithValue(ctx, defines.DatTypKey{}, catalog.SystemDBTypeDataBranch), nil
}

// preflightLogicalRestoreDatabases checks every restorable database identity
// before account-level restore starts replacing the current catalog. Once the
// common protocol owns data-branch identity, the per-database restore path
// still reads and propagates the marker immediately before CREATE DATABASE.
func preflightLogicalRestoreDatabases(
	ctx context.Context,
	dbNames []string,
	protocolVersion int64,
	load logicalRestoreDatabaseDefinitionLoader,
) error {
	if dataBranchDatabaseIdentitySupported(protocolVersion) {
		return nil
	}
	for _, dbName := range dbNames {
		if needSkipDb(dbName) {
			continue
		}
		definition, err := load(dbName)
		if err != nil {
			return err
		}
		if _, err = prepareLogicalRestoreDatabase(ctx, dbName, definition, protocolVersion); err != nil {
			return err
		}
	}
	return nil
}
