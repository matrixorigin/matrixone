// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package v4_0_8

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// Handler makes the SQL task catalog changes a distinct upgrade generation.
var Handler = &versionHandle{metadata: versions.Version{
	Version: "4.0.8", MinUpgradeVersion: "4.0.7",
	UpgradeCluster: versions.Yes, UpgradeTenant: versions.Yes,
	VersionOffset:           uint32(len(v4_0_7.SQLTaskOwnershipUpgradeEntries())),
	RequiredProtocolVersion: defines.MORPCVersion126,
}}

type versionHandle struct{ metadata versions.Version }

func (v *versionHandle) Metadata() versions.Version { return v.metadata }

func (v *versionHandle) Prepare(_ context.Context, txn executor.TxnExecutor, _ bool) error {
	txn.Use(catalog.MO_CATALOG)
	return nil
}

func (v *versionHandle) HandleClusterUpgrade(_ context.Context, txn executor.TxnExecutor) error {
	for _, entry := range v4_0_7.SQLTaskOwnershipUpgradeEntries() {
		if err := entry.Upgrade(txn, uint32(txn.Txn().TxnOptions().AccountID)); err != nil {
			return err
		}
	}
	return nil
}

func (v *versionHandle) HandleTenantUpgrade(context.Context, int32, executor.TxnExecutor) error {
	return nil
}

func (v *versionHandle) HandleCreateFrameworkDeps(executor.TxnExecutor) error {
	return moerr.NewInternalErrorNoCtx("only the founding catalog version initializes upgrade framework dependencies")
}
