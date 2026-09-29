// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package v4_0_11

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var Handler = &versionHandle{metadata: versions.Version{
	Version:                 "4.0.11",
	MinUpgradeVersion:       "4.0.10",
	UpgradeCluster:          versions.Yes,
	UpgradeTenant:           versions.No,
	VersionOffset:           uint32(len(clusterUpgEntries)),
	RequiredProtocolVersion: defines.MORPCVersion101,
}}

type versionHandle struct{ metadata versions.Version }

func (v *versionHandle) Metadata() versions.Version { return v.metadata }

func (v *versionHandle) Prepare(_ context.Context, txn executor.TxnExecutor, _ bool) error {
	txn.Use(catalog.MO_CATALOG)
	return nil
}

func (v *versionHandle) HandleTenantUpgrade(_ context.Context, _ int32, _ executor.TxnExecutor) error {
	return nil
}

func (v *versionHandle) HandleClusterUpgrade(_ context.Context, txn executor.TxnExecutor) error {
	for _, entry := range clusterUpgEntries {
		if err := entry.Upgrade(txn, catalog.System_Account); err != nil {
			return err
		}
	}
	return nil
}

func (v *versionHandle) HandleCreateFrameworkDeps(_ executor.TxnExecutor) error {
	return moerr.NewInternalErrorNoCtxf("Only v1.2.0 can initialize upgrade framework, current version is:%s", v.metadata.Version)
}
