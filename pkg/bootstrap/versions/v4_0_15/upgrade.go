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

package v4_0_15

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var Handler = &versionHandle{
	metadata: versions.Version{
		Version:                 "4.0.15",
		MinUpgradeVersion:       "4.0.14",
		UpgradeCluster:          versions.Yes,
		UpgradeTenant:           versions.No,
		VersionOffset:           uint32(len(catalog.MaterializedViewCatalogDDLs)),
		RequiredProtocolVersion: defines.MORPCVersion113,
	},
}

type versionHandle struct {
	metadata versions.Version
}

func (v *versionHandle) Metadata() versions.Version {
	return v.metadata
}

func (v *versionHandle) Prepare(_ context.Context, txn executor.TxnExecutor, _ bool) error {
	txn.Use(catalog.MO_CATALOG)
	return nil
}

func (v *versionHandle) HandleTenantUpgrade(_ context.Context, tenantID int32, txn executor.TxnExecutor) error {
	return nil
}

func (v *versionHandle) HandleClusterUpgrade(_ context.Context, tx executor.TxnExecutor) error {
	for _, ddl := range catalog.MaterializedViewCatalogDDLs {
		ddl = strings.Replace(ddl, "create cluster table ", "create cluster table if not exists ", 1)
		result, err := tx.Exec(ddl, executor.StatementOption{}.WithAccountID(catalog.System_Account))
		if err != nil {
			return err
		}
		result.Close()
	}
	return nil
}

func (v *versionHandle) HandleCreateFrameworkDeps(_ executor.TxnExecutor) error {
	return moerr.NewInternalErrorNoCtxf("Only v1.2.0 can initialize upgrade framework, current version is:%s", v.metadata.Version)
}
