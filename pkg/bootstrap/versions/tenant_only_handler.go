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

package versions

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// TenantOnlyHandler executes version-specific tenant upgrades without cluster upgrades.
type TenantOnlyHandler struct {
	metadata Version
	entries  *[]UpgradeEntry
}

func NewTenantOnlyHandler(metadata Version, entries *[]UpgradeEntry) *TenantOnlyHandler {
	return &TenantOnlyHandler{metadata: metadata, entries: entries}
}

func (v *TenantOnlyHandler) Metadata() Version { return v.metadata }

func (v *TenantOnlyHandler) Prepare(_ context.Context, txn executor.TxnExecutor, _ bool) error {
	txn.Use(catalog.MO_CATALOG)
	return nil
}

func (v *TenantOnlyHandler) HandleTenantUpgrade(_ context.Context, tenantID int32, txn executor.TxnExecutor) error {
	logger := runtime.ServiceRuntime(txn.Txn().TxnOptions().CN).Logger()
	return RunTenantUpgradeEntriesWithLogger(tenantID, txn, *v.entries, logger, func() string { return v.metadata.Version })
}

func (v *TenantOnlyHandler) HandleClusterUpgrade(_ context.Context, _ executor.TxnExecutor) error {
	return nil
}

func (v *TenantOnlyHandler) HandleCreateFrameworkDeps(_ executor.TxnExecutor) error {
	return moerr.NewInternalErrorNoCtxf("Only v1.2.0 can initialize upgrade framework, current version is:%s", v.metadata.Version)
}
