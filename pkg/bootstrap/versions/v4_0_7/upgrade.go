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

package v4_0_7

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var (
	Handler *versionHandle
)

func init() {
	Handler = &versionHandle{
		metadata: versions.Version{
			Version:           "4.0.7",
			MinUpgradeVersion: "4.0.6",
			UpgradeCluster:    versions.Yes,
			UpgradeTenant:     versions.Yes,
			VersionOffset:     uint32(len(tenantUpgEntries) + len(clusterUpgEntries)),
			// The final view definitions and writers require the current protocol.
			RequiredProtocolVersion: defines.MORPCVersion126,
		},
	}
}

type versionHandle struct {
	metadata versions.Version
}

func (v *versionHandle) Metadata() versions.Version {
	return v.metadata
}

func (v *versionHandle) Prepare(ctx context.Context, txn executor.TxnExecutor, final bool) error {
	txn.Use(catalog.MO_CATALOG)
	return nil
}

func (v *versionHandle) HandleTenantUpgrade(ctx context.Context, tenantID int32, txn executor.TxnExecutor) error {
	_, err := v.UpgradeTenant(ctx, tenantID, versions.DefaultCatalogUpgradeObjectBytes,
		func(_ string, _ uint64, _ bool, work func(executor.TxnExecutor) error) (bool, error) {
			return true, work(txn)
		})
	return err
}

func (v *versionHandle) TenantUpgradeEntryCount() int { return len(tenantUpgEntries) }

func (v *versionHandle) HandleClusterUpgrade(ctx context.Context, txn executor.TxnExecutor) error {
	for i := range clusterUpgEntries {
		for {
			more, err := v.UpgradeClusterEntryPage(ctx, i, txn, 64<<20)
			if err != nil {
				return err
			}
			if !more {
				break
			}
		}
	}
	getLogger(txn.Txn().TxnOptions().CN).Info("cluster upgrade success",
		zap.String("toVersion", v.Metadata().Version))
	return nil
}

func (v *versionHandle) ClusterUpgradeEntryCount() int { return len(clusterUpgEntries) }

func (v *versionHandle) UpgradeClusterEntryPage(_ context.Context, index int, txn executor.TxnExecutor, objectLimit uint64) (bool, error) {
	if index < 0 || index >= len(clusterUpgEntries) {
		return false, moerr.NewInvalidInputNoCtxf("invalid cluster upgrade entry %d", index)
	}
	entry := clusterUpgEntries[index]
	txn = limitedCatalogTxn{TxnExecutor: txn, limit: objectLimit}
	start := time.Now()
	if entry.TableName == catalog.MOSysAsyncTask && entry.UpgType == versions.MODIFY_METADATA {
		accountID := uint32(txn.Txn().TxnOptions().AccountID)
		done, err := entry.CheckFunc(txn, accountID)
		if err != nil || done {
			return false, err
		}
		if err := versions.CheckCommonProtocolVersion(txn, entry.RequiredProtocolVersion); err != nil {
			return false, err
		}
		sql := fmt.Sprintf("%s order by task_id limit %d", entry.UpgSql, CatalogDataPageSize)
		result, err := txn.Exec(sql, versions.UpgradeStatementOption(accountID))
		if err != nil {
			return false, err
		}
		affected := result.AffectedRows
		result.Close()
		if affected == 0 {
			return false, moerr.NewInvalidStateNoCtx("orphan task cleanup made no progress")
		}
		return true, nil
	}
	if err := entry.Upgrade(txn, uint32(txn.Txn().TxnOptions().AccountID)); err != nil {
		getLogger(txn.Txn().TxnOptions().CN).Error("cluster upgrade entry execute error",
			zap.Error(err), zap.String("version", v.Metadata().Version), zap.String("upgrade entry", entry.String()))
		return false, err
	}
	if entry.UpgType == versions.ADD_INDEX {
		ready, err := entry.CheckFunc(txn, uint32(txn.Txn().TxnOptions().AccountID))
		if err != nil {
			return false, err
		}
		if !ready {
			return false, &IndexConflict{fmt.Sprintf("index on %s.%s did not become ready", entry.Schema, entry.TableName)}
		}
	}
	getLogger(txn.Txn().TxnOptions().CN).Info("cluster upgrade entry complete",
		zap.String("upgrade entry", entry.String()), zap.Int64("time cost(ms)", time.Since(start).Milliseconds()), zap.String("toVersion", v.Metadata().Version))
	return false, nil
}

func (v *versionHandle) HandleCreateFrameworkDeps(txn executor.TxnExecutor) error {
	return moerr.NewInternalErrorNoCtxf("Only v1.2.0 can initialize upgrade framework, current version is:%s", Handler.metadata.Version)
}
