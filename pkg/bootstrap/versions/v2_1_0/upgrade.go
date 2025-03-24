// Copyright 2025 Matrix Origin
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

package v2_1_0

import (
	"context"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"go.uber.org/zap"
)

var (
	Handler = &versionHandle{
		metadata: versions.Version{
			Version:           "2.1.0",
			MinUpgradeVersion: "2.0.3",
			UpgradeCluster:    versions.Yes,
			UpgradeTenant:     versions.Yes,
			VersionOffset:     uint32(len(clusterUpgEntries) + len(tenantUpgEntries)),
		},
	}
)

type versionHandle struct {
	metadata versions.Version
}

func (v *versionHandle) Metadata() versions.Version {
	return v.metadata
}

func (v *versionHandle) Prepare(
	ctx context.Context,
	txn executor.TxnExecutor,
	final bool) error {
	txn.Use(catalog.MO_CATALOG)
	return nil
}

func (v *versionHandle) HandleTenantUpgrade(
	ctx context.Context,
	tenantID int32,
	txn executor.TxnExecutor) error {

	for _, upgEntry := range tenantUpgEntries {
		start := time.Now()

		err := upgEntry.Upgrade(txn, uint32(tenantID))
		if err != nil {
			getLogger(txn.Txn().TxnOptions().CN).Error("tenant upgrade entry execute error", zap.Error(err), zap.Int32("tenantId", tenantID), zap.String("version", v.Metadata().Version), zap.String("upgrade entry", upgEntry.String()))
			return err
		}

		duration := time.Since(start)
		getLogger(txn.Txn().TxnOptions().CN).Info("tenant upgrade entry complete",
			zap.String("upgrade entry", upgEntry.String()),
			zap.Int64("time cost(ms)", duration.Milliseconds()),
			zap.Int32("tenantId", tenantID),
			zap.String("toVersion", v.Metadata().Version))
	}

	return nil
}

func (v *versionHandle) HandleClusterUpgrade(
	ctx context.Context,
	txn executor.TxnExecutor) error {
	for _, upgEntry := range clusterUpgEntries {
		start := time.Now()

		err := upgEntry.Upgrade(txn, catalog.System_Account)
		if err != nil {
			getLogger(txn.Txn().TxnOptions().CN).Error("cluster upgrade entry execute error", zap.Error(err), zap.String("version", v.Metadata().Version), zap.String("upgrade entry", upgEntry.String()))
			return err
		}

		duration := time.Since(start)
		getLogger(txn.Txn().TxnOptions().CN).Info("cluster upgrade entry complete",
			zap.String("upgrade entry", upgEntry.String()),
			zap.Int64("time cost(ms)", duration.Milliseconds()),
			zap.String("toVersion", v.Metadata().Version))
	}
	return nil
}

func (v *versionHandle) HandleCreateFrameworkDeps(txn executor.TxnExecutor) error {
	return moerr.NewInternalErrorNoCtxf("Only v1.2.0 can initialize upgrade framework, current version is:%s", Handler.metadata.Version)
}

func upgradeForBM25(accountId uint32, txn executor.TxnExecutor) error {

	getLogger(txn.Txn().TxnOptions().CN).Info("begin upgrade for BM25",
		zap.Uint32("accountId", accountId))

	sql := `select distinct b.datname, a.index_table_name from mo_catalog.mo_indexes a 
	join mo_catalog.mo_database b on a.database_id = b.dat_id where a.algo = 'fulltext'`

	res, err := txn.Exec(sql, executor.StatementOption{}.WithAccountID(accountId))
	if err != nil {
		return err
	}
	defer res.Close()

	for _, batch := range res.Batches {
		for i := 0; i < batch.RowCount(); i++ {
			dbName := batch.Vecs[0].GetStringAt(i)
			idxTable := batch.Vecs[1].GetStringAt(i)

			checkSQL := fmt.Sprintf("select count(*) from `%s`.`%s` where word = '%s'", dbName, idxTable, fulltext.DOC_LEN_WORD)
			checkRes, checkErr := txn.Exec(checkSQL, executor.StatementOption{}.WithAccountID(accountId))
			if checkErr != nil {
				return checkErr
			}
			defer checkRes.Close()
			if len(checkRes.Batches) == 0 {
				continue
			}
			rowCount := vector.GetFixedAtNoTypeCheck[uint64](checkRes.Batches[0].Vecs[0], 0)
			if rowCount > 0 {
				continue
			}

			insertSQL := fmt.Sprintf("insert into `%s`.`%s` select doc_id, count(*), '%s' from `%s`.`%s` group by doc_id", dbName, idxTable, fulltext.DOC_LEN_WORD, dbName, idxTable)
			insertRes, insertErr := txn.Exec(insertSQL, executor.StatementOption{}.WithAccountID(accountId))
			if insertErr != nil {
				return insertErr
			}
			defer insertRes.Close()

			getLogger(txn.Txn().TxnOptions().CN).Info("insert doclen for BM25",
				zap.String("db", dbName),
				zap.String("index table", idxTable))
		}
	}

	getLogger(txn.Txn().TxnOptions().CN).Info("finish upgrade for BM25",
		zap.Uint32("accountId", accountId))
	return nil
}
