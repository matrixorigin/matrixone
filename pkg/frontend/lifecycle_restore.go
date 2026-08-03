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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	lifecyclepkg "github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/lifecycle"
)

const (
	lifecycleMaxRestoreStagingBytesPerAccount = uint64(12) << 40
	lifecycleArchiveCloseTimeout              = 30 * time.Second
	lifecycleMaxConcurrentRestoresPerCN       = 1
)

var lifecycleRestoreSlots = make(
	chan struct{},
	lifecycleMaxConcurrentRestoresPerCN,
)

func handleRestoreArchiveDataset(
	ctx context.Context,
	ses *Session,
	statement *tree.RestoreArchiveDataset,
) error {
	if statement == nil || statement.Target == nil {
		return fmt.Errorf("Lifecycle Restore target is required")
	}
	releaseRestore, acquired := tryAcquireLifecycleRestoreSlot(
		lifecycleRestoreSlots,
	)
	if !acquired {
		metricv2.LifecycleResourceRejectionCounter.WithLabelValues(
			"restore_cn_concurrency",
		).Inc()
		return moerr.NewInternalError(
			ctx,
			"RESOURCE_BUSY: this CN is already running the certified number of Lifecycle Restores",
		)
	}
	defer releaseRestore()
	background := ses.GetBackgroundExec(ctx)
	defer background.Close()
	if err := ensureLifecycleFeatureEnabled(ctx, ses, background); err != nil {
		return err
	}
	sqlExecutor, err := lifecycleSQLExecutor(ses.GetService())
	if err != nil {
		return err
	}
	accountID := ses.GetTenantInfo().GetTenantID()
	datasetReader := lifecyclepkg.SQLDatasetReader{
		Executor: sqlExecutor,
	}
	dataset, err := datasetReader.GetRestoreDataset(
		ctx,
		accountID,
		statement.DatasetID,
	)
	if err != nil {
		return err
	}
	databaseName := string(statement.Target.Schema())
	if databaseName == "" {
		databaseName = ses.GetDatabaseName()
	}
	tableName := string(statement.Target.Name())
	if databaseName == "" || tableName == "" {
		return fmt.Errorf("Lifecycle Restore target database and table are required")
	}
	if err := validateLifecycleRestoreTargetName(tableName); err != nil {
		return err
	}
	databaseID, err := lifecycleDatabaseID(
		ctx,
		sqlExecutor,
		accountID,
		databaseName,
	)
	if err != nil {
		return err
	}
	repository := disttae.SQLRestoreRepository{
		AccountID:          accountID,
		TargetDatabaseName: databaseName,
		Executor:           sqlExecutor,
		Engine:             getPu(ses.GetService()).StorageEngine,
		MPool:              ses.proc.Mp(),
		AutoIncrement: incrservice.GetAutoIncrementService(
			ses.GetService(),
		),
		Roots: lifecyclepkg.SQLCleanupRootRepository{
			Executor: sqlExecutor,
		},
		MaxRestoreStagingBytesPerAccount: lifecycleMaxRestoreStagingBytesPerAccount,
	}
	restoreAttempt, resumed, err := repository.FindResumable(
		ctx,
		dataset.DatasetID,
		databaseID,
		tableName,
	)
	if err != nil {
		return err
	}
	if lifecycleRestoreAlreadyPublished(resumed, restoreAttempt.State) {
		return nil
	}
	if !resumed {
		restoreID := uuid.NewString()
		restoreAttempt = lifecyclepkg.RestoreAttempt{
			RestoreID:         restoreID,
			LeaseID:           uuid.NewString(),
			Deadline:          time.Now().Add(24 * time.Hour),
			StagingDatabaseID: databaseID,
			HiddenName: catalog.LifecycleRestoreTableNamePrefix +
				strings.ReplaceAll(restoreID, "-", ""),
			TargetDatabaseID:   databaseID,
			TargetDatabaseName: databaseName,
			TargetName:         tableName,
		}
	}
	target, err := lifecyclepkg.ParseFrozenArchiveTarget(dataset.StageIdentity)
	if err != nil {
		return err
	}
	if target.StageID != dataset.StageID {
		return fmt.Errorf("Lifecycle Dataset Stage identity mismatch")
	}
	archiveFS, err := lifecyclepkg.NewArchiveFileService(ctx, target)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancelClose := context.WithTimeout(
			context.WithoutCancel(ctx),
			lifecycleArchiveCloseTimeout,
		)
		defer cancelClose()
		archiveFS.Close(closeCtx)
	}()

	coordinator := newLifecycleRestoreCoordinator(
		lifecyclepkg.FileServiceArchiveStore{
			FileService:    archiveFS,
			MaxListEntries: 100_000,
		},
		repository,
	)
	return coordinator.Restore(
		ctx,
		dataset,
		restoreAttempt,
	)
}

func tryAcquireLifecycleRestoreSlot(
	slots chan struct{},
) (func(), bool) {
	select {
	case slots <- struct{}{}:
		metricv2.LifecycleActiveRestoreGauge.Inc()
		var once sync.Once
		return func() {
			once.Do(func() {
				<-slots
				metricv2.LifecycleActiveRestoreGauge.Dec()
			})
		}, true
	default:
		return nil, false
	}
}

func newLifecycleRestoreCoordinator(
	store lifecyclepkg.ArchiveStore,
	repository lifecyclepkg.RestoreRepository,
) lifecyclepkg.RestoreCoordinator {
	return lifecyclepkg.RestoreCoordinator{
		Store:      store,
		Repository: repository,
		Config: lifecyclepkg.RestoreConfig{
			MaxChunkRows:         65_536,
			MaxChunkLogicalBytes: 64 << 20,
			Deadline:             24 * time.Hour,
		},
		Faults: lifecyclepkg.MOFaultInjector{},
	}
}

func validateLifecycleRestoreTargetName(tableName string) error {
	if catalog.IsLifecycleRestoreStagingTable(tableName) {
		return fmt.Errorf(
			"Lifecycle Restore target cannot use the reserved canonical staging name %s",
			tableName,
		)
	}
	return nil
}

func lifecycleRestoreAlreadyPublished(resumed bool, state string) bool {
	return resumed && state == "DONE"
}

func handlePurgeArchiveDataset(
	ctx context.Context,
	ses *Session,
	statement *tree.PurgeArchiveDataset,
) error {
	if statement == nil {
		return fmt.Errorf("Lifecycle Purge input is required")
	}
	sqlExecutor, err := lifecycleSQLExecutor(ses.GetService())
	if err != nil {
		return err
	}
	accountID := ses.GetTenantInfo().GetTenantID()
	dataset, err := (lifecyclepkg.SQLDatasetReader{
		Executor: sqlExecutor,
	}).GetRestoreDataset(ctx, accountID, statement.DatasetID)
	if err != nil {
		return err
	}
	repository := disttae.SQLRestoreRepository{
		AccountID: accountID,
		Executor:  sqlExecutor,
		Roots: lifecyclepkg.SQLCleanupRootRepository{
			Executor: sqlExecutor,
		},
	}
	return (lifecyclepkg.RestoreCoordinator{
		Repository: repository,
	}).Purge(ctx, dataset, time.Now())
}

func lifecycleSQLExecutor(service string) (executor.SQLExecutor, error) {
	value, ok := moruntime.ServiceRuntime(service).GetGlobalVariables(
		moruntime.InternalSQLExecutor,
	)
	if !ok {
		return nil, fmt.Errorf("Lifecycle internal SQL executor is unavailable")
	}
	sqlExecutor, ok := value.(executor.SQLExecutor)
	if !ok || sqlExecutor == nil {
		return nil, fmt.Errorf("Lifecycle internal SQL executor has invalid type")
	}
	return sqlExecutor, nil
}

func lifecycleDatabaseID(
	ctx context.Context,
	sqlExecutor executor.SQLExecutor,
	accountID uint32,
	databaseName string,
) (uint64, error) {
	result, err := sqlExecutor.Exec(
		ctx,
		fmt.Sprintf(
			"select dat_id from mo_catalog.mo_database where datname=%s",
			quoteSQLStringLiteral(databaseName),
		),
		executor.Options{}.WithAccountID(accountID),
	)
	if err != nil {
		return 0, err
	}
	defer result.Close()
	var databaseID uint64
	rowsRead := 0
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		if len(columns) != 1 || rowsRead+rows != 1 {
			return false
		}
		databaseID = vector.GetFixedAtNoTypeCheck[uint64](columns[0], 0)
		rowsRead += rows
		return true
	})
	if rowsRead != 1 || databaseID == 0 {
		return 0, fmt.Errorf(
			"Lifecycle Restore target database %s does not exist",
			databaseName,
		)
	}
	return databaseID, nil
}
