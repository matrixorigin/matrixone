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

package test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	lifecyclepkg "github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/lifecycle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/require"
)

func TestLifecycleRestoreNoPKTableThroughCNToTN(t *testing.T) {
	const (
		accountID         = uint32(1)
		databaseName      = "lifecycle_restore_fakepk"
		hiddenName        = catalog.LifecycleRestoreTableNamePrefix + "11111111111111111111111111111111"
		targetName        = "restored_events"
		restoreID         = "11111111-1111-1111-1111-111111111111"
		datasetID         = "22222222-2222-2222-2222-222222222222"
		leaseID           = "33333333-3333-3333-3333-333333333333"
		rootID            = "44444444-4444-4444-4444-444444444444"
		archiveAttemptID  = "55555555-5555-5555-5555-555555555555"
		bindingID         = "66666666-6666-6666-6666-666666666666"
		datasetLogicalCap = uint64(1 << 20)
	)

	baseCtx := context.WithValue(context.Background(), defines.TenantIDKey{}, accountID)
	disttaeEngine, taeEngine, rpcAgent, mp := testutil.CreateEngines(
		baseCtx,
		testutil.TestOptions{},
		t,
	)
	defer func() {
		disttaeEngine.Close(baseCtx)
		require.NoError(t, taeEngine.Close(true))
		rpcAgent.Close()
	}()

	ctx, cancel := context.WithTimeout(baseCtx, 2*time.Minute)
	defer cancel()
	systemCtx := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)

	autoIncrement := incrservice.NewIncrService(
		"",
		incrservice.NewMemStore(),
		incrservice.Config{CountPerAllocate: 32},
	)
	previousAutoIncrement := incrservice.GetAutoIncrementService("")
	if previousAutoIncrement == nil {
		previousAutoIncrement = NewMockAutoIncrementService("restore-test-fallback")
	}
	incrservice.SetAutoIncrementServiceByID("", autoIncrement)
	defer func() {
		incrservice.SetAutoIncrementServiceByID("", previousAutoIncrement)
		autoIncrement.Close()
	}()

	value, ok := runtime.ServiceRuntime("").GetGlobalVariables(runtime.InternalSQLExecutor)
	require.True(t, ok)
	sqlExecutor, ok := value.(executor.SQLExecutor)
	require.True(t, ok)

	// The test engine bootstraps only the core catalog. Keep this fixture local
	// and create exactly the system and tenant tables used by Restore.
	mustExecLifecycleRestoreSQL(
		t,
		ctx,
		sqlExecutor,
		accountID,
		frontend.MoCatalogMoIndexesDDL,
	)
	mustExecLifecycleRestoreSQL(
		t,
		ctx,
		sqlExecutor,
		accountID,
		frontend.MoCatalogMoForeignKeysDDL,
	)
	mustExecLifecycleRestoreSQL(
		t,
		systemCtx,
		sqlExecutor,
		catalog.System_Account,
		frontend.MoCatalogMoIndexesDDL,
	)
	mustExecLifecycleRestoreSQL(
		t,
		systemCtx,
		sqlExecutor,
		catalog.System_Account,
		frontend.MoCatalogMoAccountDDL,
	)
	mustExecLifecycleRestoreSQL(
		t,
		systemCtx,
		sqlExecutor,
		catalog.System_Account,
		`insert into mo_catalog.mo_account(
account_id,account_name,admin_name,status,created_time,comments,version,create_version)
values(1,'lifecycle_restore_test','admin','open',utc_timestamp(),'test fixture',1,'test')`,
	)
	mustExecLifecycleRestoreSQL(
		t,
		systemCtx,
		sqlExecutor,
		catalog.System_Account,
		frontend.MoCatalogMoISCPLogDDL,
	)
	mustExecLifecycleRestoreSQL(
		t,
		systemCtx,
		sqlExecutor,
		catalog.System_Account,
		frontend.MoCatalogMoIndexUpdateDDL,
	)
	mustExecLifecycleRestoreSQL(
		t,
		systemCtx,
		sqlExecutor,
		catalog.System_Account,
		frontend.MoCatalogFeatureRegistryDDL,
	)
	mustExecLifecycleRestoreSQL(
		t,
		systemCtx,
		sqlExecutor,
		catalog.System_Account,
		catalog.MoLifecycleCleanupRootsDDL,
	)
	mustExecLifecycleRestoreSQL(
		t,
		systemCtx,
		sqlExecutor,
		catalog.System_Account,
		`insert into mo_catalog.mo_feature_registry(
feature_code,description,scope_spec,enabled)
values('LIFECYCLE','test','{"allowed_scope":[]}',true)`,
	)

	for _, ddl := range []string{
		frontend.MoCatalogMoTablePartitionsDDL,
		frontend.MoCatalogMoAutoIncrTableDDL,
		catalog.MoLifecycleDatasetsDDL,
		catalog.MoLifecycleRestoreAttemptsDDL,
		catalog.MoLifecycleRestoreChunksDDL,
	} {
		mustExecLifecycleRestoreSQL(t, ctx, sqlExecutor, accountID, ddl)
	}
	mustExecLifecycleRestoreSQL(
		t,
		ctx,
		sqlExecutor,
		accountID,
		"create database "+databaseName,
	)
	databaseID := queryLifecycleRestoreUint64(
		t,
		ctx,
		sqlExecutor,
		accountID,
		fmt.Sprintf(
			"select dat_id from mo_catalog.mo_database where datname='%s'",
			databaseName,
		),
	)

	schema := lifecyclepkg.SchemaDescriptor{
		FormatVersion:      1,
		SourceTableID:      901,
		SourceTableVersion: 1,
		SourceDatabaseName: "source_db",
		SourceTableName:    "events",
		Columns: []lifecyclepkg.SchemaColumn{
			{
				Ordinal:        0,
				SourceColumnID: 1,
				Name:           "id",
				TypeID:         int32(types.T_uint64),
				NotNull:        true,
				AutoIncrement:  true,
			},
			{
				Ordinal:        1,
				SourceColumnID: 2,
				Name:           "payload",
				TypeID:         int32(types.T_int64),
				NotNull:        true,
			},
		},
	}
	schemaDigest, err := schema.Digest()
	require.NoError(t, err)
	verifiedHash := sha256.Sum256([]byte("Lifecycle Restore fake-PK integration"))
	zeroDigest := [sha256.Size]byte{}
	mustExecLifecycleRestoreSQL(
		t,
		ctx,
		sqlExecutor,
		accountID,
		fmt.Sprintf(
			`insert into mo_catalog.mo_lifecycle_datasets(
dataset_id,account_id,binding_id,binding_generation,logical_table_id,
source_physical_table_id,source_snapshot_ts,evaluation_time,cutoff,
source_set_digest,schema_descriptor_digest,lifecycle_column_id,
lifecycle_column_type,lifecycle_min,lifecycle_max,
root_id,attempt_id,manifest_key,manifest_sha256,content_hash,row_count,
logical_bytes,stage_id,stage_identity_blob,purge_eligible_at,state,version,
access_generation,restore_lease_id,restore_deadline,publish_txn_id,
created_at,updated_at)
values(unhex('%s'),%d,unhex('%s'),1,900,901,x'01',utc_timestamp(),
utc_timestamp(),unhex('%s'),unhex('%s'),1,%d,0,1,unhex('%s'),unhex('%s'),
'manifest.json',unhex('%s'),unhex('%s'),4,%d,7,x'01',
date_add(utc_timestamp(),interval 1 day),'PUBLISHED',1,1,null,null,x'01',
utc_timestamp(),utc_timestamp())`,
			lifecycleRestoreTestUUIDHex(datasetID),
			accountID,
			lifecycleRestoreTestUUIDHex(bindingID),
			hex.EncodeToString(zeroDigest[:]),
			hex.EncodeToString(schemaDigest[:]),
			int32(types.T_timestamp),
			lifecycleRestoreTestUUIDHex(rootID),
			lifecycleRestoreTestUUIDHex(archiveAttemptID),
			hex.EncodeToString(zeroDigest[:]),
			hex.EncodeToString(verifiedHash[:]),
			datasetLogicalCap,
		),
	)

	hiddenCreateSQL, err := schema.BuildRestoreCreateTableSQL(
		ctx,
		databaseName,
		hiddenName,
	)
	require.NoError(t, err)
	repository := disttae.SQLRestoreRepository{
		AccountID:                        accountID,
		TargetDatabaseName:               databaseName,
		Executor:                         sqlExecutor,
		Engine:                           disttaeEngine.Engine,
		MPool:                            mp,
		AutoIncrement:                    autoIncrement,
		MaxRestoreStagingBytesPerAccount: 2 * datasetLogicalCap,
	}
	dataset := lifecyclepkg.RestoreDataset{
		DatasetID:    datasetID,
		AccountID:    accountID,
		ContentHash:  verifiedHash,
		RowCount:     4,
		LogicalBytes: datasetLogicalCap,
		Version:      1,
		State:        "PUBLISHED",
	}
	initialAttempt := lifecyclepkg.RestoreAttempt{
		RestoreID:            restoreID,
		DatasetID:            datasetID,
		DatasetIDs:           []string{datasetID},
		LeaseID:              leaseID,
		Deadline:             time.Now().UTC().Add(time.Minute).Truncate(time.Microsecond),
		StagingDatabaseID:    databaseID,
		HiddenName:           hiddenName,
		TargetDatabaseID:     databaseID,
		TargetDatabaseName:   databaseName,
		TargetName:           targetName,
		Scope:                lifecyclepkg.RestoreScopeDataset,
		SourceLogicalTableID: 900,
		TotalChunkCount:      2,
		SelectedLogicalBytes: datasetLogicalCap,
	}
	initialAttempt.SelectionDigest = lifecyclepkg.BuildRestoreSelectionDigest(
		initialAttempt.Scope,
		initialAttempt.SourceLogicalTableID,
		0,
		0,
		initialAttempt.DatasetIDs,
	)
	attempt, err := repository.Initialize(
		ctx,
		lifecyclepkg.RestoreInitializeRequest{
			Dataset:         dataset,
			Datasets:        []lifecyclepkg.RestoreDataset{dataset},
			Attempt:         initialAttempt,
			HiddenCreateSQL: hiddenCreateSQL,
		},
	)
	require.NoError(t, err)
	require.Equal(t, "IMPORTING", attempt.State)
	require.NotZero(t, attempt.StagingTableID)

	readTxn, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.NoError(t, err)
	require.NoError(t, disttaeEngine.Engine.New(ctx, readTxn))
	_, _, stagingRelation, err := disttaeEngine.Engine.GetRelationById(
		ctx,
		readTxn,
		attempt.StagingTableID,
	)
	require.NoError(t, err)
	tableDef := stagingRelation.GetTableDef(ctx)
	require.NotNil(t, tableDef.Pkey)
	require.Equal(t, catalog.FakePrimaryKeyColName, tableDef.Pkey.PkeyColName)
	require.Len(t, tableDef.Cols, len(schema.Columns)+2)
	fakePKIndex, rowIDIndex := -1, -1
	for index, column := range tableDef.Cols {
		switch column.Name {
		case catalog.FakePrimaryKeyColName:
			fakePKIndex = index
		case catalog.Row_ID:
			rowIDIndex = index
		}
	}
	require.NotEqual(t, -1, fakePKIndex)
	require.NotEqual(t, -1, rowIDIndex)
	require.True(t, tableDef.Cols[fakePKIndex].Hidden)
	require.True(t, tableDef.Cols[fakePKIndex].Typ.AutoIncr)
	require.Equal(t, int32(types.T_uint64), tableDef.Cols[fakePKIndex].Typ.Id)
	require.True(t, tableDef.Cols[rowIDIndex].Hidden)
	require.Equal(t, int32(types.T_Rowid), tableDef.Cols[rowIDIndex].Typ.Id)
	require.NoError(t, readTxn.Commit(ctx))

	failedRows, failedReceipt := lifecycleRestoreTestChunk(
		t,
		ctx,
		schema,
		datasetID,
		restoreID,
		0,
		[]int64{11, 22},
	)
	failedRepository := repository
	failedRepository.Executor = &failLifecycleRestoreChunkReceiptExecutor{
		delegate: sqlExecutor,
	}
	_, err = failedRepository.ImportChunk(
		ctx,
		attempt,
		failedReceipt,
		schema,
		failedRows,
	)
	require.ErrorContains(t, err, "injected Lifecycle Restore Chunk Receipt failure")
	require.Empty(t, queryLifecycleRestoreRows(
		t,
		ctx,
		sqlExecutor,
		accountID,
		databaseName,
		hiddenName,
	).payloads)
	require.Equal(t, uint64(0), queryLifecycleRestoreUint64(
		t,
		ctx,
		sqlExecutor,
		accountID,
		fmt.Sprintf(
			`select cast(count(*) as bigint unsigned) from mo_catalog.mo_lifecycle_restore_chunks
where restore_id=unhex('%s')`,
			lifecycleRestoreTestUUIDHex(restoreID),
		),
	))
	rolledBackAttempt, getErr := repository.GetAttempt(ctx, restoreID)
	require.NoError(t, getErr)
	require.Equal(t, uint64(0), rolledBackAttempt.NextChunkOrdinal)
	require.Equal(t, uint64(0), rolledBackAttempt.RestoredRows)

	importConcurrently := func(
		current lifecyclepkg.RestoreAttempt,
		rowSets [][][]lifecyclepkg.CanonicalCell,
		receiptSets []lifecyclepkg.RestoreChunkReceipt,
	) []error {
		require.Len(t, rowSets, len(receiptSets))
		start := make(chan struct{})
		errors := make([]error, len(receiptSets))
		var workers sync.WaitGroup
		workers.Add(len(receiptSets))
		for index := range receiptSets {
			go func(index int) {
				defer workers.Done()
				<-start
				_, errors[index] = repository.ImportChunk(
					ctx,
					current,
					receiptSets[index],
					schema,
					rowSets[index],
				)
			}(index)
		}
		close(start)
		workers.Wait()
		return errors
	}

	rows0, receipt0 := lifecycleRestoreTestChunk(
		t,
		ctx,
		schema,
		datasetID,
		restoreID,
		0,
		[]int64{11, 22},
	)
	rows0Duplicate, receipt0Duplicate := lifecycleRestoreTestChunk(
		t,
		ctx,
		schema,
		datasetID,
		restoreID,
		0,
		[]int64{11, 22},
	)
	sameDigestErrors := importConcurrently(
		attempt,
		[][][]lifecyclepkg.CanonicalCell{rows0, rows0Duplicate},
		[]lifecyclepkg.RestoreChunkReceipt{receipt0, receipt0Duplicate},
	)
	require.True(t, sameDigestErrors[0] == nil || sameDigestErrors[1] == nil)
	attempt, err = repository.GetAttempt(ctx, restoreID)
	require.NoError(t, err)
	require.Equal(t, uint64(1), attempt.NextChunkOrdinal)
	require.Equal(t, uint64(2), attempt.RestoredRows)
	attempt, err = repository.ImportChunk(ctx, attempt, receipt0, schema, rows0)
	require.NoError(t, err, "same digest retry must converge idempotently")

	rows1, receipt1 := lifecycleRestoreTestChunk(
		t,
		ctx,
		schema,
		datasetID,
		restoreID,
		1,
		[]int64{33, 44},
	)
	rows1Competing, receipt1Competing := lifecycleRestoreTestChunk(
		t,
		ctx,
		schema,
		datasetID,
		restoreID,
		1,
		[]int64{33, 44},
	)
	receipt1Competing.ChunkDigest = sha256.Sum256([]byte("competing chunk digest"))
	differentDigestErrors := importConcurrently(
		attempt,
		[][][]lifecyclepkg.CanonicalCell{rows1, rows1Competing},
		[]lifecyclepkg.RestoreChunkReceipt{receipt1, receipt1Competing},
	)
	require.True(t, differentDigestErrors[0] == nil || differentDigestErrors[1] == nil)
	attempt, err = repository.GetAttempt(ctx, restoreID)
	require.NoError(t, err)
	require.Equal(t, uint64(2), attempt.NextChunkOrdinal)
	require.Equal(t, uint64(4), attempt.RestoredRows)
	winningReceipts, err := repository.ListChunkReceipts(ctx, restoreID)
	require.NoError(t, err)
	require.Len(t, winningReceipts, 2)
	var losingReceipt lifecyclepkg.RestoreChunkReceipt
	if winningReceipts[1].ChunkDigest == receipt1.ChunkDigest {
		losingReceipt = receipt1Competing
	} else {
		require.Equal(t, receipt1Competing.ChunkDigest, winningReceipts[1].ChunkDigest)
		losingReceipt = receipt1
	}
	_, err = repository.ImportChunk(ctx, attempt, losingReceipt, schema, rows1)
	require.ErrorContains(t, err, "Chunk digest corruption")

	require.Equal(t, uint64(2), attempt.NextChunkOrdinal)
	require.Equal(t, uint64(4), attempt.RestoredRows)
	receipts, err := repository.ListChunkReceipts(ctx, restoreID)
	require.NoError(t, err)
	require.Len(t, receipts, 2)

	hiddenRows := queryLifecycleRestoreRows(
		t,
		ctx,
		sqlExecutor,
		accountID,
		databaseName,
		hiddenName,
	)
	require.Equal(t, []int64{11, 22, 33, 44}, hiddenRows.payloads)
	require.Equal(t, []uint64{11, 22, 33, 44}, hiddenRows.autoIDs)
	require.Len(t, hiddenRows.fakePKs, 4)
	assertLifecycleRestoreFakePKs(t, hiddenRows.fakePKs)

	require.NoError(t, repository.Publish(
		ctx,
		attempt,
		verifiedHash,
		schema,
		[]lifecyclepkg.AutoIncrementMax{{
			ColumnOrdinal: 0,
			Value:         "44",
		}},
	))
	publishedRows := queryLifecycleRestoreRows(
		t,
		ctx,
		sqlExecutor,
		accountID,
		databaseName,
		targetName,
	)
	require.Equal(t, hiddenRows, publishedRows)
	assertLifecycleRestoreFakePKs(t, publishedRows.fakePKs)

	current, err := repository.GetAttempt(ctx, restoreID)
	require.NoError(t, err)
	require.Equal(t, "DONE", current.State)
	require.Equal(t, verifiedHash, current.VerifiedHash)
	previousFakePKs := append([]uint64(nil), publishedRows.fakePKs...)
	mustExecLifecycleRestoreSQL(
		t,
		ctx,
		sqlExecutor,
		accountID,
		fmt.Sprintf(
			"insert into `%s`.`%s`(payload) values(55)",
			databaseName,
			targetName,
		),
	)
	afterInsert := queryLifecycleRestoreRows(
		t,
		ctx,
		sqlExecutor,
		accountID,
		databaseName,
		targetName,
	)
	require.Equal(t, []int64{11, 22, 33, 44, 55}, afterInsert.payloads)
	require.Equal(t, []uint64{11, 22, 33, 44, 45}, afterInsert.autoIDs)
	assertLifecycleRestoreFakePKs(t, afterInsert.fakePKs)
	require.NotContains(t, previousFakePKs, afterInsert.fakePKs[len(afterInsert.fakePKs)-1])
	leaseCount := queryLifecycleRestoreUint64(
		t,
		ctx,
		sqlExecutor,
		accountID,
		fmt.Sprintf(
			`select cast(count(*) as bigint unsigned) from mo_catalog.mo_lifecycle_datasets
where dataset_id=unhex('%s') and restore_lease_id is null`,
			lifecycleRestoreTestUUIDHex(datasetID),
		),
	)
	require.Equal(t, uint64(1), leaseCount)

	// Exercise the real Publish/CleanupHidden race through ordinary MO
	// transactions.  Whichever transaction wins owns the only legal terminal
	// state; the loser must not rename or drop across that state transition.
	const (
		raceRestoreID = "77777777-7777-7777-7777-777777777777"
		raceLeaseID   = "88888888-8888-8888-8888-888888888888"
		raceHidden    = catalog.LifecycleRestoreTableNamePrefix + "99999999999999999999999999999999"
		raceTarget    = "restored_events_race"
	)
	datasetVersion := queryLifecycleRestoreUint64(
		t,
		ctx,
		sqlExecutor,
		accountID,
		fmt.Sprintf(
			`select version from mo_catalog.mo_lifecycle_datasets
where dataset_id=unhex('%s')`,
			lifecycleRestoreTestUUIDHex(datasetID),
		),
	)
	raceHiddenCreateSQL, err := schema.BuildRestoreCreateTableSQL(
		ctx,
		databaseName,
		raceHidden,
	)
	require.NoError(t, err)
	raceDataset := lifecyclepkg.RestoreDataset{
		DatasetID:    datasetID,
		AccountID:    accountID,
		ContentHash:  verifiedHash,
		RowCount:     4,
		LogicalBytes: datasetLogicalCap,
		Version:      datasetVersion,
		State:        "PUBLISHED",
	}
	raceInitialAttempt := lifecyclepkg.RestoreAttempt{
		RestoreID:            raceRestoreID,
		DatasetID:            datasetID,
		DatasetIDs:           []string{datasetID},
		LeaseID:              raceLeaseID,
		Deadline:             time.Now().UTC().Add(time.Minute).Truncate(time.Microsecond),
		StagingDatabaseID:    databaseID,
		HiddenName:           raceHidden,
		TargetDatabaseID:     databaseID,
		TargetDatabaseName:   databaseName,
		TargetName:           raceTarget,
		Scope:                lifecyclepkg.RestoreScopeDataset,
		SourceLogicalTableID: 900,
		TotalChunkCount:      2,
		SelectedLogicalBytes: datasetLogicalCap,
	}
	raceInitialAttempt.SelectionDigest = lifecyclepkg.BuildRestoreSelectionDigest(
		raceInitialAttempt.Scope,
		raceInitialAttempt.SourceLogicalTableID,
		0,
		0,
		raceInitialAttempt.DatasetIDs,
	)
	raceAttempt, err := repository.Initialize(
		ctx,
		lifecyclepkg.RestoreInitializeRequest{
			Dataset:         raceDataset,
			Datasets:        []lifecyclepkg.RestoreDataset{raceDataset},
			Attempt:         raceInitialAttempt,
			HiddenCreateSQL: raceHiddenCreateSQL,
		},
	)
	require.NoError(t, err)
	for ordinal, values := range [][]int64{{11, 22}, {33, 44}} {
		rows, receipt := lifecycleRestoreTestChunk(
			t,
			ctx,
			schema,
			datasetID,
			raceRestoreID,
			uint64(ordinal),
			values,
		)
		raceAttempt, err = repository.ImportChunk(
			ctx,
			raceAttempt,
			receipt,
			schema,
			rows,
		)
		require.NoError(t, err)
	}

	startRace := make(chan struct{})
	var raceWorkers sync.WaitGroup
	raceWorkers.Add(2)
	var publishErr error
	var cleanupErr error
	go func() {
		defer raceWorkers.Done()
		<-startRace
		publishErr = repository.Publish(
			ctx,
			raceAttempt,
			verifiedHash,
			schema,
			[]lifecyclepkg.AutoIncrementMax{{
				ColumnOrdinal: 0,
				Value:         "44",
			}},
		)
	}()
	go func() {
		defer raceWorkers.Done()
		<-startRace
		cleanupErr = repository.CleanupHidden(ctx, raceRestoreID)
	}()
	close(startRace)
	raceWorkers.Wait()

	raceCurrent, err := repository.GetAttempt(ctx, raceRestoreID)
	require.NoError(t, err)
	hiddenCount := queryLifecycleRestoreUint64(
		t,
		ctx,
		sqlExecutor,
		accountID,
		fmt.Sprintf(
			`select cast(count(*) as bigint unsigned) from mo_catalog.mo_tables
where reldatabase_id=%d and relname='%s'`,
			databaseID,
			raceHidden,
		),
	)
	require.Zero(t, hiddenCount)
	targetCount := queryLifecycleRestoreUint64(
		t,
		ctx,
		sqlExecutor,
		accountID,
		fmt.Sprintf(
			`select cast(count(*) as bigint unsigned) from mo_catalog.mo_tables
where reldatabase_id=%d and relname='%s'`,
			databaseID,
			raceTarget,
		),
	)
	switch raceCurrent.State {
	case "DONE":
		require.NoError(t, publishErr)
		require.Equal(t, uint64(1), targetCount)
		require.NoError(t, repository.CleanupHidden(ctx, raceRestoreID))
		require.Equal(t, []int64{11, 22, 33, 44}, queryLifecycleRestoreRows(
			t,
			ctx,
			sqlExecutor,
			accountID,
			databaseName,
			raceTarget,
		).payloads)
	case "FAILED":
		require.NoError(t, cleanupErr)
		require.Error(t, publishErr)
		require.Zero(t, targetCount)
	default:
		t.Fatalf(
			"Publish/CleanupHidden race did not converge: state=%s publish=%v cleanup=%v",
			raceCurrent.State,
			publishErr,
			cleanupErr,
		)
	}

	// Close the physical Purge loop with the real tenant Dataset row and real
	// system-owned SQL Root.  Dataset visibility changes first; Provider files
	// are removed asynchronously and PURGED is published only after quiescence.
	rootRepository := lifecyclepkg.SQLCleanupRootRepository{Executor: sqlExecutor}
	purgeNow := time.Now().UTC().Add(48 * time.Hour).Truncate(time.Microsecond)
	archivePrefix := "archive/" + rootID + "/" + archiveAttemptID
	cleanupRoot := lifecyclepkg.CleanupRoot{
		RootID:               rootID,
		AttemptID:            archiveAttemptID,
		Mode:                 lifecyclepkg.CleanupModeArchiveWhole,
		OwnerAccountID:       accountID,
		LogicalTableID:       900,
		PhysicalTableID:      901,
		ExecutorEpoch:        1,
		WorkerDeadline:       purgeNow.Add(time.Minute),
		ArchiveNamespace:     "restore-test-archive",
		CredentialHandle:     "restore-test-credential",
		ArchivePrefix:        archivePrefix,
		ManifestKey:          archivePrefix + "/manifest.json",
		ReservedCleanupBytes: datasetLogicalCap,
		State:                lifecyclepkg.CleanupRootPublished,
		StateVersion:         1,
		CleanupAfter:         purgeNow,
		TemporaryCleanupDone: true,
	}
	require.NoError(t, rootRepository.Register(ctx, cleanupRoot))
	repository.Roots = rootRepository
	purgeDatasetVersion := queryLifecycleRestoreUint64(
		t,
		ctx,
		sqlExecutor,
		accountID,
		fmt.Sprintf(
			`select version from mo_catalog.mo_lifecycle_datasets
where dataset_id=unhex('%s')`,
			lifecycleRestoreTestUUIDHex(datasetID),
		),
	)
	require.NoError(t, repository.RequestPurge(
		ctx,
		lifecyclepkg.RestoreDataset{
			DatasetID: datasetID,
			RootID:    rootID,
			State:     "PUBLISHED",
			Version:   purgeDatasetVersion,
		},
		purgeNow,
	))
	require.Equal(t, uint64(1), queryLifecycleRestoreUint64(
		t,
		ctx,
		sqlExecutor,
		accountID,
		fmt.Sprintf(
			`select cast(count(*) as bigint unsigned)
from mo_catalog.mo_lifecycle_datasets
where dataset_id=unhex('%s') and state='DELETE_PENDING'`,
			lifecycleRestoreTestUUIDHex(datasetID),
		),
	))
	rootAfterRequest, err := rootRepository.Get(ctx, rootID)
	require.NoError(t, err)
	require.Equal(t, lifecyclepkg.CleanupRootDeletePending, rootAfterRequest.State)
	sweepStart := rootAfterRequest.CleanupAfter.Add(time.Second)

	archive := newLifecycleRestoreCleanupStore()
	archive.put(archivePrefix+"/payload-0.parquet", []byte("payload"))
	archive.put(archivePrefix+"/manifest.json", []byte("manifest"))
	finalizeDataset := func(finalizeCtx context.Context, _ lifecyclepkg.CleanupRoot) error {
		result, finalizeErr := sqlExecutor.Exec(
			finalizeCtx,
			fmt.Sprintf(
				`update mo_catalog.mo_lifecycle_datasets
set state='PURGED',version=version+1,updated_at=utc_timestamp()
where dataset_id=unhex('%s') and state='DELETE_PENDING'`,
				lifecycleRestoreTestUUIDHex(datasetID),
			),
			executor.Options{}.
				WithAccountID(accountID).
				WithWaitCommittedLogApplied(),
		)
		if finalizeErr != nil {
			return finalizeErr
		}
		defer result.Close()
		if result.AffectedRows != 1 {
			return fmt.Errorf(
				"Lifecycle Dataset Purge finalizer affected %d rows",
				result.AffectedRows,
			)
		}
		return nil
	}
	sweeper := lifecyclepkg.CleanupSweeper{
		Roots:               rootRepository,
		Archive:             archive,
		QuiescenceWindow:    time.Second,
		FinalizePublication: finalizeDataset,
	}
	require.NoError(t, sweeper.SweepOne(ctx, rootID, sweepStart))
	require.Empty(t, archive.keys())
	archive.put(archivePrefix+"/late-put.parquet", []byte("late"))
	require.NoError(t, sweeper.SweepOne(ctx, rootID, sweepStart.Add(time.Second)))
	require.Empty(t, archive.keys())
	require.NoError(t, sweeper.SweepOne(ctx, rootID, sweepStart.Add(2*time.Second)))
	require.NoError(t, sweeper.SweepOne(ctx, rootID, sweepStart.Add(4*time.Second)))
	require.Equal(t, uint64(1), queryLifecycleRestoreUint64(
		t,
		ctx,
		sqlExecutor,
		accountID,
		fmt.Sprintf(
			`select cast(count(*) as bigint unsigned)
from mo_catalog.mo_lifecycle_datasets
where dataset_id=unhex('%s') and state='PURGED'`,
			lifecycleRestoreTestUUIDHex(datasetID),
		),
	))
	cleanedRoot, err := rootRepository.Get(ctx, rootID)
	require.NoError(t, err)
	require.Equal(t, lifecyclepkg.CleanupRootCleaned, cleanedRoot.State)
}

func mustExecLifecycleRestoreSQL(
	t *testing.T,
	ctx context.Context,
	sqlExecutor executor.SQLExecutor,
	accountID uint32,
	sql string,
) {
	t.Helper()
	result, err := sqlExecutor.Exec(
		ctx,
		sql,
		executor.Options{}.
			WithAccountID(accountID).
			WithWaitCommittedLogApplied(),
	)
	require.NoError(t, err, sql)
	result.Close()
}

func queryLifecycleRestoreUint64(
	t *testing.T,
	ctx context.Context,
	sqlExecutor executor.SQLExecutor,
	accountID uint32,
	sql string,
) uint64 {
	t.Helper()
	result, err := sqlExecutor.Exec(
		ctx,
		sql,
		executor.Options{}.
			WithAccountID(accountID).
			WithWaitCommittedLogApplied(),
	)
	require.NoError(t, err, sql)
	defer result.Close()
	var value uint64
	rowsRead := 0
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		require.Len(t, columns, 1)
		for row := 0; row < rows; row++ {
			require.False(t, columns[0].GetNulls().Contains(uint64(row)))
			value = vector.GetFixedAtNoTypeCheck[uint64](columns[0], row)
			rowsRead++
		}
		return true
	})
	require.Equal(t, 1, rowsRead)
	return value
}

type lifecycleRestoreTestRows struct {
	payloads []int64
	autoIDs  []uint64
	fakePKs  []uint64
}

func queryLifecycleRestoreRows(
	t *testing.T,
	ctx context.Context,
	sqlExecutor executor.SQLExecutor,
	accountID uint32,
	databaseName string,
	tableName string,
) lifecycleRestoreTestRows {
	t.Helper()
	result, err := sqlExecutor.Exec(
		ctx,
		fmt.Sprintf(
			"select id,payload,%s from `%s`.`%s` order by payload",
			catalog.FakePrimaryKeyColName,
			databaseName,
			tableName,
		),
		executor.Options{}.
			WithAccountID(accountID).
			WithWaitCommittedLogApplied(),
	)
	require.NoError(t, err)
	defer result.Close()
	var values lifecycleRestoreTestRows
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		require.Len(t, columns, 3)
		for row := 0; row < rows; row++ {
			require.False(t, columns[0].GetNulls().Contains(uint64(row)))
			require.False(t, columns[1].GetNulls().Contains(uint64(row)))
			require.False(t, columns[2].GetNulls().Contains(uint64(row)))
			values.autoIDs = append(
				values.autoIDs,
				vector.GetFixedAtNoTypeCheck[uint64](columns[0], row),
			)
			values.payloads = append(
				values.payloads,
				vector.GetFixedAtNoTypeCheck[int64](columns[1], row),
			)
			values.fakePKs = append(
				values.fakePKs,
				vector.GetFixedAtNoTypeCheck[uint64](columns[2], row),
			)
		}
		return true
	})
	return values
}

func assertLifecycleRestoreFakePKs(t *testing.T, values []uint64) {
	t.Helper()
	seen := make(map[uint64]struct{}, len(values))
	for _, value := range values {
		require.NotZero(t, value)
		_, duplicate := seen[value]
		require.False(t, duplicate, "duplicate fake primary key %d", value)
		seen[value] = struct{}{}
	}
}

func lifecycleRestoreTestChunk(
	t *testing.T,
	ctx context.Context,
	schema lifecyclepkg.SchemaDescriptor,
	datasetID string,
	restoreID string,
	ordinal uint64,
	values []int64,
) ([][]lifecyclepkg.CanonicalCell, lifecyclepkg.RestoreChunkReceipt) {
	t.Helper()
	schemaDigest, err := schema.Digest()
	require.NoError(t, err)
	encoder := lifecyclepkg.NewCanonicalValueEncoder(schemaDigest)
	rows := make([][]lifecyclepkg.CanonicalCell, len(values))
	for index, value := range values {
		rows[index] = []lifecyclepkg.CanonicalCell{
			{
				Type:  types.T_uint64.ToType(),
				Value: uint64(value),
			},
			{
				Type:  types.T_int64.ToType(),
				Value: value,
			},
		}
		require.NoError(t, encoder.WriteRow(ctx, rows[index]))
	}
	contentHash := encoder.Sum()
	digestInput := append([]byte(fmt.Sprintf("chunk-%d:", ordinal)), contentHash[:]...)
	return rows, lifecyclepkg.RestoreChunkReceipt{
		RestoreID:            restoreID,
		DatasetID:            datasetID,
		DatasetChunkOrdinal:  ordinal,
		ChunkOrdinal:         ordinal,
		FileOrdinal:          uint32(ordinal),
		RowGroupOrdinal:      0,
		ChunkDigest:          sha256.Sum256(digestInput),
		RowCount:             encoder.RowCount(),
		LogicalBytes:         encoder.LogicalBytes(),
		CanonicalContentHash: contentHash,
	}
}

func lifecycleRestoreTestUUIDHex(value string) string {
	return strings.ReplaceAll(value, "-", "")
}

type failLifecycleRestoreChunkReceiptExecutor struct {
	delegate executor.SQLExecutor
	failed   bool
}

type lifecycleRestoreCleanupStore struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newLifecycleRestoreCleanupStore() *lifecycleRestoreCleanupStore {
	return &lifecycleRestoreCleanupStore{objects: make(map[string][]byte)}
}

func (store *lifecycleRestoreCleanupStore) put(key string, value []byte) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.objects[key] = append([]byte(nil), value...)
}

func (store *lifecycleRestoreCleanupStore) List(
	_ context.Context,
	prefix string,
) ([]string, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	keys := make([]string, 0, len(store.objects))
	for key := range store.objects {
		if strings.HasPrefix(key, strings.TrimSuffix(prefix, "/")+"/") {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

func (store *lifecycleRestoreCleanupStore) Delete(
	_ context.Context,
	key string,
) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	delete(store.objects, key)
	return nil
}

func (store *lifecycleRestoreCleanupStore) keys() []string {
	store.mu.Lock()
	defer store.mu.Unlock()
	keys := make([]string, 0, len(store.objects))
	for key := range store.objects {
		keys = append(keys, key)
	}
	return keys
}

func (sqlExecutor *failLifecycleRestoreChunkReceiptExecutor) Exec(
	ctx context.Context,
	sql string,
	options executor.Options,
) (executor.Result, error) {
	return sqlExecutor.delegate.Exec(ctx, sql, options)
}

func (sqlExecutor *failLifecycleRestoreChunkReceiptExecutor) ExecTxn(
	ctx context.Context,
	execFunc func(executor.TxnExecutor) error,
	options executor.Options,
) error {
	return sqlExecutor.delegate.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			return execFunc(failLifecycleRestoreChunkReceiptTxn{
				delegate: txn,
				owner:    sqlExecutor,
			})
		},
		options,
	)
}

type failLifecycleRestoreChunkReceiptTxn struct {
	delegate executor.TxnExecutor
	owner    *failLifecycleRestoreChunkReceiptExecutor
}

func (txn failLifecycleRestoreChunkReceiptTxn) Use(database string) {
	txn.delegate.Use(database)
}

func (txn failLifecycleRestoreChunkReceiptTxn) LockTable(table string) error {
	return txn.delegate.LockTable(table)
}

func (txn failLifecycleRestoreChunkReceiptTxn) Exec(
	sql string,
	options executor.StatementOption,
) (executor.Result, error) {
	if !txn.owner.failed && strings.Contains(
		strings.ToLower(sql),
		"insert into mo_catalog.mo_lifecycle_restore_chunks",
	) {
		txn.owner.failed = true
		return executor.Result{}, fmt.Errorf(
			"injected Lifecycle Restore Chunk Receipt failure",
		)
	}
	return txn.delegate.Exec(sql, options)
}

func (txn failLifecycleRestoreChunkReceiptTxn) Txn() client.TxnOperator {
	return txn.delegate.Txn()
}
