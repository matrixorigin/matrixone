// Copyright 2021 Matrix Origin
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

package publication

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"go.uber.org/zap"
)

// submitObjectsAsInsert submits objects as INSERT operation
func submitObjectsAsInsert(ctx context.Context, taskID string, txn client.TxnOperator, cnEngine engine.Engine, tombstoneInsertStats []*ObjectWithTableInfo, dataInsertStats []*ObjectWithTableInfo, mp *mpool.MPool) error {
	if len(tombstoneInsertStats) == 0 && len(dataInsertStats) == 0 {
		return nil
	}

	if cnEngine == nil {
		return moerr.NewInternalError(ctx, "engine is nil")
	}

	// Group objects by (dbName, tableName)
	type tableKey struct {
		dbName    string
		tableName string
	}
	tombstoneByTable := make(map[tableKey][]objectio.ObjectStats)
	dataByTable := make(map[tableKey][]objectio.ObjectStats)

	for _, obj := range tombstoneInsertStats {
		key := tableKey{dbName: obj.DBName, tableName: obj.TableName}
		tombstoneByTable[key] = append(tombstoneByTable[key], obj.Stats)
	}
	for _, obj := range dataInsertStats {
		key := tableKey{dbName: obj.DBName, tableName: obj.TableName}
		dataByTable[key] = append(dataByTable[key], obj.Stats)
	}

	// Process each table separately
	for key, tombstoneStats := range tombstoneByTable {
		if len(tombstoneStats) == 0 {
			continue
		}

		// Get database using transaction from iteration context
		db, err := cnEngine.Database(ctx, key.dbName, txn)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to get database %s: %v", key.dbName, err)
		}

		// Get relation using transaction from iteration context
		rel, err := db.Relation(ctx, key.tableName, nil)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to get relation %s.%s: %v", key.dbName, key.tableName, err)
		}

		// Get table definition to check for fake pk
		tableDef := rel.GetTableDef(ctx)
		hasFakePK := false
		if tableDef != nil && tableDef.Pkey != nil {
			hasFakePK = catalog.IsFakePkName(tableDef.Pkey.PkeyColName)
		}

		// Update ObjectStats flags before submitting
		for i := range tombstoneStats {
			updateObjectStatsFlags(&tombstoneStats[i], true, hasFakePK) // isTombstone = true
		}

		// Collect object abbreviations for logging
		var createObjs []string
		for _, stats := range tombstoneStats {
			createObjs = append(createObjs, stats.ObjectName().ObjectId().ShortStringEx())
		}
		if len(createObjs) > 0 {
			logutil.Info("ccpr-iteration objectsubmit",
				zap.String("task_id", taskID),
				zap.String("database", key.dbName),
				zap.String("table", key.tableName),
				zap.String("operation", "create"),
				zap.Strings("objects", createObjs),
			)
		}

		// Create batch with ObjectStats for deletion
		deleteBat := batch.NewWithSize(1)
		deleteBat.SetAttributes([]string{catalog.ObjectMeta_ObjectStats})

		// ObjectStats column (T_binary)
		statsVec := vector.NewVec(types.T_binary.ToType())
		deleteBat.Vecs[0] = statsVec

		// Append ObjectStats to the batch using Marshal()
		for _, stats := range tombstoneStats {
			statsBytes := stats.Marshal()
			if err := vector.AppendBytes(statsVec, statsBytes, false, mp); err != nil {
				deleteBat.Clean(mp)
				return moerr.NewInternalErrorf(ctx, "failed to append tombstone object stats: %v", err)
			}
		}

		deleteBat.SetRowCount(len(tombstoneStats))

		// Delete through relation with skipTransfer flag to avoid transfer errors
		// CCPR tombstones should not be transferred since they are already properly positioned
		skipTransferCtx := context.WithValue(ctx, defines.SkipTransferKey{}, true)
		if err := rel.Delete(skipTransferCtx, deleteBat, ""); err != nil {
			deleteBat.Clean(mp)
			return moerr.NewInternalErrorf(ctx, "failed to delete tombstone objects: %v", err)
		}
		deleteBat.Clean(mp)
	}

	// Handle regular data objects: use the original Write logic
	for key, dataStats := range dataByTable {
		if len(dataStats) == 0 {
			continue
		}

		// Get database using transaction from iteration context
		db, err := cnEngine.Database(ctx, key.dbName, txn)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to get database %s: %v", key.dbName, err)
		}

		// Get relation using transaction from iteration context
		rel, err := db.Relation(ctx, key.tableName, nil)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to get relation %s.%s: %v", key.dbName, key.tableName, err)
		}

		// Get table definition to check for fake pk
		tableDef := rel.GetTableDef(ctx)
		hasFakePK := false
		if tableDef != nil && tableDef.Pkey != nil {
			hasFakePK = catalog.IsFakePkName(tableDef.Pkey.PkeyColName)
		}

		// Update ObjectStats flags before submitting
		for i := range dataStats {
			updateObjectStatsFlags(&dataStats[i], false, hasFakePK) // isTombstone = false
		}

		// Collect object abbreviations for logging
		var createObjs []string
		for _, stats := range dataStats {
			createObjs = append(createObjs, stats.ObjectName().ObjectId().ShortStringEx())
		}
		if len(createObjs) > 0 {
			logutil.Info("ccpr-iteration objectsubmit",
				zap.String("task_id", taskID),
				zap.String("database", key.dbName),
				zap.String("table", key.tableName),
				zap.String("operation", "create"),
				zap.Strings("objects", createObjs),
			)
		}

		// Create batch with ObjectStats using the same structure as s3util
		bat := batch.NewWithSize(2)
		bat.SetAttributes([]string{catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats})

		// First column: BlockInfo (T_text)
		blockInfoVec := vector.NewVec(types.T_text.ToType())
		bat.Vecs[0] = blockInfoVec

		// Second column: ObjectStats (T_binary)
		statsVec := vector.NewVec(types.T_binary.ToType())
		bat.Vecs[1] = statsVec

		// Use ExpandObjectStatsToBatch to properly expand ObjectStats to batch
		// This handles the correct mapping between blocks and their parent objects
		if err := colexec.ExpandObjectStatsToBatch(
			mp,
			false, // isTombstone = false for INSERT
			bat,
			true, // isCNCreated = true
			dataStats...,
		); err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to expand object stats to batch: %v", err)
		}

		// Write through relation
		if err := rel.Write(ctx, bat); err != nil {
			bat.Clean(mp)
			return moerr.NewInternalErrorf(ctx, "failed to write objects: %v", err)
		}
		bat.Clean(mp)
	}

	return nil
}

// submitObjectsAsDelete submits objects as DELETE operation
// It uses SoftDeleteObject to soft delete objects by setting their deleteat timestamp
func submitObjectsAsDelete(
	ctx context.Context,
	taskID string,
	txn client.TxnOperator,
	cnEngine engine.Engine,
	statsList []*ObjectWithTableInfo,
	mp *mpool.MPool,
) error {
	if len(statsList) == 0 {
		return nil
	}

	if cnEngine == nil {
		return moerr.NewInternalError(ctx, "engine is nil")
	}

	// Group objects by (dbName, tableName)
	type tableKey struct {
		dbName    string
		tableName string
	}
	statsByTable := make(map[tableKey][]*ObjectWithTableInfo)

	for _, obj := range statsList {
		key := tableKey{dbName: obj.DBName, tableName: obj.TableName}
		statsByTable[key] = append(statsByTable[key], obj)
	}

	// Process each table separately
	for key, tableStats := range statsByTable {
		if len(tableStats) == 0 {
			continue
		}

		// Get database using transaction from iteration context
		db, err := cnEngine.Database(ctx, key.dbName, txn)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to get database %s: %v", key.dbName, err)
		}

		// Get relation using transaction from iteration context
		rel, err := db.Relation(ctx, key.tableName, nil)
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to get relation %s.%s: %v", key.dbName, key.tableName, err)
		}

		// Get table definition to check for fake pk
		tableDef := rel.GetTableDef(ctx)
		hasFakePK := false
		if tableDef != nil && tableDef.Pkey != nil {
			hasFakePK = catalog.IsFakePkName(tableDef.Pkey.PkeyColName)
		}

		// Update ObjectStats flags before submitting
		for i := range tableStats {
			updateObjectStatsFlags(&tableStats[i].Stats, tableStats[i].IsTombstone, hasFakePK)
		}

		// Try to use SoftDeleteObject if available (for disttae txnTable or txnTableDelegate)
		// Otherwise fall back to the old Delete method
		// Check if it's a txnTableDelegate first
		if delegate, ok := rel.(interface {
			SoftDeleteObject(ctx context.Context, objID *objectio.ObjectId, isTombstone bool) error
		}); ok {
			// Use SoftDeleteObject for each object
			// The deleteat will be set to the transaction's commit timestamp
			var deleteObjs []string
			for _, obj := range tableStats {
				objID := obj.Stats.ObjectName().ObjectId()
				deleteObjs = append(deleteObjs, objID.ShortStringEx())

				// objID is already *objectio.ObjectId, so we pass it directly
				if err := delegate.SoftDeleteObject(ctx, objID, obj.IsTombstone); err != nil {
					return moerr.NewInternalErrorf(ctx, "failed to soft delete object %s: %v", objID.ShortStringEx(), err)
				}
			}
			if len(deleteObjs) > 0 {
				logutil.Info("ccpr-iteration objectsubmit",
					zap.String("task_id", taskID),
					zap.String("database", key.dbName),
					zap.String("table", key.tableName),
					zap.String("operation", "delete"),
					zap.Strings("objects", deleteObjs),
				)
			}
		} else {
			return moerr.NewInternalErrorf(ctx, "failed to use SoftDeleteObject for relation %s.%s", key.dbName, key.tableName)
		}
	}
	return nil
}

// GetObjectListMap retrieves the object list from snapshot diff and returns a map
func GetObjectListMap(ctx context.Context, iterationCtx *IterationContext, cnEngine engine.Engine) (map[objectio.ObjectId]*ObjectWithTableInfo, error) {

	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	objectListResult, cancel, err := GetObjectListFromSnapshotDiff(ctxWithTimeout, iterationCtx)
	if err != nil {
		err = moerr.NewInternalErrorf(ctx, "failed to get object list from snapshot diff: %v", err)
		return nil, err
	}
	defer cancel()
	defer func() {
		if objectListResult != nil {
			objectListResult.Close()
		}
	}()
	// Map to deduplicate objects by ObjectId
	// Key: ObjectId, Value: object info
	objectMap := make(map[objectio.ObjectId]*ObjectWithTableInfo)

	if objectListResult != nil {
		// Check for errors during iteration
		if err = objectListResult.Err(); err != nil {
			err = moerr.NewInternalErrorf(ctx, "error reading object list result: %v", err)
			return nil, err
		}

		objectCount := 0
		// Iterate through object list
		for objectListResult.Next() {
			objectCount++
			// Read columns: db name, table name, object stats, create at, delete at, is tombstone
			var dbName, tableName string
			var statsBytes []byte
			var createAt, deleteAt types.TS
			var isTombstone bool

			if err = objectListResult.Scan(&dbName, &tableName, &statsBytes, &createAt, &deleteAt, &isTombstone); err != nil {
				err = moerr.NewInternalErrorf(ctx, "failed to scan object list result: %v", err)
				return nil, err
			}

			// Parse ObjectStats from bytes
			var stats objectio.ObjectStats
			stats.UnMarshal(statsBytes)

			// Get ObjectId from stats
			objID := *stats.ObjectName().ObjectId()
			delete := !deleteAt.IsEmpty()

			// Check if this object already exists in map
			if _, exists := objectMap[objID]; exists {
				// If there are two records, one without delete and one with delete, use delete to override
				if delete {
					// New record is delete, override existing record
					objectMap[objID] = &ObjectWithTableInfo{
						Stats:       stats,
						IsTombstone: isTombstone,
						Delete:      true,
						DBName:      dbName,
						TableName:   tableName,
					}
				}
			} else {
				// New object, add to map
				objectMap[objID] = &ObjectWithTableInfo{
					Stats:       stats,
					IsTombstone: isTombstone,
					Delete:      delete,
					DBName:      dbName,
					TableName:   tableName,
				}
			}

		}

	}

	return objectMap, nil
}

// ApplyObjects applies object changes to the downstream cluster
func ApplyObjects(
	ctx context.Context,
	taskID string,
	accountID uint32,
	indexTableMappings map[string]string,
	objectMap map[objectio.ObjectId]*ObjectWithTableInfo,
	upstreamExecutor SQLExecutor,
	localExecutor SQLExecutor,
	currentTS types.TS,
	txn client.TxnOperator,
	cnEngine engine.Engine,
	mp *mpool.MPool,
	fs fileservice.FileService,
	filterObjectWorker FilterObjectWorker,
	getChunkWorker GetChunkWorker,
	writeObjectWorker WriteObjectWorker,
	subscriptionAccountName string,
	pubName string,
	ccprCache CCPRTxnCacheWriter,
	aobjectMap AObjectMap,
	ttlChecker TTLChecker,
) (err error) {
	// Check TTL before starting
	if ttlChecker != nil && ttlChecker() {
		return ErrSyncProtectionTTLExpired
	}

	var collectedTombstoneDeleteStats []*ObjectWithTableInfo
	var collectedTombstoneInsertStats []*ObjectWithTableInfo
	var collectedDataDeleteStats []*ObjectWithTableInfo
	var collectedDataInsertStats []*ObjectWithTableInfo

	// Get txnID from txn operator for CCPR cache
	var txnID []byte
	if txn != nil {
		txnID = txn.Txn().ID
	}

	// Separate data objects and tombstone objects
	var dataObjects []*ObjectWithTableInfo
	var tombstoneObjects []*ObjectWithTableInfo
	for _, info := range objectMap {
		// Apply index table name mapping
		if indexTableMappings != nil {
			if downstreamName, exists := indexTableMappings[info.TableName]; exists {
				info.TableName = downstreamName
			}
		}
		if info.IsTombstone {
			tombstoneObjects = append(tombstoneObjects, info)
		} else {
			dataObjects = append(dataObjects, info)
		}
	}

	// Phase 1: Submit and process all DATA objects first (without aobjectMap for tombstone rewriting)
	// This ensures aobjectMap is populated with data object mappings before tombstone processing
	for _, info := range dataObjects {
		if !info.Delete {
			statsBytes := info.Stats.Marshal()
			// Data objects don't need aobjectMap for rewriting, pass nil
			filterJob := NewFilterObjectJob(ctx, statsBytes, currentTS, upstreamExecutor, false, fs, mp, getChunkWorker, writeObjectWorker, subscriptionAccountName, pubName, ccprCache, txnID, nil, ttlChecker)
			if filterObjectWorker != nil {
				filterObjectWorker.SubmitFilterObject(filterJob)
			} else {
				filterJob.Execute()
			}
			info.FilterJob = filterJob
		}
	}

	// Phase 2: Wait for all DATA filter jobs to complete and update aobjectMap
	for _, info := range dataObjects {
		if info.Stats.GetAppendable() {
			upstreamObjID := info.Stats.ObjectName().ObjectId()
			upstreamIDStr := upstreamObjID.String()

			if info.Delete {
				// Query existing mapping from aobjectMap
				if aobjectMap != nil {
					if existingMapping, exists := aobjectMap.Get(upstreamIDStr); exists {
						// Add existing downstream object to delete stats
						collectedDataDeleteStats = append(collectedDataDeleteStats, &ObjectWithTableInfo{
							Stats:       existingMapping.DownstreamStats,
							DBName:      existingMapping.DBName,
							TableName:   existingMapping.TableName,
							IsTombstone: false,
							Delete:      true,
						})
						// Delete the mapping from aobjectMap
						aobjectMap.Delete(upstreamIDStr)
					}
				}
			} else {
				filterResult := info.FilterJob.WaitDone().(*FilterObjectJobResult)
				if filterResult.Err != nil {
					err = moerr.NewInternalErrorf(ctx, "failed to filter data object: %v", filterResult.Err)
					return
				}
				// Query existing mapping from aobjectMap and delete old downstream object
				if aobjectMap != nil {
					if existingMapping, exists := aobjectMap.Get(upstreamIDStr); exists {
						collectedDataDeleteStats = append(collectedDataDeleteStats, &ObjectWithTableInfo{
							Stats:       existingMapping.DownstreamStats,
							DBName:      existingMapping.DBName,
							TableName:   existingMapping.TableName,
							IsTombstone: false,
							Delete:      true,
						})
					}
				}
				// Insert/update new mapping to aobjectMap
				if filterResult.HasMappingUpdate && filterResult.CurrentStats.ObjectName() != nil && aobjectMap != nil {
					aobjectMap.Set(upstreamIDStr, &AObjectMapping{
						DownstreamStats: filterResult.CurrentStats,
						IsTombstone:     false,
						DBName:          info.DBName,
						TableName:       info.TableName,
						RowOffsetMap:    filterResult.RowOffsetMap,
					})
					// Add new downstream object to insert stats
					collectedDataInsertStats = append(collectedDataInsertStats, &ObjectWithTableInfo{
						Stats:       filterResult.CurrentStats,
						DBName:      info.DBName,
						TableName:   info.TableName,
						IsTombstone: false,
						Delete:      false,
					})
				}
			}
		} else {
			// Handle non-appendable data objects
			if info.Delete {
				collectedDataDeleteStats = append(collectedDataDeleteStats, &ObjectWithTableInfo{
					Stats:       info.Stats,
					DBName:      info.DBName,
					TableName:   info.TableName,
					IsTombstone: false,
					Delete:      true,
				})
			} else {
				filterResult := info.FilterJob.WaitDone().(*FilterObjectJobResult)
				if filterResult.Err != nil {
					err = moerr.NewInternalErrorf(ctx, "failed to filter data object: %v", filterResult.Err)
					return
				}
				if !filterResult.DownstreamStats.IsZero() {
					collectedDataInsertStats = append(collectedDataInsertStats, &ObjectWithTableInfo{
						Stats:       filterResult.DownstreamStats,
						DBName:      info.DBName,
						TableName:   info.TableName,
						IsTombstone: false,
						Delete:      false,
					})
				}
			}
		}
	}

	// Phase 3: Now submit all TOMBSTONE objects (with aobjectMap for rowid rewriting)
	// At this point, aobjectMap contains all data object mappings
	for _, info := range tombstoneObjects {
		if !info.Delete {
			statsBytes := info.Stats.Marshal()
			// Tombstone objects need aobjectMap for rowid rewriting
			filterJob := NewFilterObjectJob(ctx, statsBytes, currentTS, upstreamExecutor, true, fs, mp, getChunkWorker, writeObjectWorker, subscriptionAccountName, pubName, ccprCache, txnID, aobjectMap, ttlChecker)
			if filterObjectWorker != nil {
				filterObjectWorker.SubmitFilterObject(filterJob)
			} else {
				filterJob.Execute()
			}
			info.FilterJob = filterJob
		}
	}

	// Phase 4: Wait for all TOMBSTONE filter jobs to complete
	for _, info := range tombstoneObjects {
		if info.Stats.GetAppendable() {
			upstreamObjID := info.Stats.ObjectName().ObjectId()
			upstreamIDStr := upstreamObjID.String()

			if info.Delete {
				// Query existing mapping from aobjectMap
				if aobjectMap != nil {
					if existingMapping, exists := aobjectMap.Get(upstreamIDStr); exists {
						collectedTombstoneDeleteStats = append(collectedTombstoneDeleteStats, &ObjectWithTableInfo{
							Stats:       existingMapping.DownstreamStats,
							DBName:      existingMapping.DBName,
							TableName:   existingMapping.TableName,
							IsTombstone: true,
							Delete:      true,
						})
						// Delete the mapping from aobjectMap
						aobjectMap.Delete(upstreamIDStr)
					}
				}
			} else {
				filterResult := info.FilterJob.WaitDone().(*FilterObjectJobResult)
				if filterResult.Err != nil {
					err = moerr.NewInternalErrorf(ctx, "failed to filter tombstone object: %v", filterResult.Err)
					return
				}
				// Query existing mapping from aobjectMap and delete old downstream object
				if aobjectMap != nil {
					if existingMapping, exists := aobjectMap.Get(upstreamIDStr); exists {
						collectedTombstoneDeleteStats = append(collectedTombstoneDeleteStats, &ObjectWithTableInfo{
							Stats:       existingMapping.DownstreamStats,
							DBName:      existingMapping.DBName,
							TableName:   existingMapping.TableName,
							IsTombstone: true,
							Delete:      true,
						})
					}
				}
				// Insert/update new mapping to aobjectMap
				if filterResult.HasMappingUpdate && filterResult.CurrentStats.ObjectName() != nil && aobjectMap != nil {
					aobjectMap.Set(upstreamIDStr, &AObjectMapping{
						DownstreamStats: filterResult.CurrentStats,
						IsTombstone:     true,
						DBName:          info.DBName,
						TableName:       info.TableName,
					})
					collectedTombstoneInsertStats = append(collectedTombstoneInsertStats, &ObjectWithTableInfo{
						Stats:       filterResult.CurrentStats,
						DBName:      info.DBName,
						TableName:   info.TableName,
						IsTombstone: true,
						Delete:      false,
					})
				}
			}
		} else {
			// Handle non-appendable tombstone objects
			if info.Delete {
				collectedTombstoneDeleteStats = append(collectedTombstoneDeleteStats, &ObjectWithTableInfo{
					Stats:       info.Stats,
					DBName:      info.DBName,
					TableName:   info.TableName,
					IsTombstone: true,
					Delete:      true,
				})
			} else {
				filterResult := info.FilterJob.WaitDone().(*FilterObjectJobResult)
				if filterResult.Err != nil {
					err = moerr.NewInternalErrorf(ctx, "failed to filter tombstone object: %v", filterResult.Err)
					return
				}
				if !filterResult.DownstreamStats.IsZero() {
					collectedTombstoneInsertStats = append(collectedTombstoneInsertStats, &ObjectWithTableInfo{
						Stats:       filterResult.DownstreamStats,
						DBName:      info.DBName,
						TableName:   info.TableName,
						IsTombstone: true,
						Delete:      false,
					})
				}
			}
		}
	}

	// Submit all collected objects to TN in order: tombstone delete -> tombstone insert -> data delete -> data insert
	// Use downstream account ID from iterationCtx.SrcInfo
	// Set PkCheckByTN to SkipAllDedup to completely skip all deduplication checks in TN
	downstreamCtx := context.WithValue(ctx, defines.TenantIDKey{}, accountID)
	downstreamCtx = context.WithValue(downstreamCtx, defines.PkCheckByTN{}, int8(cmd_util.SkipAllDedup))

	// 1. Submit tombstone delete objects (soft delete)
	if len(collectedTombstoneDeleteStats) > 0 {
		if err = submitObjectsAsDelete(downstreamCtx, taskID, txn, cnEngine, collectedTombstoneDeleteStats, mp); err != nil {
			err = moerr.NewInternalErrorf(ctx, "failed to submit tombstone delete objects: %v", err)
			return
		}
	}

	// 2. Submit tombstone insert objects
	if len(collectedTombstoneInsertStats) > 0 {
		if err = submitObjectsAsInsert(downstreamCtx, taskID, txn, cnEngine, collectedTombstoneInsertStats, nil, mp); err != nil {
			err = moerr.NewInternalErrorf(ctx, "failed to submit tombstone insert objects: %v", err)
			return
		}
	}

	// 3. Submit data delete objects (soft delete)
	if len(collectedDataDeleteStats) > 0 {
		if err = submitObjectsAsDelete(downstreamCtx, taskID, txn, cnEngine, collectedDataDeleteStats, mp); err != nil {
			err = moerr.NewInternalErrorf(ctx, "failed to submit data delete objects: %v", err)
			return
		}
	}

	// 4. Submit data insert objects
	if len(collectedDataInsertStats) > 0 {
		if err = submitObjectsAsInsert(downstreamCtx, taskID, txn, cnEngine, nil, collectedDataInsertStats, mp); err != nil {
			err = moerr.NewInternalErrorf(ctx, "failed to submit data insert objects: %v", err)
			return
		}
	}
	return
}
