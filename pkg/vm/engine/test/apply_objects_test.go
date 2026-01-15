// Copyright 2024 Matrix Origin
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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/backup"
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/publication"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	taetestutil "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ObjectMapJSON represents the serializable format of objectmap
type ObjectMapJSON struct {
	ObjectID    string `json:"object_id"`
	Stats       string `json:"stats"` // ObjectStats as base64-encoded string
	DBName      string `json:"db_name"`
	TableName   string `json:"table_name"`
	IsTombstone bool   `json:"is_tombstone"`
	Delete      bool   `json:"delete"`
}

// CollectAndExportObjects collects objectmap from catalog and exports to directory
// It collects objects from the table using catalog, builds objectmap,
// serializes it to base64 JSON file, and copies object files from fileservice to directory
func CollectAndExportObjects(
	ctx context.Context,
	fs fileservice.FileService,
	dir string,
	tableEntry *catalog.TableEntry,
	dbname string,
	tablename string,
	fromts, tots types.TS,
) error {
	// Clean and create directory
	if err := os.RemoveAll(dir); err != nil {
		return moerr.NewInternalError(ctx, fmt.Sprintf("failed to remove directory %s: %v", dir, err))
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return moerr.NewInternalError(ctx, fmt.Sprintf("failed to create directory %s: %v", dir, err))
	}

	// Build objectmap from catalog
	objectMap := make(map[objectio.ObjectId]*publication.ObjectWithTableInfo)

	// Collect data objects
	dataIt := tableEntry.MakeDataObjectIt()
	defer dataIt.Release()
	for ok := dataIt.Last(); ok; ok = dataIt.Prev() {
		objEntry := dataIt.Item()
		// Skip C entries having drop intent
		if objEntry.IsCEntry() && objEntry.HasDCounterpart() {
			continue
		}
		// Check timestamp range
		if !fromts.IsEmpty() && objEntry.CreatedAt.LT(&fromts) {
			continue
		}
		if !tots.IsEmpty() && objEntry.CreatedAt.GT(&tots) {
			continue
		}

		objID := *objEntry.ID()
		stats := objEntry.ObjectStats
		delete := !objEntry.DeletedAt.IsEmpty()

		// Check if this object already exists in map
		if existing, exists := objectMap[objID]; exists {
			// If there are two records, one without delete and one with delete, use delete to override
			if delete {
				// New record is delete, override existing record
				objectMap[objID] = &publication.ObjectWithTableInfo{
					Stats:       stats,
					IsTombstone: false,
					Delete:      true,
					DBName:      dbname,
					TableName:   tablename,
				}
			} else if existing.Delete {
				// Existing record is delete, keep delete (don't override)
				// Keep existing record
			} else {
				// Both are non-delete, update with new record
				objectMap[objID] = &publication.ObjectWithTableInfo{
					Stats:       stats,
					IsTombstone: false,
					Delete:      false,
					DBName:      dbname,
					TableName:   tablename,
				}
			}
		} else {
			// New object, add to map
			objectMap[objID] = &publication.ObjectWithTableInfo{
				Stats:       stats,
				IsTombstone: false,
				Delete:      delete,
				DBName:      dbname,
				TableName:   tablename,
			}
		}
	}

	// Collect tombstone objects
	tombstoneIt := tableEntry.MakeTombstoneObjectIt()
	defer tombstoneIt.Release()
	for ok := tombstoneIt.Last(); ok; ok = tombstoneIt.Prev() {
		objEntry := tombstoneIt.Item()
		// Skip C entries having drop intent
		if objEntry.IsCEntry() && objEntry.HasDCounterpart() {
			continue
		}
		// Check timestamp range
		if !fromts.IsEmpty() && objEntry.CreatedAt.LT(&fromts) {
			continue
		}
		if !tots.IsEmpty() && objEntry.CreatedAt.GT(&tots) {
			continue
		}

		objID := *objEntry.ID()
		stats := objEntry.ObjectStats
		delete := !objEntry.DeletedAt.IsEmpty()

		// Check if this object already exists in map
		if existing, exists := objectMap[objID]; exists {
			// If there are two records, one without delete and one with delete, use delete to override
			if delete {
				// New record is delete, override existing record
				objectMap[objID] = &publication.ObjectWithTableInfo{
					Stats:       stats,
					IsTombstone: true,
					Delete:      true,
					DBName:      dbname,
					TableName:   tablename,
				}
			} else if existing.Delete {
				// Existing record is delete, keep delete (don't override)
				// Keep existing record
			} else {
				// Both are non-delete, update with new record
				objectMap[objID] = &publication.ObjectWithTableInfo{
					Stats:       stats,
					IsTombstone: true,
					Delete:      false,
					DBName:      dbname,
					TableName:   tablename,
				}
			}
		} else {
			// New object, add to map
			objectMap[objID] = &publication.ObjectWithTableInfo{
				Stats:       stats,
				IsTombstone: true,
				Delete:      delete,
				DBName:      dbname,
				TableName:   tablename,
			}
		}
	}

	// Serialize objectmap to JSON with base64 encoding
	objectMapJSON := make(map[string]ObjectMapJSON)
	for objID, info := range objectMap {
		statsBytes := info.Stats.Marshal()
		objectMapJSON[objID.String()] = ObjectMapJSON{
			ObjectID:    objID.String(),
			Stats:       base64.StdEncoding.EncodeToString(statsBytes),
			DBName:      info.DBName,
			TableName:   info.TableName,
			IsTombstone: info.IsTombstone,
			Delete:      info.Delete,
		}
	}

	// Write objectmap to JSON file directly using os.WriteFile
	jsonData, err := json.MarshalIndent(objectMapJSON, "", "  ")
	if err != nil {
		return moerr.NewInternalError(ctx, fmt.Sprintf("failed to marshal objectmap: %v", err))
	}
	objectMapFile := filepath.Join(dir, "objectmap.json")
	if err := os.WriteFile(objectMapFile, jsonData, 0644); err != nil {
		return moerr.NewInternalError(ctx, fmt.Sprintf("failed to write objectmap file: %v", err))
	}

	// Create local fileservice for destination directory (for copying object files)
	dstFS, err := fileservice.NewLocalFS(ctx, "local", dir, fileservice.DisabledCacheConfig, nil)
	if err != nil {
		return moerr.NewInternalError(ctx, fmt.Sprintf("failed to create local fileservice: %v", err))
	}

	// Copy object files from fs to dir
	for _, info := range objectMap {
		// Skip deleted objects
		if info.Delete {
			continue
		}

		// Get object name from stats
		objectName := info.Stats.ObjectName().String()

		// Copy file from fs to dstFS
		_, err := backup.CopyFile(ctx, fs, dstFS, objectName, "", objectName)
		if err != nil {
			return err
		}
	}

	return nil
}

func PrepareDataAppend(
	t *testing.T,
	dir string,
	collectFn func(
		ctx context.Context,
		fs fileservice.FileService,
		dir string,
		tableEntry *catalog.TableEntry,
		dbname string,
		tablename string,
		fromts, tots types.TS,
	) error,
) {
	ctx := context.Background()
	tae := taetestutil.NewTestEngine(ctx, "test", t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(2, 1)
	schema.Name = "testTable"
	schema.Extra.BlockMaxRows = 50
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 50)
	taetestutil.CreateRelationAndAppend(t, 0, tae.DB, "db", schema, bat, true)
	tae.ForceCheckpoint()

	// Call CollectAndExportObjects with catalog
	dbname := "db"
	tablename := schema.Name
	fromts := types.TS{}
	tots := tae.TxnMgr.Now()
	txn, rel := tae.GetRelation()
	tblEntry := rel.GetMeta().(*catalog.TableEntry)
	assert.NoError(t, txn.Commit(ctx))

	err := collectFn(ctx, tae.Opts.Fs, dir, tblEntry, dbname, tablename, fromts, tots)
	assert.NoError(t, err)
}

func DDLAppend(t *testing.T, disttaeEngine *testutil.TestDisttaeEngine) {
	// Create database and table in disttaeEngine before ApplyObjects
	// Use the same schema as the exported objects
	destSchema := catalog.MockSchemaAll(2, 1)
	destSchema.Name = "testTable"
	destDBName := "db"
	destTableName := destSchema.Name
	destSchema.Extra.BlockMaxRows = 50

	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, uint32(0))
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	// Create a new txn for creating database and table
	createTxn, err := disttaeEngine.NewTxnOperator(ctxWithTimeout, disttaeEngine.Now())
	require.NoError(t, err)

	// Create database
	err = disttaeEngine.Engine.Create(ctxWithTimeout, destDBName, createTxn)
	require.NoError(t, err)

	// Get database
	destDB, err := disttaeEngine.Engine.Database(ctxWithTimeout, destDBName, createTxn)
	require.NoError(t, err)

	// Convert schema to table defs
	defs, err := testutil.EngineTableDefBySchema(destSchema)
	require.NoError(t, err)

	// Create table
	err = destDB.Create(ctxWithTimeout, destTableName, defs)
	require.NoError(t, err)

	// Commit the create txn
	err = createTxn.Commit(ctxWithTimeout)
	require.NoError(t, err)
}

// loadObjectMapFromDir loads objectmap from a directory
func loadObjectMapFromDir(ctx context.Context, dir string) (map[objectio.ObjectId]*publication.ObjectWithTableInfo, error) {
	// Read objectmap.json from directory
	// Note: CollectAndExportObjects writes to dir/objectmap.json using fileservice
	// So we need to read from the same location
	objectMapFile := filepath.Join(dir, "objectmap.json")
	jsonData, err := os.ReadFile(objectMapFile)
	if err != nil {
		return nil, moerr.NewInternalError(ctx, fmt.Sprintf("failed to read objectmap file %s: %v", objectMapFile, err))
	}

	// Check if file is empty or contains invalid content
	if len(jsonData) == 0 {
		return nil, moerr.NewInternalError(ctx, fmt.Sprintf("objectmap file %s is empty", objectMapFile))
	}

	// Check if it looks like HTML (common error case)
	if len(jsonData) > 0 && jsonData[0] == '<' {
		return nil, moerr.NewInternalError(ctx, fmt.Sprintf("objectmap file %s contains HTML instead of JSON (first 100 chars: %s)", objectMapFile, string(jsonData[:min(100, len(jsonData))])))
	}

	// Parse JSON
	var objectMapJSON map[string]ObjectMapJSON
	if err := json.Unmarshal(jsonData, &objectMapJSON); err != nil {
		return nil, moerr.NewInternalError(ctx, fmt.Sprintf("failed to unmarshal objectmap from %s: %v (first 200 chars: %s)", objectMapFile, err, string(jsonData[:min(200, len(jsonData))])))
	}

	// Convert to objectMap
	objectMap := make(map[objectio.ObjectId]*publication.ObjectWithTableInfo)
	for objIDStr, objJSON := range objectMapJSON {
		// Parse ObjectId from string
		// Format: "{segment}_{offset}" where segment is UUID string and offset is uint16
		parts := strings.SplitN(objIDStr, "_", 2)
		if len(parts) != 2 {
			return nil, moerr.NewInternalError(ctx, fmt.Sprintf("invalid object id format: %s", objIDStr))
		}
		segment, err := types.ParseUuid(parts[0])
		if err != nil {
			return nil, moerr.NewInternalError(ctx, fmt.Sprintf("failed to parse segment UUID %s: %v", parts[0], err))
		}
		offsetUint, err := strconv.ParseUint(parts[1], 10, 16)
		if err != nil {
			return nil, moerr.NewInternalError(ctx, fmt.Sprintf("failed to parse offset %s: %v", parts[1], err))
		}
		offset := uint16(offsetUint)
		var objID objectio.ObjectId
		copy(objID[:types.UuidSize], segment[:])
		copy(objID[types.UuidSize:types.UuidSize+2], types.EncodeUint16(&offset))

		// Decode stats from base64
		statsBytes, err := base64.StdEncoding.DecodeString(objJSON.Stats)
		if err != nil {
			return nil, moerr.NewInternalError(ctx, fmt.Sprintf("failed to decode stats for object %s: %v", objIDStr, err))
		}

		var stats objectio.ObjectStats
		if len(statsBytes) == objectio.ObjectStatsLen {
			stats.UnMarshal(statsBytes)
		}

		objectMap[objID] = &publication.ObjectWithTableInfo{
			Stats:       stats,
			DBName:      objJSON.DBName,
			TableName:   objJSON.TableName,
			IsTombstone: objJSON.IsTombstone,
			Delete:      objJSON.Delete,
		}
	}

	return objectMap, nil
}

type applyObjectCase struct {
	ddlFn     func(t *testing.T, disttaeEngine *testutil.TestDisttaeEngine)
	preDataFn func(
		t *testing.T,
		dir string,
		collectFn func(
			ctx context.Context,
			fs fileservice.FileService,
			dir string,
			tableEntry *catalog.TableEntry,
			dbname string,
			tablename string,
			fromts, tots types.TS,
		) error,
	)
	checkFn func()
}

var applyObjectCases = []applyObjectCase{
	{
		ddlFn:     DDLAppend,
		preDataFn: PrepareDataAppend,
	},
}

func TestApplyObjects(t *testing.T) {
	dir := "/tmp/test_apply_objects"
	for _, testCase := range applyObjectCases {
		testCase.preDataFn(t, dir, CollectAndExportObjects)
		runApplyObjects(t, dir, testCase.ddlFn)

	}
}

func runApplyObjects(
	t *testing.T,
	dir string,
	ddlFn func(t *testing.T, disttaeEngine *testutil.TestDisttaeEngine),
) {
	pkgcatalog.SetupDefines("")

	var (
		accountID = pkgcatalog.System_Account
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountID)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	// Start cluster
	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	exportDir := dir
	// Verify objectmap.json file exists and is readable
	objectMapFile := filepath.Join(exportDir, "objectmap.json")
	if _, err := os.Stat(objectMapFile); err != nil {
		t.Fatalf("objectmap.json file does not exist at %s: %v", objectMapFile, err)
	}

	// Load objectmap from directory
	objectMap, err := loadObjectMapFromDir(ctx, exportDir)
	require.NoError(t, err)

	// Create empty aobjectMap and indexTableMappings
	aobjectMap := make(map[objectio.ObjectId]publication.AObjMapping)
	indexTableMappings := make(map[string]string)

	// Get fileservice from taeHandler (which has the fileservice)
	fs := taeHandler.GetDB().Opts.Fs

	// Create mock upstream executor (not used since we'll stub GetObjectFromUpstream)
	var upstreamExecutor publication.SQLExecutor

	// Create mpool
	mp := mpool.MustNewZero()
	defer func() {
		var buf []byte
		mp.Free(buf)
	}()

	// Stub GetObjectFromUpstream to read from directory
	stub := gostub.Stub(
		&publication.GetObjectFromUpstream,
		func(ctx context.Context, executor publication.SQLExecutor, objectName string) ([]byte, error) {
			// Read object file from directory
			objectPath := filepath.Join(exportDir, objectName)
			data, err := os.ReadFile(objectPath)
			assert.NoError(t, err)
			return data[4:], nil
		},
	)
	defer stub.Reset()

	ddlFn(t, disttaeEngine)
	// Create txn from disttaeEngine
	cnTxn, err := disttaeEngine.NewTxnOperator(ctxWithTimeout, disttaeEngine.Now())
	require.NoError(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
	// Call ApplyObjects
	taskID := "test-task-1"
	currentTS := types.TimestampToTS(disttaeEngine.Now())
	err = publication.ApplyObjects(
		ctxWithTimeout,
		taskID,
		accountID,
		indexTableMappings,
		aobjectMap,
		objectMap,
		upstreamExecutor,
		currentTS,
		cnTxn,
		disttaeEngine.Engine,
		mp,
		fs,
	)
	require.NoError(t, err)

	// Commit txn
	err = cnTxn.Commit(ctxWithTimeout)
	require.NoError(t, err)
	t.Log(taeHandler.GetDB().Catalog.SimplePPString(3))
}
