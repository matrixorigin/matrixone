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

package logtail

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	catalog2 "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

const (
	SnapshotTypeIdx types.Enum = iota
	SnapshotTypeCluster
	SnapshotTypeAccount
	SnapshotTypeDatabase
	SnapshotTypeTable
)

// mo_snapshot's schema
const (
	ColSnapshotId uint16 = iota
	ColSName
	ColTS
	ColLevel
	ColAccountName
	ColDatabaseName
	ColTableName
	ColObjId
)

const (
	TableInfoTypeIdx types.Enum = iota
	SnapshotTidIdx
	AObjectDelIdx
	PitrTidIdx
	IscpTidIdx
)

const MoTablesPK = "mo_tables_pk"

const (
	PitrUnitYear   = "y"
	PitrUnitMonth  = "mo"
	PitrUnitDay    = "d"
	PitrUnitHour   = "h"
	PitrUnitMinute = "m"
)

const (
	PitrLevelCluster  = "cluster"
	PitrLevelAccount  = "account"
	PitrLevelDatabase = "database"
	PitrLevelTable    = "table"
)

// pitr's schema
const (
	ColPitrId uint16 = iota
	ColPitrName
	ColPitrCreateAccount
	ColPitrCreateTime
	ColPitrModifiedTime
	ColPitrLevel
	ColPitrAccountId
	ColPitrAccountName
	ColPitrDatabaseName
	ColPitrTableName
	ColPitrObjId
	ColPitrLength
	ColPitrUnit
)

// iscp schema
const (
	ColIscpAccountId uint16 = iota
	ColIscpTableId
	ColIscpJobName
	ColIscpJobId
	ColIscpJobSpec
	ColIscpJobState
	ColIscpWatermark
	ColIscpJobStatus
	ColIscpCreateAt
	ColIscpDropAt
)

var (
	objectInfoSchemaAttr = []string{
		catalog.ObjectAttr_ObjectStats,
		catalog.EntryNode_CreateAt,
		catalog.EntryNode_DeleteAt,
		catalog2.BlockMeta_DeltaLoc,
		SnapshotAttr_TID,
	}
	objectInfoSchemaTypes = []types.Type{
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_TS, types.MaxVarcharLen, 0),
		types.New(types.T_TS, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, 5000, 0),
		types.New(types.T_uint64, 0, 0),
	}

	tableInfoSchemaAttr = []string{
		catalog2.SystemColAttr_AccID,
		catalog2.SystemRelAttr_DBID,
		SnapshotAttr_TID,
		catalog2.SystemRelAttr_CreateAt,
		catalog.EntryNode_DeleteAt,
		MoTablesPK,
	}

	tableInfoSchemaTypes = []types.Type{
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_TS, types.MaxVarcharLen, 0),
		types.New(types.T_TS, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
	}

	snapshotSchemaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_int64, 0, 0),
		types.New(types.T_enum, 0, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_uint64, 0, 0),
	}

	aObjectDelSchemaAttr = []string{
		catalog.EntryNode_DeleteAt,
	}

	aObjectDelSchemaTypes = []types.Type{
		types.New(types.T_TS, types.MaxVarcharLen, 0),
	}
)

type objectInfo struct {
	stats    objectio.ObjectStats
	createAt types.TS
	deleteAt types.TS
}

type tableInfo struct {
	accountID uint32
	dbID      uint64
	tid       uint64
	createAt  types.TS
	deleteAt  types.TS
	pk        string
}

// SnapshotInfo represents snapshot information at different levels
// Shared structure for both PITR and Snapshot functionality
type SnapshotInfo struct {
	cluster  []types.TS
	account  map[uint32][]types.TS
	database map[uint64][]types.TS
	tables   map[uint64][]types.TS
}

// PitrInfo is an alias for backward compatibility
type PitrInfo = SnapshotInfo

func NewPitrInfo() *PitrInfo {
	return &PitrInfo{
		cluster:  make([]types.TS, 1),
		account:  make(map[uint32][]types.TS),
		database: make(map[uint64][]types.TS),
		tables:   make(map[uint64][]types.TS),
	}
}

func NewSnapshotInfo() *SnapshotInfo {
	return &SnapshotInfo{
		cluster:  make([]types.TS, 0),
		account:  make(map[uint32][]types.TS),
		database: make(map[uint64][]types.TS),
		tables:   make(map[uint64][]types.TS),
	}
}

func (p *SnapshotInfo) IsEmpty() bool {
	if p == nil {
		return true
	}
	for _, ts := range p.cluster {
		if !ts.IsEmpty() {
			return false
		}
	}
	for _, timestamps := range p.account {
		for _, ts := range timestamps {
			if !ts.IsEmpty() {
				return false
			}
		}
	}
	for _, timestamps := range p.database {
		for _, ts := range timestamps {
			if !ts.IsEmpty() {
				return false
			}
		}
	}
	for _, timestamps := range p.tables {
		for _, ts := range timestamps {
			if !ts.IsEmpty() {
				return false
			}
		}
	}
	return true
}

// GetTS returns the earliest applicable timestamp for PITR usage
// For PITR, we only need the first (earliest) timestamp from each level
func (p *SnapshotInfo) GetTS(
	accountID uint32,
	dbID uint64,
	tableID uint64,
) (ts types.TS) {
	if p == nil {
		return
	}
	// Get the first cluster timestamp (for PITR)
	if len(p.cluster) > 0 {
		ts = p.cluster[0]
	}

	// Get the first account timestamp
	if accountTSList := p.account[accountID]; len(accountTSList) > 0 {
		accountTS := accountTSList[0]
		if ts.IsEmpty() || accountTS.LT(&ts) {
			ts = accountTS
		}
	}

	// Get the first database timestamp
	if dbTSList := p.database[dbID]; len(dbTSList) > 0 {
		dbTS := dbTSList[0]
		if ts.IsEmpty() || dbTS.LT(&ts) {
			ts = dbTS
		}
	}

	// Get the first table timestamp
	if tableTSList := p.tables[tableID]; len(tableTSList) > 0 {
		tableTS := tableTSList[0]
		if ts.IsEmpty() || tableTS.LT(&ts) {
			ts = tableTS
		}
	}
	return
}

// GetSnapshotsByLevel returns all snapshots for a specific level and object ID
func (p *SnapshotInfo) GetSnapshotsByLevel(level string, objID uint64) []types.TS {
	if p == nil {
		return nil
	}
	switch level {
	case PitrLevelCluster:
		return p.cluster
	case PitrLevelAccount:
		if objID > math.MaxUint32 {
			return nil
		}
		return p.account[uint32(objID)]
	case PitrLevelDatabase:
		return p.database[objID]
	case PitrLevelTable:
		return p.tables[objID]
	default:
		return nil
	}
}

func (p *SnapshotInfo) MinTS() (ts types.TS) {
	if p == nil {
		return
	}
	// find the minimum cluster ts
	for _, clusterTS := range p.cluster {
		if ts.IsEmpty() || clusterTS.LT(&ts) {
			ts = clusterTS
		}
	}

	// find the minimum account ts
	for _, tsList := range p.account {
		for _, accountTS := range tsList {
			if ts.IsEmpty() || accountTS.LT(&ts) {
				ts = accountTS
			}
		}
	}

	// find the minimum database ts
	for _, tsList := range p.database {
		for _, dbTS := range tsList {
			if ts.IsEmpty() || dbTS.LT(&ts) {
				ts = dbTS
			}
		}
	}

	// find the minimum table ts
	for _, tsList := range p.tables {
		for _, tableTS := range tsList {
			if ts.IsEmpty() || tableTS.LT(&ts) {
				ts = tableTS
			}
		}
	}
	return
}

func (p *SnapshotInfo) ToTsList() []types.TS {
	if p == nil {
		return nil
	}
	var totalCount int
	totalCount += len(p.cluster)
	for _, tsList := range p.account {
		totalCount += len(tsList)
	}
	for _, tsList := range p.database {
		totalCount += len(tsList)
	}
	for _, tsList := range p.tables {
		totalCount += len(tsList)
	}

	result := make([]types.TS, 0, totalCount)

	// Add cluster timestamps
	result = append(result, p.cluster...)

	// Add account timestamps
	for _, tsList := range p.account {
		result = append(result, tsList...)
	}

	// Add database timestamps
	for _, tsList := range p.database {
		result = append(result, tsList...)
	}

	// Add table timestamps
	for _, tsList := range p.tables {
		result = append(result, tsList...)
	}

	return result
}

// Special table information structure, used to process special tables such as PITR and ISCP
type specialTableInfo struct {
	tid        uint64
	objects    map[objectio.Segmentid]*objectInfo
	tombstones map[objectio.Segmentid]*objectInfo
}

func (st *specialTableInfo) init() {
	st.objects = make(map[objectio.Segmentid]*objectInfo)
	st.tombstones = make(map[objectio.Segmentid]*objectInfo)
}

func (st *specialTableInfo) reset() {
	st.objects = nil
	st.tombstones = nil
	st.init()
}

func (st *specialTableInfo) trim() {
	for id, info := range st.objects {
		if info == nil || !info.deleteAt.IsEmpty() {
			delete(st.objects, id)
		}
	}
	for id, info := range st.tombstones {
		if info == nil || !info.deleteAt.IsEmpty() {
			delete(st.tombstones, id)
		}
	}
}

func (st *specialTableInfo) getTombstonesStats() ([]objectio.ObjectStats, error) {
	tombstonesStats := make([]objectio.ObjectStats, 0)
	for _, obj := range st.tombstones {
		if obj == nil {
			return nil, moerr.NewInternalErrorNoCtx(
				"special table has nil tombstone metadata",
			)
		}
		tombstonesStats = append(tombstonesStats, obj.stats)
	}
	return tombstonesStats, nil
}

// General object processing functions
func (st *specialTableInfo) processObjects(
	ctx context.Context,
	fs fileservice.FileService,
	idxes []uint16,
	ds *BackupDeltaLocDataSource,
	mp *mpool.MPool,
	validator func(bat *batch.Batch) error,
	processor func(bat *batch.Batch, r int) error,
) error {
	if ctx == nil || fs == nil || ds == nil || mp == nil || processor == nil {
		return moerr.NewInvalidInputNoCtx(
			"special table processing requires context, file service, data source, mpool, and processor",
		)
	}
	for _, object := range st.objects {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		default:
		}
		if object == nil {
			return moerr.NewInternalError(ctx, "special table has nil object metadata")
		}
		name := object.stats.ObjectName()
		if err := forEachObjectBlockLocation(ctx, object.stats, func(loc objectio.Location) error {
			blk := objectio.BlockInfo{
				BlockID: *objectio.BuildObjectBlockid(name, loc.ID()),
				MetaLoc: objectio.ObjectLocation(loc),
			}

			bat, _, err := blockio.BlockDataReadBackup(ctx, &blk, ds, idxes, types.TS{}, fs)
			if err != nil {
				return err
			}
			if err = func() error {
				if bat == nil {
					return moerr.NewInternalError(ctx, "special table block has an invalid column set")
				}
				defer bat.Clean(common.DebugAllocator)
				if len(bat.Vecs) != len(idxes) || len(bat.Vecs) == 0 {
					return moerr.NewInternalError(ctx, "special table block has an invalid column set")
				}
				rowCount := bat.RowCount()
				for pos, vec := range bat.Vecs {
					if vec == nil || vec.Length() != rowCount {
						return moerr.NewInternalErrorf(
							ctx, "special table block column %d is malformed", pos,
						)
					}
				}
				if validator != nil {
					if validateErr := validator(bat); validateErr != nil {
						return validateErr
					}
				}
				for r := 0; r < rowCount; r++ {
					if processErr := processor(bat, r); processErr != nil {
						return processErr
					}
				}
				return nil
			}(); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (st *specialTableInfo) clone() *specialTableInfo {
	clone := &specialTableInfo{
		tid:        st.tid,
		objects:    make(map[objectio.Segmentid]*objectInfo),
		tombstones: make(map[objectio.Segmentid]*objectInfo),
	}
	for id, info := range st.objects {
		if info == nil {
			clone.objects[id] = nil
			continue
		}
		clone.objects[id] = &objectInfo{
			stats:    info.stats,
			createAt: info.createAt,
			deleteAt: info.deleteAt,
		}
	}
	for id, info := range st.tombstones {
		if info == nil {
			clone.tombstones[id] = nil
			continue
		}
		clone.tombstones[id] = &objectInfo{
			stats:    info.stats,
			createAt: info.createAt,
			deleteAt: info.deleteAt,
		}
	}
	return clone
}

type SnapshotMeta struct {
	sync.RWMutex
	// updateMu preserves checkpoint application order while the read-only
	// checkpoint scan runs outside RWMutex. Readers are not blocked by that
	// scan, but two updates still cannot overtake each other.
	updateMu sync.Mutex

	// all objects&tombstones in the mo_snapshots table, because there
	// will be multiple mo_snapshots, so here is a map, the key is tid.
	objects    map[uint64]map[objectio.Segmentid]*objectInfo
	tombstones map[uint64]map[objectio.Segmentid]*objectInfo

	aobjDelTsMap map[types.TS]struct{} // used for filering out transferred tombstones

	pitr specialTableInfo
	iscp specialTableInfo

	// tables records all the table information of mo, the key is account id,
	// and the map is the mapping of table id and table information.
	//
	// tables is used to facilitate the use of an account id to obtain
	// all table information under the account.
	tables map[uint32]map[uint64]*tableInfo

	// tableIDIndex records all the index information of mo, the key is
	// account id, and the value is the tableInfo
	tableIDIndex map[uint64]*tableInfo

	// tablePKIndex records all the index information of mo, the key is
	// the mo_table pk, and the value is the tableInfo
	tablePKIndex map[string][]*tableInfo

	// the key of snapshotTableIDs is the table id of a snapshot table
	// each account has one dedicated snapshot table
	snapshotTableIDs map[uint64]struct{}
}

func NewSnapshotMeta() *SnapshotMeta {
	meta := &SnapshotMeta{
		objects:          make(map[uint64]map[objectio.Segmentid]*objectInfo),
		tombstones:       make(map[uint64]map[objectio.Segmentid]*objectInfo),
		aobjDelTsMap:     make(map[types.TS]struct{}),
		tables:           make(map[uint32]map[uint64]*tableInfo),
		tableIDIndex:     make(map[uint64]*tableInfo),
		snapshotTableIDs: make(map[uint64]struct{}),
		tablePKIndex:     make(map[string][]*tableInfo),
	}
	meta.pitr.init()
	meta.iscp.init()
	return meta
}

func copyObjectsLocked(
	objects map[uint64]map[objectio.Segmentid]*objectInfo,
) map[uint64]map[objectio.Segmentid]*objectInfo {
	newMap := make(map[uint64]map[objectio.Segmentid]*objectInfo)
	for k, v := range objects {
		newMap[k] = make(map[objectio.Segmentid]*objectInfo)
		for kk, vv := range v {
			if vv == nil {
				newMap[k][kk] = nil
				continue
			}
			entry := *vv
			newMap[k][kk] = &entry
		}
	}
	return newMap
}

func IsMoTable(tid uint64) bool {
	return tid == catalog2.MO_TABLES_ID
}

type tombstone struct {
	rowid types.Rowid
	pk    types.Tuple
	ts    types.TS
}

func validateObjectStatsBlockCount(
	ctx context.Context,
	stats objectio.ObjectStats,
) (int, error) {
	blockCount := stats.BlkCnt()
	if blockCount == 0 {
		return 0, moerr.NewInternalErrorf(
			ctx, "object %s has no blocks", stats.ObjectName().String(),
		)
	}
	if blockCount > uint32(math.MaxUint16)+1 {
		return 0, moerr.NewInternalErrorf(
			ctx, "object %s has unsupported block count %d",
			stats.ObjectName().String(), blockCount,
		)
	}
	return int(blockCount), nil
}

func forEachObjectBlockLocation(
	ctx context.Context,
	stats objectio.ObjectStats,
	consume func(objectio.Location) error,
) error {
	if ctx == nil || consume == nil {
		return moerr.NewInvalidInputNoCtx("object block iteration requires context and a consumer")
	}
	blockCount, err := validateObjectStatsBlockCount(ctx, stats)
	if err != nil {
		return err
	}
	for block := 0; block < blockCount; block++ {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		default:
		}
		location := stats.ObjectLocation()
		location.SetID(uint16(block))
		if err := consume(location); err != nil {
			return err
		}
	}
	return nil
}

type checkpointObjectMutation struct {
	tid                uint64
	stats              objectio.ObjectStats
	createTS, deleteTS types.TS
}

type snapshotTableMutation struct {
	name, dbName string
	tid, db      uint64
	account      uint32
	createAt     types.TS
	pk           string
}

type tableInfoUpdatePlan struct {
	tableMutations        []snapshotTableMutation
	deleteRows            []tombstone
	pendingAObjectDeletes map[types.TS]struct{}
	startTS, endTS        types.TS
}

func collectCheckpointObjectMutations(
	ctx context.Context,
	data *CKPReader,
) (
	dataMutations []checkpointObjectMutation,
	tombstoneMutations []checkpointObjectMutation,
	err error,
) {
	if ctx == nil || data == nil {
		return nil, nil, moerr.NewInvalidInputNoCtx(
			"snapshot checkpoint collection requires context and checkpoint data",
		)
	}
	// CKPReader.ForEachRow uses unchecked vector access internally and can
	// panic when persisted checkpoint metadata is malformed. A corrupt input
	// must fail this update without terminating the process or publishing the
	// mutations collected before the malformed row.
	defer func() {
		if recovered := recover(); recovered != nil {
			dataMutations = nil
			tombstoneMutations = nil
			err = moerr.NewInternalErrorf(
				ctx, "snapshot checkpoint iteration failed: %v", recovered,
			)
		}
	}()
	dataMutations = make([]checkpointObjectMutation, 0)
	tombstoneMutations = make([]checkpointObjectMutation, 0)
	err = data.ForEachRow(
		ctx,
		func(
			account uint32,
			dbid, tid uint64,
			objectType int8,
			stats objectio.ObjectStats,
			createTS, deleteTS types.TS,
			rowID types.Rowid,
		) error {
			mutation := checkpointObjectMutation{
				tid: tid, stats: stats, createTS: createTS, deleteTS: deleteTS,
			}
			switch objectType {
			case ckputil.ObjectType_Data:
				dataMutations = append(dataMutations, mutation)
			case ckputil.ObjectType_Tombstone:
				tombstoneMutations = append(tombstoneMutations, mutation)
			default:
				return moerr.NewInternalErrorf(
					ctx, "snapshot checkpoint row has unknown object type %d", objectType,
				)
			}
			return nil
		},
	)
	if err != nil {
		return nil, nil, err
	}
	return dataMutations, tombstoneMutations, nil
}

func (sm *SnapshotMeta) updateTableInfo(
	ctx context.Context,
	fs fileservice.FileService,
	data *CKPReader, startts, endts types.TS,
) error {
	if sm == nil {
		return moerr.NewInvalidInputNoCtx("snapshot table update requires metadata")
	}
	if endts.LT(&startts) {
		return moerr.NewInvalidInputNoCtxf(
			"snapshot checkpoint range %s-%s is reversed",
			startts.ToString(), endts.ToString(),
		)
	}
	dataMutations, tombstoneMutations, err := collectCheckpointObjectMutations(ctx, data)
	if err != nil {
		return err
	}
	plan, err := collectTableInfoUpdatePlan(
		ctx, fs, dataMutations, tombstoneMutations, startts, endts,
	)
	if err != nil {
		return err
	}
	sm.Lock()
	defer sm.Unlock()
	return sm.applyTableInfoUpdatePlan(plan)
}

func collectTableInfoUpdatePlan(
	ctx context.Context,
	fs fileservice.FileService,
	dataMutations, tombstoneMutations []checkpointObjectMutation,
	startts, endts types.TS,
) (plan *tableInfoUpdatePlan, err error) {
	if ctx == nil || fs == nil {
		return nil, moerr.NewInvalidInputNoCtx(
			"snapshot table update planning requires context and file service",
		)
	}
	if endts.LT(&startts) {
		return nil, moerr.NewInvalidInputNoCtxf(
			"snapshot checkpoint range %s-%s is reversed",
			startts.ToString(), endts.ToString(),
		)
	}
	// Persisted vectors are decoded through several legacy helpers that use
	// unchecked access. Convert structural panics into an atomic update error;
	// per-block defers below still release the currently loaded batch.
	defer func() {
		if recovered := recover(); recovered != nil {
			plan = nil
			err = moerr.NewInternalErrorf(
				ctx, "snapshot table update planning failed: %v", recovered,
			)
		}
	}()
	var objects map[uint64]map[objectio.Segmentid]*objectInfo
	var tombstones map[uint64]map[objectio.Segmentid]*objectInfo
	objects = make(map[uint64]map[objectio.Segmentid]*objectInfo, 1)
	tombstones = make(map[uint64]map[objectio.Segmentid]*objectInfo, 1)
	objects[catalog2.MO_TABLES_ID] = make(map[objectio.Segmentid]*objectInfo)
	tombstones[catalog2.MO_TABLES_ID] = make(map[objectio.Segmentid]*objectInfo)
	collector := func(
		objects *map[uint64]map[objectio.Segmentid]*objectInfo,
		_ *map[objectio.Segmentid]*objectInfo,
		_ *map[objectio.Segmentid]*objectInfo,
		tid uint64,
		stats objectio.ObjectStats,
		createTS types.TS, deleteTS types.TS,
	) {
		if !IsMoTable(tid) {
			return
		}
		if !stats.GetAppendable() {
			// mo_table only consumes appendable object
			return
		}
		id := stats.ObjectName().SegmentId()
		moTable := (*objects)[tid]

		// dropped object will overwrite the created object, updating the deleteAt
		obj := moTable[id]
		if obj == nil {
			moTable[id] = &objectInfo{
				stats: stats,
			}
		}
		if !createTS.IsEmpty() {
			moTable[id].createAt = createTS
		}
		if !deleteTS.IsEmpty() {
			moTable[id].deleteAt = deleteTS
		}
	}
	for _, mutation := range dataMutations {
		collector(
			&objects, nil, nil, mutation.tid, mutation.stats,
			mutation.createTS, mutation.deleteTS,
		)
	}
	for _, mutation := range tombstoneMutations {
		collector(
			&tombstones, nil, nil, mutation.tid, mutation.stats,
			mutation.createTS, mutation.deleteTS,
		)
	}
	tObjects := objects[catalog2.MO_TABLES_ID]
	tTombstones := tombstones[catalog2.MO_TABLES_ID]
	orderedInfos := make([]*objectInfo, 0, len(tObjects))
	for _, info := range tObjects {
		orderedInfos = append(orderedInfos, info)
	}
	slices.SortFunc(orderedInfos, func(a, b *objectInfo) int {
		return a.createAt.Compare(&b.createAt)
	})
	tableMutations := make([]snapshotTableMutation, 0)
	pendingAObjectDeleteTS := make(map[types.TS]struct{})

	for _, obj := range orderedInfos {
		if !obj.deleteAt.IsEmpty() {
			pendingAObjectDeleteTS[obj.deleteAt] = struct{}{}
		}
		if err := forEachObjectBlockLocation(ctx, obj.stats, func(location objectio.Location) error {
			objectBat, _, specialLayout, err := ioutil.LoadOneBlockWithSpecialLayout(
				ctx, fs, location, objectio.SchemaData,
			)
			if err != nil {
				return err
			}
			if objectBat == nil {
				return moerr.NewInternalError(ctx, "snapshot table object loader returned no batch")
			}
			defer objectBat.Clean(common.DebugAllocator)
			commitPos, ok := specialLayout.Resolve(objectio.SEQNUM_COMMITTS)
			if !ok {
				return moerr.NewInternalError(ctx, "snapshot table object has no commit timestamp")
			}
			// 0 is table id
			// 1 is table name
			// 11 is account id
			rowCount := objectBat.RowCount()
			requiredPositions := []int{0, 1, 2, 3, 11, catalog2.MO_TABLES_CPKEY_IDX, int(commitPos)}
			if abortPos, hasAbort := specialLayout.Resolve(objectio.SEQNUM_ABORT); hasAbort {
				requiredPositions = append(requiredPositions, int(abortPos))
			}
			for _, pos := range requiredPositions {
				if pos < 0 || pos >= len(objectBat.Vecs) || objectBat.Vecs[pos] == nil {
					return moerr.NewInternalErrorf(ctx, "snapshot table object is missing column %d", pos)
				}
			}
			for pos, vec := range objectBat.Vecs {
				if vec == nil || vec.Length() != rowCount {
					return moerr.NewInternalErrorf(
						ctx, "snapshot table object column %d has invalid logical row count", pos,
					)
				}
			}
			if objectBat.Vecs[0].GetType().Oid != types.T_uint64 ||
				!objectBat.Vecs[1].GetType().IsVarlen() ||
				!objectBat.Vecs[2].GetType().IsVarlen() ||
				objectBat.Vecs[3].GetType().Oid != types.T_uint64 ||
				objectBat.Vecs[11].GetType().Oid != types.T_uint32 ||
				!objectBat.Vecs[catalog2.MO_TABLES_CPKEY_IDX].GetType().IsVarlen() {
				return moerr.NewInternalError(ctx, "snapshot table object has invalid system column types")
			}
			for _, pos := range []int{0, 1, 2, 3, 11, catalog2.MO_TABLES_CPKEY_IDX} {
				if objectBat.Vecs[pos].GetNulls().Any() {
					return moerr.NewInternalErrorf(ctx, "snapshot table object column %d contains null rows", pos)
				}
			}
			creates, validateErr := ioutil.ValidateTombstoneCommitTSColumn(
				rowCount, objectBat.Vecs[commitPos],
			)
			if validateErr != nil {
				return validateErr
			}
			var aborts ioutil.TombstoneAbortColumn
			if abortPos, ok := specialLayout.Resolve(objectio.SEQNUM_ABORT); ok {
				if int(abortPos) >= len(objectBat.Vecs) {
					return moerr.NewInternalError(ctx, "snapshot table object has invalid abort position")
				}
				abortVec := objectBat.Vecs[abortPos]
				aborts, validateErr = ioutil.ValidateTombstoneAbortColumn(rowCount, abortVec)
				if validateErr != nil {
					return validateErr
				}
			}
			for i := 0; i < rowCount; i++ {
				if i&1023 == 0 {
					select {
					case <-ctx.Done():
						return context.Cause(ctx)
					default:
					}
				}
				if aborts.IsPresent() && aborts.At(i) {
					continue
				}
				createAt := creates.At(i)
				if createAt.Equal(&txnif.UncommitTS) {
					continue
				}
				if createAt.LT(&startts) || createAt.GT(&endts) {
					continue
				}
				name := string(objectBat.Vecs[1].GetBytesAt(i))
				dbName := string(objectBat.Vecs[2].GetBytesAt(i))
				tid := vector.GetFixedAtNoTypeCheck[uint64](objectBat.Vecs[0], i)
				account := vector.GetFixedAtNoTypeCheck[uint32](objectBat.Vecs[11], i)
				db := vector.GetFixedAtNoTypeCheck[uint64](objectBat.Vecs[3], i)
				var tuple types.Tuple
				tuple, _, _, err = types.DecodeTuple(
					objectBat.Vecs[catalog2.MO_TABLES_CPKEY_IDX].GetRawBytesAt(i))
				if err != nil {
					return err
				}
				tableMutations = append(tableMutations, snapshotTableMutation{
					name: name, dbName: dbName, tid: tid, db: db, account: account,
					createAt: createAt, pk: tuple.ErrString(nil),
				})
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}

	deleteRows := make([]tombstone, 0)
	for _, obj := range tTombstones {
		if err := forEachObjectBlockLocation(ctx, obj.stats, func(location objectio.Location) error {
			objectBat, _, layout, err := loadOneBlockWithBackupLayout(ctx, fs, location)
			if err != nil {
				return err
			}
			if objectBat == nil {
				return moerr.NewInternalError(ctx, "snapshot tombstone object loader returned no batch")
			}
			defer objectBat.Clean(common.DebugAllocator)

			commitPos, validateErr := validateBackupTombstoneBatch(ctx, objectBat, layout)
			if validateErr != nil {
				return validateErr
			}
			rowCount := objectBat.RowCount()
			rowIDs, validateErr := ioutil.ValidateTombstoneRowIDColumn(
				rowCount, objectBat.Vecs[0],
			)
			if validateErr != nil {
				return validateErr
			}
			commitTSs, validateErr := ioutil.ValidateTombstoneCommitTSColumn(
				rowCount, objectBat.Vecs[commitPos],
			)
			if validateErr != nil {
				return validateErr
			}
			var aborts ioutil.TombstoneAbortColumn
			if abortPos, ok := layout.Resolve(objectio.SEQNUM_ABORT); ok {
				if int(abortPos) >= len(objectBat.Vecs) {
					return moerr.NewInternalError(ctx, "snapshot tombstone object has invalid abort position")
				}
				abortVec := objectBat.Vecs[abortPos]
				aborts, validateErr = ioutil.ValidateTombstoneAbortColumn(rowCount, abortVec)
				if validateErr != nil {
					return validateErr
				}
			}
			for i := 0; i < rowCount; i++ {
				if i&1023 == 0 {
					select {
					case <-ctx.Done():
						return context.Cause(ctx)
					default:
					}
				}
				if aborts.IsPresent() && aborts.At(i) {
					continue
				}
				pk, _, _, decodeErr := types.DecodeTuple(objectBat.Vecs[1].GetRawBytesAt(i))
				if decodeErr != nil {
					return decodeErr
				}
				commitTs := commitTSs.At(i)
				if commitTs.Equal(&txnif.UncommitTS) {
					continue
				}
				if commitTs.LT(&startts) || commitTs.GT(&endts) {
					continue
				}
				_, pendingPublish := pendingAObjectDeleteTS[commitTs]
				if pendingPublish {
					logutil.Infof("yyyy skip table %v @ %v", pk.ErrString(nil), commitTs.ToString())
					continue
				}
				deleteRows = append(deleteRows, tombstone{
					rowid: rowIDs[i],
					pk:    pk,
					ts:    commitTs,
				})
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	slices.SortFunc(deleteRows, func(a, b tombstone) int {
		return a.ts.Compare(&b.ts)
	})
	return &tableInfoUpdatePlan{
		tableMutations:        tableMutations,
		deleteRows:            deleteRows,
		pendingAObjectDeletes: pendingAObjectDeleteTS,
		startTS:               startts,
		endTS:                 endts,
	}, nil
}

// applyTableInfoUpdatePlan publishes a fully validated, I/O-free plan. The
// caller holds SnapshotMeta's write lock, so readers observe either the old
// state or the complete checkpoint update.
func (sm *SnapshotMeta) applyTableInfoUpdatePlan(plan *tableInfoUpdatePlan) error {
	if plan == nil {
		return moerr.NewInvalidInputNoCtx("snapshot table update plan is nil")
	}
	// Validate pointer-bearing current state before changing anything. A failed
	// update must not leave a half-published table/index graph.
	for account, tables := range sm.tables {
		for tid, table := range tables {
			if table == nil {
				return moerr.NewInternalErrorNoCtxf(
					"snapshot account %d table %d has nil metadata", account, tid,
				)
			}
		}
	}
	for tid, table := range sm.tableIDIndex {
		if table == nil {
			return moerr.NewInternalErrorNoCtxf(
				"snapshot table index %d has nil metadata", tid,
			)
		}
	}
	for pk, tables := range sm.tablePKIndex {
		for position, table := range tables {
			if table == nil {
				return moerr.NewInternalErrorNoCtxf(
					"snapshot primary-key index %q entry %d has nil metadata", pk, position,
				)
			}
		}
	}
	if sm.aobjDelTsMap == nil {
		sm.aobjDelTsMap = make(map[types.TS]struct{})
	}
	if sm.tables == nil {
		sm.tables = make(map[uint32]map[uint64]*tableInfo)
	}
	if sm.tableIDIndex == nil {
		sm.tableIDIndex = make(map[uint64]*tableInfo)
	}
	if sm.tablePKIndex == nil {
		sm.tablePKIndex = make(map[string][]*tableInfo)
	}
	if sm.snapshotTableIDs == nil {
		sm.snapshotTableIDs = make(map[uint64]struct{})
	}
	if sm.pitr.objects == nil {
		sm.pitr.objects = make(map[objectio.Segmentid]*objectInfo)
	}
	if sm.pitr.tombstones == nil {
		sm.pitr.tombstones = make(map[objectio.Segmentid]*objectInfo)
	}
	if sm.iscp.objects == nil {
		sm.iscp.objects = make(map[objectio.Segmentid]*objectInfo)
	}
	if sm.iscp.tombstones == nil {
		sm.iscp.tombstones = make(map[objectio.Segmentid]*objectInfo)
	}
	for deleteTS := range plan.pendingAObjectDeletes {
		sm.aobjDelTsMap[deleteTS] = struct{}{}
	}
	for _, mutation := range plan.tableMutations {
		createAt := mutation.createAt
		if mutation.dbName == catalog2.MO_CATALOG && mutation.name == catalog2.MO_SNAPSHOTS {
			sm.snapshotTableIDs[mutation.tid] = struct{}{}
			logutil.Info(
				"UpdateSnapTable-P1",
				zap.Uint64("tid", mutation.tid),
				zap.Uint32("account", mutation.account),
				zap.String("create-at", createAt.ToString()),
			)
		}
		if mutation.dbName == catalog2.MO_CATALOG && mutation.name == catalog2.MO_PITR {
			if sm.pitr.tid > 0 && sm.pitr.tid != mutation.tid {
				logutil.Warn(
					"GC-PANIC-UPDATE-TABLE-P2",
					zap.Uint64("tid", mutation.tid),
					zap.Uint64("old-tid", sm.pitr.tid),
				)
				sm.pitr.reset()
			}
			sm.pitr.tid = mutation.tid
		}
		if mutation.dbName == catalog2.MO_CATALOG && mutation.name == catalog2.MO_ISCP_LOG {
			if sm.iscp.tid > 0 && sm.iscp.tid != mutation.tid {
				logutil.Warn(
					"GC-PANIC-UPDATE-TABLE-P2-ISCP",
					zap.Uint64("tid", mutation.tid),
					zap.Uint64("old-tid", sm.iscp.tid),
				)
				sm.iscp.reset()
			}
			sm.iscp.tid = mutation.tid
		}
		if sm.tables[mutation.account] == nil {
			sm.tables[mutation.account] = make(map[uint64]*tableInfo)
		}
		tInfo := sm.tables[mutation.account][mutation.tid]
		if tInfo != nil {
			if tInfo.createAt.GT(&createAt) {
				logutil.Warn(
					"GC-PANIC-UPDATE-TABLE-P3",
					zap.Uint64("tid", mutation.tid),
					zap.String("name", mutation.pk),
					zap.String("old-create-at", tInfo.createAt.ToString()),
					zap.String("new-create-at", createAt.ToString()),
				)
				tInfo.createAt = createAt
			}
			if tInfo.pk == mutation.pk {
				// MO_TABLES can contain multiple row versions for one table
				// (for example after ALTER). Keep one queue entry per version so
				// the corresponding update tombstones are consumed without
				// mistaking them for a table drop.
				sm.tablePKIndex[mutation.pk] = append(sm.tablePKIndex[mutation.pk], tInfo)
				continue
			}
			createAt = tInfo.createAt
		}
		tInfo = &tableInfo{
			accountID: mutation.account,
			dbID:      mutation.db,
			tid:       mutation.tid,
			createAt:  createAt,
			pk:        mutation.pk,
		}
		sm.tables[mutation.account][mutation.tid] = tInfo
		sm.tableIDIndex[mutation.tid] = tInfo
		sm.tablePKIndex[mutation.pk] = append(sm.tablePKIndex[mutation.pk], tInfo)
	}
	for _, delRow := range plan.deleteRows {
		if _, protected := sm.aobjDelTsMap[delRow.ts]; protected {
			continue
		}
		pk := delRow.pk.ErrString(nil)
		if sm.tablePKIndex[pk] == nil {
			continue
		}
		if len(sm.tablePKIndex[pk]) == 0 {
			logutil.Warn("GC-PANIC-UPDATE-TABLE-P5",
				zap.String("pk", delRow.pk.ErrString(nil)),
				zap.String("rowid", delRow.rowid.String()),
				zap.String("commit", delRow.ts.ToString()),
				zap.String("start", plan.startTS.ToString()),
				zap.String("end", plan.endTS.ToString()))
			continue
		}
		table := sm.tablePKIndex[pk][0]
		if !table.deleteAt.IsEmpty() && table.deleteAt.GT(&delRow.ts) {
			logutil.Warn("GC-PANIC-UPDATE-TABLE-P6",
				zap.Uint64("tid", table.tid),
				zap.String("old-delete-at", table.deleteAt.ToString()),
				zap.String("new-delete-at", delRow.ts.ToString()))
		}
		table.deleteAt = delRow.ts
		sm.tablePKIndex[pk] = sm.tablePKIndex[pk][1:]
		if len(sm.tablePKIndex[pk]) != 0 {
			continue
		}

		if sm.tableIDIndex[table.tid] == nil {
			//In the upgraded cluster, because the inc checkpoint is consumed halfway,
			// there may be no record of the create table entry, only the delete entry
			continue
		}
		if len(sm.tablePKIndex[pk]) == 0 {
			if sm.tableIDIndex[table.tid] != nil && table.pk != sm.tableIDIndex[table.tid].pk {
				continue
			}
		}
		sm.tableIDIndex[table.tid] = table
		if sm.tables[table.accountID] == nil {
			sm.tables[table.accountID] = make(map[uint64]*tableInfo)
		}
		sm.tables[table.accountID][table.tid] = table
	}

	for pkIndex, tInfos := range sm.tablePKIndex {
		if len(tInfos) > 1 {
			logutil.Warn(
				"GC-PANIC-UPDATE-TABLE-P7",
				zap.String("table", pkIndex),
				zap.Int("len", len(tInfos)),
			)
		}
		if len(tInfos) == 0 {
			continue
		}
		tInfos[0].deleteAt = types.TS{}
	}
	return nil
}

func (sm *SnapshotMeta) Update(
	ctx context.Context,
	fs fileservice.FileService,
	data *CKPReader,
	startts, endts types.TS,
	taskName string,
) (err error) {
	if sm == nil || ctx == nil || fs == nil || data == nil {
		return moerr.NewInvalidInputNoCtx(
			"snapshot update requires metadata, context, file service, and checkpoint data",
		)
	}
	if endts.LT(&startts) {
		return moerr.NewInvalidInputNoCtxf(
			"snapshot checkpoint range %s-%s is reversed",
			startts.ToString(), endts.ToString(),
		)
	}
	sm.updateMu.Lock()
	defer sm.updateMu.Unlock()
	start := time.Now()
	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		logger(
			"GC-SnapshotMeta-Update",
			zap.Error(err),
			zap.Duration("cost", time.Since(start)),
			zap.String("start-ts", startts.ToString()),
			zap.String("end-ts", endts.ToString()),
			zap.String("task", taskName),
		)
	}()
	// Read and validate the checkpoint iteration before publishing table state.
	// One pass stages both object kinds and is reused for table metadata and
	// object publication, avoiding a second full checkpoint scan under lock.
	dataMutations, tombstoneMutations, collectErr :=
		collectCheckpointObjectMutations(ctx, data)
	if collectErr != nil {
		err = collectErr
		return
	}

	// Object reads and row decoding can also be substantial. Build the complete
	// fallible update plan before taking the metadata lock; updateMu preserves
	// checkpoint order while readers continue using the previous state.
	plan, planErr := collectTableInfoUpdatePlan(
		ctx, fs, dataMutations, tombstoneMutations, startts, endts,
	)
	if planErr != nil {
		err = planErr
		return
	}

	sm.Lock()
	defer sm.Unlock()
	if err = sm.applyTableInfoUpdatePlan(plan); err != nil {
		return
	}
	if sm.objects == nil {
		sm.objects = make(map[uint64]map[objectio.Segmentid]*objectInfo)
	}
	if sm.tombstones == nil {
		sm.tombstones = make(map[uint64]map[objectio.Segmentid]*objectInfo)
	}

	if len(sm.snapshotTableIDs) == 0 && sm.pitr.tid == 0 && sm.iscp.tid == 0 {
		return
	}

	collector := func(
		objects1 *map[uint64]map[objectio.Segmentid]*objectInfo,
		objects2 *map[objectio.Segmentid]*objectInfo,
		objects3 *map[objectio.Segmentid]*objectInfo,
		tid uint64,
		stats objectio.ObjectStats,
		createTS types.TS, deleteTS types.TS,
	) {
		mapFun := func(
			objects1 map[objectio.Segmentid]*objectInfo,
		) {
			id := stats.ObjectName().SegmentId()
			if objects1[id] == nil {
				objects1[id] = &objectInfo{
					stats:    stats,
					createAt: createTS,
					deleteAt: deleteTS,
				}
				logutil.Info(
					"GC-SnapshotMeta-Update-Collector",
					zap.Uint64("table-id", tid),
					zap.String("object-name", id.String()),
					zap.String("create-at", createTS.ToString()),
					zap.String("task", taskName),
				)

				return
			}
			if objects1[id].deleteAt.IsEmpty() {
				objects1[id].deleteAt = deleteTS
				logutil.Info(
					"GC-SnapshotMeta-Update-Collector",
					zap.Uint64("table-id", tid),
					zap.String("object-name", id.String()),
					zap.String("delete-at", deleteTS.ToString()),
				)
			}
		}
		if tid == sm.pitr.tid {
			if *objects2 == nil {
				*objects2 = make(map[objectio.Segmentid]*objectInfo)
			}
			mapFun(*objects2)
		}
		if tid == sm.iscp.tid {
			if *objects3 == nil {
				*objects3 = make(map[objectio.Segmentid]*objectInfo)
			}
			mapFun(*objects3)
		}
		if _, ok := sm.snapshotTableIDs[tid]; !ok {
			return
		}
		if (*objects1)[tid] == nil {
			(*objects1)[tid] = make(map[objectio.Segmentid]*objectInfo)
		}
		mapFun((*objects1)[tid])
	}
	for _, mutation := range dataMutations {
		collector(
			&sm.objects, &sm.pitr.objects, &sm.iscp.objects,
			mutation.tid, mutation.stats, mutation.createTS, mutation.deleteTS,
		)
	}
	for _, mutation := range tombstoneMutations {
		collector(
			&sm.tombstones, &sm.pitr.tombstones, &sm.iscp.tombstones,
			mutation.tid, mutation.stats, mutation.createTS, mutation.deleteTS,
		)
	}

	trimList := func(
		objects map[uint64]map[objectio.Segmentid]*objectInfo,
		objects2 map[objectio.Segmentid]*objectInfo) {
		for _, objs := range objects {
			for id, info := range objs {
				if info == nil || !info.deleteAt.IsEmpty() {
					delete(objs, id)
				}
			}
		}
		for id, info := range objects2 {
			if info == nil || !info.deleteAt.IsEmpty() {
				delete(objects2, id)
			}
		}
	}

	// Cleaning up common objects and tombstones
	trimList(sm.objects, nil)
	trimList(sm.tombstones, nil)

	// Clean up special table objects and tombstones
	sm.pitr.trim()
	sm.iscp.trim()
	return
}

func NewSnapshotDataSource(
	ctx context.Context,
	fs fileservice.FileService,
	ts types.TS,
	stats []objectio.ObjectStats,
) *BackupDeltaLocDataSource {
	ds := make(map[string]*objData)
	return &BackupDeltaLocDataSource{
		ctx:        ctx,
		fs:         fs,
		ts:         ts,
		ds:         ds,
		tombstones: stats,
		needShrink: false,
		owned:      make(map[*batch.Batch]struct{}),
	}
}

func checkedSnapshotAccountID(
	ctx context.Context,
	source string,
	value uint64,
) (uint32, error) {
	if value > math.MaxUint32 {
		return 0, moerr.NewInternalErrorf(
			ctx, "%s account id %d exceeds uint32", source, value,
		)
	}
	return uint32(value), nil
}

func (sm *SnapshotMeta) GetSnapshot(
	ctx context.Context,
	sid string,
	fs fileservice.FileService,
	mp *mpool.MPool,
	extraClusterTS ...types.TS,
) (*SnapshotInfo, error) {
	var err error
	if sm == nil || ctx == nil || fs == nil {
		return nil, moerr.NewInvalidInputNoCtx(
			"snapshot read requires metadata, context, and file service",
		)
	}

	start := time.Now()
	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		logger(
			"GetSnapshot",
			zap.Error(err),
			zap.Duration("cost", time.Since(start)),
		)
	}()

	sm.RLock()
	objects := copyObjectsLocked(sm.objects)
	tombstones := copyObjectsLocked(sm.tombstones)
	sm.RUnlock()
	snapshotInfo := NewSnapshotInfo()
	idxes := []uint16{ColTS, ColLevel, ColObjId}
	// The object list already defines the persisted state being read. Use the
	// maximum timestamp so HLC commits ahead of local wall time are not omitted;
	// BlockDataReadBackup still filters the UncommitTS sentinel explicitly.
	checkpointTS := types.MaxTs()
	for tid, objMap := range objects {
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		default:
		}
		tombstonesStats := make([]objectio.ObjectStats, 0)
		if tombstoneMap, ok := tombstones[tid]; ok {
			for _, object := range tombstoneMap {
				if object == nil {
					return nil, moerr.NewInternalError(ctx, "snapshot tombstone metadata entry is nil")
				}
				if _, validateErr := validateObjectStatsBlockCount(ctx, object.stats); validateErr != nil {
					return nil, validateErr
				}
				tombstonesStats = append(tombstonesStats, object.stats)
			}
		}
		ds := NewSnapshotDataSource(ctx, fs, checkpointTS, tombstonesStats)
		for _, object := range objMap {
			if object == nil {
				ds.Close()
				return nil, moerr.NewInternalError(ctx, "snapshot object metadata entry is nil")
			}
			blockCount, validateErr := validateObjectStatsBlockCount(ctx, object.stats)
			if validateErr != nil {
				ds.Close()
				return nil, validateErr
			}
			for i := 0; i < blockCount; i++ {
				blk := object.stats.ConstructBlockInfo(uint16(i))
				bat, _, readErr := blockio.BlockDataReadBackup(
					ctx, &blk, ds, idxes, types.TS{}, fs,
				)
				if readErr != nil {
					ds.Close()
					err = readErr
					return nil, err
				}
				processErr := func() error {
					if bat == nil {
						return moerr.NewInternalError(ctx, "snapshot block has an invalid column set")
					}
					defer bat.Clean(common.DebugAllocator)
					if len(bat.Vecs) != len(idxes) {
						return moerr.NewInternalError(ctx, "snapshot block has an invalid column set")
					}
					rowCount := bat.RowCount()
					expectedTypes := [...]types.T{types.T_int64, types.T_enum, types.T_uint64}
					for pos, vec := range bat.Vecs {
						if vec == nil || vec.GetType().Oid != expectedTypes[pos] ||
							vec.Length() != rowCount || vec.GetNulls().Any() {
							return moerr.NewInternalErrorf(
								ctx, "snapshot block column %d is malformed", pos,
							)
						}
					}
					for r := 0; r < rowCount; r++ {
						ts := vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], r)
						snapTs := types.BuildTS(ts, 0)
						objId := vector.GetFixedAtNoTypeCheck[uint64](bat.Vecs[2], r)
						snapshotType := vector.GetFixedAtNoTypeCheck[types.Enum](bat.Vecs[1], r)

						if snapshotType == SnapshotTypeCluster {
							// Cluster snapshot
							snapshotInfo.cluster = append(snapshotInfo.cluster, snapTs)
							logutil.Debug(
								"GetSnapshot-P1",
								zap.String("ts", snapTs.ToString()),
							)
							continue
						}

						// Account snapshot
						if snapshotType == SnapshotTypeAccount {
							id, convertErr := checkedSnapshotAccountID(ctx, "snapshot", objId)
							if convertErr != nil {
								return convertErr
							}
							if snapshotInfo.account[id] == nil {
								snapshotInfo.account[id] = make([]types.TS, 0)
							}
							snapshotInfo.account[id] = append(snapshotInfo.account[id], snapTs)
							// TODO: info to debug
							logutil.Debug(
								"GetSnapshot-P2",
								zap.String("ts", snapTs.ToString()),
								zap.Uint32("account", id),
							)
							continue
						}

						// Database snapshot
						if snapshotType == SnapshotTypeDatabase {
							id := objId
							if snapshotInfo.database[id] == nil {
								snapshotInfo.database[id] = make([]types.TS, 0)
							}
							snapshotInfo.database[id] = append(snapshotInfo.database[id], snapTs)
							logutil.Debug(
								"GetSnapshot-P3-Database",
								zap.String("ts", snapTs.ToString()),
								zap.Uint64("database", id),
							)
							continue
						}

						// Table snapshot
						if snapshotType == SnapshotTypeTable {
							id := objId
							if snapshotInfo.tables[id] == nil {
								snapshotInfo.tables[id] = make([]types.TS, 0)
							}
							snapshotInfo.tables[id] = append(snapshotInfo.tables[id], snapTs)
							logutil.Debug(
								"GetSnapshot-P4-Table",
								zap.String("ts", snapTs.ToString()),
								zap.Uint64("table", id),
							)
							continue
						}
						return moerr.NewInternalErrorf(
							ctx, "snapshot block has unknown level %d", snapshotType,
						)
					}
					return nil
				}()
				if processErr != nil {
					ds.Close()
					err = processErr
					return nil, err
				}
			}
		}
		ds.Close()
	}

	// Add extra cluster-level snapshot timestamps (e.g., for backup protection)
	// Add them before sorting so we only need to sort once
	for _, extraTS := range extraClusterTS {
		if !extraTS.IsEmpty() {
			snapshotInfo.cluster = append(snapshotInfo.cluster, extraTS)
			logutil.Info(
				"GetSnapshot-Add-Extra-Cluster-Snapshot",
				zap.String("ts", extraTS.ToString()),
			)
		}
	}

	// Sort cluster snapshots
	slices.SortFunc(snapshotInfo.cluster, func(a, b types.TS) int {
		return a.Compare(&b)
	})
	logutil.Info(
		"GetSnapshot-P3-Cluster",
		zap.Int("snapshot count", len(snapshotInfo.cluster)),
	)

	// Sort account snapshots
	for accountID, tsList := range snapshotInfo.account {
		slices.SortFunc(tsList, func(a, b types.TS) int {
			return a.Compare(&b)
		})
		snapshotInfo.account[accountID] = tsList
		logutil.Info(
			"GetSnapshot-P3-Account",
			zap.Uint32("account", accountID),
			zap.Int("snapshot count", len(tsList)),
		)
	}

	// Sort database snapshots
	for dbID, tsList := range snapshotInfo.database {
		slices.SortFunc(tsList, func(a, b types.TS) int {
			return a.Compare(&b)
		})
		snapshotInfo.database[dbID] = tsList
		logutil.Info(
			"GetSnapshot-P3-Database",
			zap.Uint64("database", dbID),
			zap.Int("snapshot count", len(tsList)),
		)
	}

	// Sort table snapshots
	for tableID, tsList := range snapshotInfo.tables {
		slices.SortFunc(tsList, func(a, b types.TS) int {
			return a.Compare(&b)
		})
		snapshotInfo.tables[tableID] = tsList
		logutil.Info(
			"GetSnapshot-P3-Table",
			zap.Uint64("table", tableID),
			zap.Int("snapshot count", len(tsList)),
		)
	}

	return snapshotInfo, nil
}

func AddDate(t time.Time, year, month, day int) time.Time {
	targetDate := t.AddDate(year, month, -t.Day()+1)
	targetDay := targetDate.AddDate(0, 1, -1).Day()
	if targetDay > t.Day() {
		targetDay = t.Day()
	}
	targetDate = targetDate.AddDate(0, 0, targetDay-1+day)
	return targetDate
}

func parseSnapshotTS(value string) (types.TS, error) {
	physicalText, logicalText, ok := strings.Cut(value, "-")
	if !ok || physicalText == "" || logicalText == "" || strings.Contains(logicalText, "-") {
		return types.TS{}, moerr.NewInvalidInputNoCtxf("invalid snapshot timestamp %q", value)
	}
	physical, err := strconv.ParseInt(physicalText, 10, 64)
	if err != nil || physical < 0 {
		return types.TS{}, moerr.NewInvalidInputNoCtxf("invalid snapshot timestamp %q", value)
	}
	logical, err := strconv.ParseUint(logicalText, 10, 32)
	if err != nil {
		return types.TS{}, moerr.NewInvalidInputNoCtxf("invalid snapshot timestamp %q", value)
	}
	return types.BuildTS(physical, uint32(logical)), nil
}

func retainMinimumISCPWatermark(
	tables map[uint64]types.TS,
	tableID uint64,
	watermark types.TS,
) {
	existing, exists := tables[tableID]
	if !exists || watermark.LT(&existing) {
		tables[tableID] = watermark
	}
}

func (sm *SnapshotMeta) GetPITR(
	ctx context.Context,
	sid string,
	gcTime time.Time,
	fs fileservice.FileService,
	mp *mpool.MPool,
) (*PitrInfo, error) {
	if sm == nil || ctx == nil || fs == nil || mp == nil {
		return nil, moerr.NewInvalidInputNoCtx(
			"PITR read requires metadata, context, file service, and mpool",
		)
	}
	idxes := []uint16{ColPitrLevel, ColPitrObjId, ColPitrLength, ColPitrUnit}

	sm.RLock()
	pitrClone := sm.pitr.clone()
	sm.RUnlock()

	checkpointTS := types.MaxTs()
	tombstonesStats, err := pitrClone.getTombstonesStats()
	if err != nil {
		return nil, err
	}
	ds := NewSnapshotDataSource(ctx, fs, checkpointTS, tombstonesStats)
	defer ds.Close()
	pitrInfo := &PitrInfo{
		cluster:  make([]types.TS, 1),
		account:  make(map[uint32][]types.TS),
		database: make(map[uint64][]types.TS),
		tables:   make(map[uint64][]types.TS),
	}

	processor := func(bat *batch.Batch, r int) error {
		objIDList := vector.MustFixedColWithTypeCheck[uint64](bat.Vecs[1])
		lengList := vector.MustFixedColWithTypeCheck[uint8](bat.Vecs[2])

		length := lengList[r]
		val := int(length)
		unit := bat.Vecs[3].GetStringAt(r)
		var ts time.Time
		if unit == PitrUnitYear {
			ts = AddDate(gcTime, -val, 0, 0)
		} else if unit == PitrUnitMonth {
			ts = AddDate(gcTime, 0, -val, 0)
		} else if unit == PitrUnitDay {
			ts = gcTime.AddDate(0, 0, -val)
		} else if unit == PitrUnitHour {
			ts = gcTime.Add(-time.Duration(val) * time.Hour)
		} else if unit == PitrUnitMinute {
			ts = gcTime.Add(-time.Duration(val) * time.Minute)
		} else {
			return moerr.NewInternalErrorf(ctx, "PITR row has unknown unit %q", unit)
		}

		pitrTS := types.BuildTS(ts.UnixNano(), 0)
		account := objIDList[r]
		level := bat.Vecs[0].GetStringAt(r)
		if level == PitrLevelCluster {
			if !pitrInfo.cluster[0].IsEmpty() {
				logutil.Warn("GC-PANIC-DUP-PIRT-P1",
					zap.String("level", "cluster"),
					zap.String("old", pitrInfo.cluster[0].ToString()),
					zap.String("new", pitrTS.ToString()),
				)
				if pitrInfo.cluster[0].LT(&pitrTS) {
					return nil
				}
			}
			pitrInfo.cluster[0] = pitrTS

		} else if level == PitrLevelAccount {
			id, convertErr := checkedSnapshotAccountID(ctx, "PITR", account)
			if convertErr != nil {
				return convertErr
			}
			if len(pitrInfo.account[id]) == 0 {
				pitrInfo.account[id] = make([]types.TS, 1)
			}
			p := pitrInfo.account[id][0]
			if !p.IsEmpty() && p.LT(&pitrTS) {
				return nil
			}
			pitrInfo.account[id][0] = pitrTS
		} else if level == PitrLevelDatabase {
			id := uint64(account)
			if len(pitrInfo.database[id]) > 0 {
				p := pitrInfo.database[id][0]
				logutil.Warn("GC-PANIC-DUP-PIRT-P2",
					zap.String("level", "database"),
					zap.Uint64("id", id),
					zap.String("old", p.ToString()),
					zap.String("new", pitrTS.ToString()),
				)
				if !p.IsEmpty() && p.LT(&pitrTS) {
					return nil
				}
			} else {
				pitrInfo.database[id] = make([]types.TS, 1)
			}
			pitrInfo.database[id][0] = pitrTS
		} else if level == PitrLevelTable {
			id := uint64(account)
			if len(pitrInfo.tables[id]) > 0 {
				p := pitrInfo.tables[id][0]
				logutil.Warn("GC-PANIC-DUP-PIRT-P3",
					zap.String("level", "table"),
					zap.Uint64("id", id),
					zap.String("old", p.ToString()),
					zap.String("new", pitrTS.ToString()),
				)
				if !p.IsEmpty() && p.LT(&pitrTS) {
					return nil
				}
			} else {
				pitrInfo.tables[id] = make([]types.TS, 1)
			}
			pitrInfo.tables[id][0] = pitrTS
		} else {
			return moerr.NewInternalErrorf(ctx, "PITR row has unknown level %q", level)
		}
		logutil.Info(
			"GC-GetPITR",
			zap.String("level", level),
			zap.Uint64("id", account),
			zap.String("ts", pitrTS.ToString()),
		)
		return nil
	}

	validator := func(bat *batch.Batch) error {
		if !bat.Vecs[0].GetType().IsVarlen() ||
			bat.Vecs[1].GetType().Oid != types.T_uint64 ||
			bat.Vecs[2].GetType().Oid != types.T_uint8 ||
			!bat.Vecs[3].GetType().IsVarlen() {
			return moerr.NewInternalError(ctx, "PITR block has invalid column types")
		}
		for pos, vec := range bat.Vecs {
			if vec.GetNulls().Any() {
				return moerr.NewInternalErrorf(ctx, "PITR block column %d contains null rows", pos)
			}
		}
		return nil
	}
	err = pitrClone.processObjects(ctx, fs, idxes, ds, mp, validator, processor)
	if err != nil {
		return nil, err
	}
	return pitrInfo, nil
}

func (sm *SnapshotMeta) GetISCP(
	ctx context.Context,
	sid string,
	fs fileservice.FileService,
	mp *mpool.MPool,
) (map[uint64]types.TS, error) {
	if sm == nil || ctx == nil || fs == nil || mp == nil {
		return nil, moerr.NewInvalidInputNoCtx(
			"ISCP read requires metadata, context, file service, and mpool",
		)
	}
	idxes := []uint16{ColIscpTableId, ColIscpWatermark, ColIscpDropAt}

	sm.RLock()
	iscpClone := sm.iscp.clone()
	sm.RUnlock()

	checkpointTS := types.MaxTs()
	tombstonesStats, err := iscpClone.getTombstonesStats()
	if err != nil {
		return nil, err
	}
	ds := NewSnapshotDataSource(ctx, fs, checkpointTS, tombstonesStats)
	defer ds.Close()
	tables := make(map[uint64]types.TS)

	processor := func(bat *batch.Batch, r int) error {
		tableIDList := vector.MustFixedColWithTypeCheck[uint64](bat.Vecs[0])
		watermarkList := bat.Vecs[1]
		dropAtList := bat.Vecs[2]

		tableID := tableIDList[r]
		watermark := watermarkList.GetBytesAt(r)
		if !dropAtList.IsNull(uint64(r)) {
			return nil
		}

		var iscpTS types.TS
		if len(watermark) > 0 {
			var parseErr error
			iscpTS, parseErr = parseSnapshotTS(string(watermark))
			if parseErr != nil {
				return parseErr
			}
		} else {
			iscpTS = types.TS{}
		}

		// For the same tableID, take the smallest TS. An empty watermark is a
		// real lower bound that protects the full history, not an absent map
		// entry, so it must not be overwritten by a later non-empty watermark.
		retainMinimumISCPWatermark(tables, tableID, iscpTS)

		logutil.Info(
			"GC-GetISCP",
			zap.Uint64("table", tableID),
			zap.String("watermark", iscpTS.ToString()),
		)
		return nil
	}

	validator := func(bat *batch.Batch) error {
		if bat.Vecs[0].GetType().Oid != types.T_uint64 ||
			!bat.Vecs[1].GetType().IsVarlen() ||
			bat.Vecs[2].GetType().Oid != types.T_timestamp {
			return moerr.NewInternalError(ctx, "ISCP block has invalid column types")
		}
		if bat.Vecs[0].GetNulls().Any() || bat.Vecs[1].GetNulls().Any() {
			return moerr.NewInternalError(ctx, "ISCP block has null key columns")
		}
		return nil
	}
	err = iscpClone.processObjects(ctx, fs, idxes, ds, mp, validator, processor)
	if err != nil {
		return nil, err
	}
	return tables, nil
}

func (sm *SnapshotMeta) SetTid(tid uint64) {
	if sm == nil {
		return
	}
	sm.Lock()
	defer sm.Unlock()
	if sm.snapshotTableIDs == nil {
		sm.snapshotTableIDs = make(map[uint64]struct{})
	}
	sm.snapshotTableIDs[tid] = struct{}{}
}

func (sm *SnapshotMeta) SaveMeta(name string, fs fileservice.FileService) (uint32, error) {
	if sm == nil || name == "" || fs == nil {
		return 0, moerr.NewInvalidInputNoCtx(
			"snapshot metadata save requires destination, name, and file service",
		)
	}
	sm.RLock()
	objects := copyObjectsLocked(sm.objects)
	tombstones := copyObjectsLocked(sm.tombstones)
	pitr := sm.pitr.clone()
	iscp := sm.iscp.clone()
	sm.RUnlock()
	if len(objects) == 0 && len(tombstones) == 0 &&
		len(pitr.objects) == 0 && len(pitr.tombstones) == 0 &&
		len(iscp.objects) == 0 && len(iscp.tombstones) == 0 {
		return 0, nil
	}
	bat := containers.NewBatch()
	deltaBat := containers.NewBatch()
	defer bat.Close()
	defer deltaBat.Close()
	for i, attr := range objectInfoSchemaAttr {
		bat.AddVector(attr, containers.MakeVector(objectInfoSchemaTypes[i], common.DebugAllocator))
		deltaBat.AddVector(attr, containers.MakeVector(objectInfoSchemaTypes[i], common.DebugAllocator))
	}
	appendBatForMap := func(
		bat *containers.Batch,
		tid uint64,
		objectMap map[objectio.Segmentid]*objectInfo,
	) error {
		for _, entry := range objectMap {
			if entry == nil {
				return moerr.NewInternalErrorNoCtx("snapshot object metadata entry is nil")
			}
			if err := vector.AppendBytes(
				bat.GetVectorByName(catalog.ObjectAttr_ObjectStats).GetDownstreamVector(),
				entry.stats[:], false, common.DebugAllocator,
			); err != nil {
				return err
			}
			if err := vector.AppendFixed[types.TS](
				bat.GetVectorByName(catalog.EntryNode_CreateAt).GetDownstreamVector(),
				entry.createAt, false, common.DebugAllocator,
			); err != nil {
				return err
			}
			if err := vector.AppendFixed[types.TS](
				bat.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector(),
				entry.deleteAt, false, common.DebugAllocator,
			); err != nil {
				return err
			}
			if err := vector.AppendBytes(
				bat.GetVectorByName(catalog2.BlockMeta_DeltaLoc).GetDownstreamVector(),
				nil, false, common.DebugAllocator,
			); err != nil {
				return err
			}
			if err := vector.AppendFixed[uint64](
				bat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector(),
				tid, false, common.DebugAllocator,
			); err != nil {
				return err
			}
		}
		return nil
	}
	appendBat := func(
		bat *containers.Batch,
		objects map[uint64]map[objectio.Segmentid]*objectInfo,
	) error {
		for tid, objectMap := range objects {
			if err := appendBatForMap(bat, tid, objectMap); err != nil {
				return err
			}
		}
		return nil
	}
	appendSpecialObjects := func(
		bat *containers.Batch,
		pitrTid uint64, pitrObjects map[objectio.Segmentid]*objectInfo,
		iscpTid uint64, iscpObjects map[objectio.Segmentid]*objectInfo,
	) error {
		if err := appendBatForMap(bat, pitrTid, pitrObjects); err != nil {
			return err
		}
		return appendBatForMap(bat, iscpTid, iscpObjects)
	}
	if err := appendBat(bat, objects); err != nil {
		return 0, err
	}
	if err := appendSpecialObjects(
		bat, pitr.tid, pitr.objects, iscp.tid, iscp.objects,
	); err != nil {
		return 0, err
	}
	if err := appendBat(deltaBat, tombstones); err != nil {
		return 0, err
	}
	if err := appendSpecialObjects(
		deltaBat, pitr.tid, pitr.tombstones, iscp.tid, iscp.tombstones,
	); err != nil {
		return 0, err
	}
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterGC, name, fs)
	if err != nil {
		return 0, err
	}
	if _, err = writer.WriteWithoutSeqnum(containers.ToCNBatch(bat)); err != nil {
		return 0, err
	}
	if deltaBat.Length() > 0 {
		if _, err = writer.WriteWithoutSeqnum(containers.ToCNBatch(deltaBat)); err != nil {
			return 0, err
		}
	}

	_, err = writer.WriteEnd(context.Background())
	if err != nil {
		return 0, err
	}
	stats := writer.GetObjectStats()
	size := stats.OriginSize()
	return size, err
}

func (sm *SnapshotMeta) SaveTableInfo(name string, fs fileservice.FileService) (uint32, error) {
	if sm == nil || name == "" || fs == nil {
		return 0, moerr.NewInvalidInputNoCtx(
			"snapshot table metadata save requires destination, name, and file service",
		)
	}
	type tableSaveEntry struct {
		table                *tableInfo
		snapshot, pitr, iscp bool
	}
	sm.RLock()
	if len(sm.tables) == 0 && len(sm.aobjDelTsMap) == 0 {
		sm.RUnlock()
		return 0, nil
	}
	tableEntries := make([]tableSaveEntry, 0, len(sm.tableIDIndex))
	for _, accountTables := range sm.tables {
		for _, table := range accountTables {
			entry := tableSaveEntry{table: table}
			if table != nil {
				tableCopy := *table
				entry.table = &tableCopy
				_, entry.snapshot = sm.snapshotTableIDs[table.tid]
				entry.pitr = table.tid == sm.pitr.tid
				entry.iscp = table.tid == sm.iscp.tid
			}
			tableEntries = append(tableEntries, entry)
		}
	}
	aObjectDeleteTS := make([]types.TS, 0, len(sm.aobjDelTsMap))
	for ts := range sm.aobjDelTsMap {
		aObjectDeleteTS = append(aObjectDeleteTS, ts)
	}
	sm.RUnlock()
	bat := containers.NewBatch()
	snapTableBat := containers.NewBatch()
	pitrTableBat := containers.NewBatch()
	iscpTableBat := containers.NewBatch()
	defer bat.Close()
	defer snapTableBat.Close()
	defer pitrTableBat.Close()
	defer iscpTableBat.Close()
	for i, attr := range tableInfoSchemaAttr {
		bat.AddVector(attr, containers.MakeVector(tableInfoSchemaTypes[i], common.DebugAllocator))
		snapTableBat.AddVector(attr, containers.MakeVector(tableInfoSchemaTypes[i], common.DebugAllocator))
		pitrTableBat.AddVector(attr, containers.MakeVector(tableInfoSchemaTypes[i], common.DebugAllocator))
		iscpTableBat.AddVector(attr, containers.MakeVector(tableInfoSchemaTypes[i], common.DebugAllocator))
	}
	appendBat := func(bat *containers.Batch, table *tableInfo) error {
		if table == nil {
			return moerr.NewInternalErrorNoCtx("snapshot table metadata entry is nil")
		}
		if err := vector.AppendFixed[uint32](
			bat.GetVectorByName(catalog2.SystemColAttr_AccID).GetDownstreamVector(),
			table.accountID, false, common.DebugAllocator,
		); err != nil {
			return err
		}
		if err := vector.AppendFixed[uint64](
			bat.GetVectorByName(catalog2.SystemRelAttr_DBID).GetDownstreamVector(),
			table.dbID, false, common.DebugAllocator,
		); err != nil {
			return err
		}
		if err := vector.AppendFixed[uint64](
			bat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector(),
			table.tid, false, common.DebugAllocator,
		); err != nil {
			return err
		}
		if err := vector.AppendFixed[types.TS](
			bat.GetVectorByName(catalog2.SystemRelAttr_CreateAt).GetDownstreamVector(),
			table.createAt, false, common.DebugAllocator,
		); err != nil {
			return err
		}
		if err := vector.AppendFixed[types.TS](
			bat.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector(),
			table.deleteAt, false, common.DebugAllocator,
		); err != nil {
			return err
		}
		return vector.AppendBytes(
			bat.GetVectorByName(MoTablesPK).GetDownstreamVector(),
			[]byte(table.pk), false, common.DebugAllocator,
		)
	}
	for _, entry := range tableEntries {
		if err := appendBat(bat, entry.table); err != nil {
			return 0, err
		}
		if entry.pitr {
			if err := appendBat(pitrTableBat, entry.table); err != nil {
				return 0, err
			}
			continue
		}
		if entry.iscp {
			if err := appendBat(iscpTableBat, entry.table); err != nil {
				return 0, err
			}
			continue
		}
		if entry.snapshot {
			if err := appendBat(snapTableBat, entry.table); err != nil {
				return 0, err
			}
		}
	}

	aObjDelTsBat := containers.NewBatch()
	defer aObjDelTsBat.Close()
	for i, attr := range aObjectDelSchemaAttr {
		aObjDelTsBat.AddVector(attr, containers.MakeVector(aObjectDelSchemaTypes[i], common.DebugAllocator))
	}

	for _, ts := range aObjectDeleteTS {
		if err := vector.AppendFixed[types.TS](
			aObjDelTsBat.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector(),
			ts, false, common.DebugAllocator,
		); err != nil {
			return 0, err
		}
	}

	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterGC, name, fs)
	if err != nil {
		return 0, err
	}
	if _, err = writer.WriteWithoutSeqnum(containers.ToCNBatch(bat)); err != nil {
		return 0, err
	}
	if _, err = writer.WriteWithoutSeqnum(containers.ToCNBatch(snapTableBat)); err != nil {
		return 0, err
	}

	if _, err = writer.WriteWithoutSeqnum(containers.ToCNBatch(aObjDelTsBat)); err != nil {
		return 0, err
	}

	if _, err = writer.WriteWithoutSeqnum(containers.ToCNBatch(pitrTableBat)); err != nil {
		return 0, err
	}

	if _, err = writer.WriteWithoutSeqnum(containers.ToCNBatch(iscpTableBat)); err != nil {
		return 0, err
	}

	_, err = writer.WriteEnd(context.Background())
	if err != nil {
		return 0, err
	}
	stats := writer.GetObjectStats()
	size := stats.OriginSize()
	return size, err
}

// General special table rebuild functions
func (sm *SnapshotMeta) rebuildSpecialTable(ins *containers.Batch, tableInfo *specialTableInfo, tableName string) {
	sm.Lock()
	defer sm.Unlock()
	insTIDs := vector.MustFixedColWithTypeCheck[uint64](
		ins.GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector())
	if ins.Length() < 1 {
		logutil.Warnf("Rebuild%s unexpected length %d", tableName, ins.Length())
		return
	}
	logutil.Infof("Rebuild %s tid %d", tableName, insTIDs[0])
	for i := 0; i < ins.Length(); i++ {
		tid := insTIDs[i]
		tableInfo.tid = tid
	}
}

func (sm *SnapshotMeta) RebuildTableInfo(ins *containers.Batch) {
	sm.Lock()
	defer sm.Unlock()
	if sm.tables == nil {
		sm.tables = make(map[uint32]map[uint64]*tableInfo)
	}
	if sm.tableIDIndex == nil {
		sm.tableIDIndex = make(map[uint64]*tableInfo)
	}
	if sm.tablePKIndex == nil {
		sm.tablePKIndex = make(map[string][]*tableInfo)
	}
	insTIDs := vector.MustFixedColWithTypeCheck[uint64](
		ins.GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector())
	insAccIDs := vector.MustFixedColWithTypeCheck[uint32](
		ins.GetVectorByName(catalog2.SystemColAttr_AccID).GetDownstreamVector())
	insDBIDs := vector.MustFixedColWithTypeCheck[uint64](
		ins.GetVectorByName(catalog2.SystemRelAttr_DBID).GetDownstreamVector())
	insCreateTSs := vector.MustFixedColWithTypeCheck[types.TS](
		ins.GetVectorByName(catalog2.SystemRelAttr_CreateAt).GetDownstreamVector())
	insDeleteTSs := vector.MustFixedColWithTypeCheck[types.TS](
		ins.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector())
	for i := 0; i < ins.Length(); i++ {
		tid := insTIDs[i]
		dbid := insDBIDs[i]
		accid := insAccIDs[i]
		createTS := insCreateTSs[i]
		deleteTS := insDeleteTSs[i]
		pk := string(ins.GetVectorByName(MoTablesPK).GetDownstreamVector().GetRawBytesAt(i))
		if sm.tables[accid] == nil {
			sm.tables[accid] = make(map[uint64]*tableInfo)
		}
		table := &tableInfo{
			tid:       tid,
			dbID:      dbid,
			accountID: accid,
			createAt:  createTS,
			deleteAt:  deleteTS,
			pk:        pk,
		}
		sm.tables[accid][tid] = table
		sm.tableIDIndex[tid] = table
		if !table.deleteAt.IsEmpty() {
			continue
		}
		if len(sm.tablePKIndex[pk]) > 0 {
			logutil.Warn(
				"GC-PANIC-REBUILD-TABLE",
				zap.String("pk", pk),
				zap.Uint64("table", tid))
		}
		sm.tablePKIndex[pk] = make([]*tableInfo, 1)
		sm.tablePKIndex[pk][0] = table
	}
}

func (sm *SnapshotMeta) RebuildTid(ins *containers.Batch) {
	sm.Lock()
	defer sm.Unlock()
	if sm.snapshotTableIDs == nil {
		sm.snapshotTableIDs = make(map[uint64]struct{})
	}
	insTIDs := vector.MustFixedColWithTypeCheck[uint64](
		ins.GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector())
	accIDs := vector.MustFixedColWithTypeCheck[uint32](
		ins.GetVectorByName(catalog2.SystemColAttr_AccID).GetDownstreamVector())
	if ins.Length() < 1 {
		logutil.Warnf("RebuildTid unexpected length %d", ins.Length())
		return
	}
	logutil.Infof("RebuildTid tid %d", insTIDs[0])
	for i := 0; i < ins.Length(); i++ {
		tid := insTIDs[i]
		accid := accIDs[i]
		if _, ok := sm.snapshotTableIDs[tid]; !ok {
			sm.snapshotTableIDs[tid] = struct{}{}
			logutil.Info("[RebuildSnapshotTid]", zap.Uint64("tid", tid), zap.Uint32("account id", accid))
		}
	}
}

func (sm *SnapshotMeta) RebuildPitr(ins *containers.Batch) {
	sm.rebuildSpecialTable(ins, &sm.pitr, "Pitr")
}

func (sm *SnapshotMeta) RebuildIscp(ins *containers.Batch) {
	sm.rebuildSpecialTable(ins, &sm.iscp, "Iscp")
}

func (sm *SnapshotMeta) RebuildAObjectDel(ins *containers.Batch) {
	sm.Lock()
	defer sm.Unlock()
	if ins.Length() < 1 {
		logutil.Warnf("RebuildAObjectDel unexpected length %d", ins.Length())
		return
	}
	sm.aobjDelTsMap = make(map[types.TS]struct{})
	commitTsVec := vector.MustFixedColWithTypeCheck[types.TS](ins.GetVectorByName(EntryNode_DeleteAt).GetDownstreamVector())
	for i := 0; i < ins.Length(); i++ {
		commitTs := commitTsVec[i]
		if _, ok := sm.aobjDelTsMap[commitTs]; ok {
			logutil.Warn("RebuildAObjectDel-Exists", zap.Any("commitTs", commitTs))
		}
		sm.aobjDelTsMap[commitTs] = struct{}{}
	}
}

type snapshotObjectMutation struct {
	stats              objectio.ObjectStats
	createTS, deleteTS types.TS
	tid                uint64
}

func parseSnapshotObjectMutations(ins *containers.Batch) ([]snapshotObjectMutation, error) {
	if ins == nil {
		return nil, moerr.NewInvalidInputNoCtx("snapshot metadata rebuild requires a batch")
	}
	getVector := func(name string) (containers.Vector, error) {
		pos, ok := ins.Nameidx[name]
		if !ok || pos < 0 || pos >= len(ins.Vecs) || ins.Vecs[pos] == nil {
			return nil, moerr.NewInternalErrorNoCtxf("snapshot metadata is missing column %q", name)
		}
		return ins.Vecs[pos], nil
	}
	statsVec, err := getVector(catalog.ObjectAttr_ObjectStats)
	if err != nil {
		return nil, err
	}
	createVec, err := getVector(catalog.EntryNode_CreateAt)
	if err != nil {
		return nil, err
	}
	deleteVec, err := getVector(catalog.EntryNode_DeleteAt)
	if err != nil {
		return nil, err
	}
	tidVec, err := getVector(SnapshotAttr_TID)
	if err != nil {
		return nil, err
	}
	if !statsVec.GetType().IsVarlen() || createVec.GetType().Oid != types.T_TS ||
		deleteVec.GetType().Oid != types.T_TS || tidVec.GetType().Oid != types.T_uint64 {
		return nil, moerr.NewInternalErrorNoCtx("snapshot metadata has invalid column types")
	}
	rowCount := statsVec.Length()
	for _, vec := range []containers.Vector{statsVec, createVec, deleteVec, tidVec} {
		if vec.Length() != rowCount || vec.NullCount() != 0 {
			return nil, moerr.NewInternalErrorNoCtx("snapshot metadata has malformed or null columns")
		}
	}
	insCreateTSs := vector.MustFixedColWithTypeCheck[types.TS](createVec.GetDownstreamVector())
	insDeleteTSs := vector.MustFixedColWithTypeCheck[types.TS](deleteVec.GetDownstreamVector())
	insTides := vector.MustFixedColWithTypeCheck[uint64](tidVec.GetDownstreamVector())
	mutations := make([]snapshotObjectMutation, 0, rowCount)
	for i := 0; i < rowCount; i++ {
		var objectStats objectio.ObjectStats
		buf := statsVec.GetDownstreamVector().GetRawBytesAt(i)
		if len(buf) != len(objectStats) {
			return nil, moerr.NewInternalErrorNoCtxf(
				"snapshot object statistics have length %d, expected %d", len(buf), len(objectStats),
			)
		}
		objectStats.UnMarshal(buf)
		mutations = append(mutations, snapshotObjectMutation{
			stats: objectStats, createTS: insCreateTSs[i], deleteTS: insDeleteTSs[i], tid: insTides[i],
		})
	}
	return mutations, nil
}

func (sm *SnapshotMeta) rebuildObjectMutationsLocked(
	mutations []snapshotObjectMutation,
	objects *map[uint64]map[objectio.Segmentid]*objectInfo,
	objects2 *map[objectio.Segmentid]*objectInfo,
	objects3 *map[objectio.Segmentid]*objectInfo,
	snapshotTableIDs map[uint64]struct{},
) {
	if *objects == nil {
		*objects = make(map[uint64]map[objectio.Segmentid]*objectInfo)
	}
	if *objects2 == nil {
		*objects2 = make(map[objectio.Segmentid]*objectInfo)
	}
	if *objects3 == nil {
		*objects3 = make(map[objectio.Segmentid]*objectInfo)
	}
	for _, mutation := range mutations {
		objectStats := mutation.stats
		createTS := mutation.createTS
		deleteTS := mutation.deleteTS
		tid := mutation.tid
		if tid == sm.pitr.tid {
			if (*objects2)[objectStats.ObjectName().SegmentId()] == nil {
				(*objects2)[objectStats.ObjectName().SegmentId()] = &objectInfo{
					stats:    objectStats,
					createAt: createTS,
					deleteAt: deleteTS,
				}
				logutil.Info(
					"GC-Rebuild-P1",
					zap.String("object-name", objectStats.ObjectName().String()),
					zap.String("create-at", createTS.ToString()),
				)
			}
			continue
		}
		if tid == sm.iscp.tid {
			if (*objects3)[objectStats.ObjectName().SegmentId()] == nil {
				(*objects3)[objectStats.ObjectName().SegmentId()] = &objectInfo{
					stats:    objectStats,
					createAt: createTS,
					deleteAt: deleteTS,
				}
				logutil.Info(
					"GC-Rebuild-ISCP-P1",
					zap.String("object-name", objectStats.ObjectName().String()),
					zap.String("create-at", createTS.ToString()),
				)
			}
			continue
		}
		if _, ok := snapshotTableIDs[tid]; !ok {
			snapshotTableIDs[tid] = struct{}{}
			logutil.Info(
				"GC-RebuildT-P2",
				zap.Uint64("tid", tid),
			)
		}
		if (*objects)[tid] == nil {
			(*objects)[tid] = make(map[objectio.Segmentid]*objectInfo)
		}
		if (*objects)[tid][objectStats.ObjectName().SegmentId()] == nil {

			(*objects)[tid][objectStats.ObjectName().SegmentId()] = &objectInfo{
				stats:    objectStats,
				createAt: createTS,
				deleteAt: deleteTS,
			}
			logutil.Info(
				"GC-Rebuild-P3",
				zap.Uint64("table-id", tid),
				zap.String("object-name", objectStats.ObjectName().String()),
				zap.String("create-at", createTS.ToString()),
			)
			continue
		}
	}
	return
}

func (sm *SnapshotMeta) Rebuild(
	ins *containers.Batch,
	objects *map[uint64]map[objectio.Segmentid]*objectInfo,
	objects2 *map[objectio.Segmentid]*objectInfo,
	objects3 *map[objectio.Segmentid]*objectInfo,
) error {
	if sm == nil || objects == nil || objects2 == nil || objects3 == nil {
		return moerr.NewInvalidInputNoCtx("snapshot metadata rebuild requires destinations")
	}
	mutations, err := parseSnapshotObjectMutations(ins)
	if err != nil {
		return err
	}
	sm.Lock()
	defer sm.Unlock()
	if sm.snapshotTableIDs == nil {
		sm.snapshotTableIDs = make(map[uint64]struct{})
	}
	sm.rebuildObjectMutationsLocked(
		mutations, objects, objects2, objects3, sm.snapshotTableIDs,
	)
	return nil
}

func convertSnapshotObjectInfoBatch(source *batch.Batch) (*containers.Batch, error) {
	if source == nil || len(source.Vecs) != len(objectInfoSchemaAttr) {
		return nil, moerr.NewInternalErrorNoCtx("snapshot metadata block has an invalid column set")
	}
	rowCount := 0
	if len(source.Vecs) > 0 && source.Vecs[0] != nil {
		rowCount = source.Vecs[0].Length()
	}
	result := containers.NewBatch()
	success := false
	defer func() {
		if !success {
			result.Close()
		}
	}()
	for i, attr := range objectInfoSchemaAttr {
		pkgVec := source.Vecs[i]
		if pkgVec == nil || pkgVec.GetType().Oid != objectInfoSchemaTypes[i].Oid {
			return nil, moerr.NewInternalErrorNoCtxf(
				"snapshot metadata column %d is malformed", i,
			)
		}
		// Historical files never populated the unused delta-location
		// compatibility column. Accept its zero-length encoding while requiring
		// every semantic column to remain rectangular.
		legacyEmptyDeltaLoc := attr == catalog2.BlockMeta_DeltaLoc && pkgVec.Length() == 0
		if pkgVec.Length() != rowCount && !legacyEmptyDeltaLoc {
			return nil, moerr.NewInternalErrorNoCtxf(
				"snapshot metadata column %d has %d rows, expected %d",
				i, pkgVec.Length(), rowCount,
			)
		}
		var vec containers.Vector
		if pkgVec.Length() == 0 {
			vec = containers.MakeVector(objectInfoSchemaTypes[i], common.DebugAllocator)
		} else {
			vec = containers.ToTNVector(pkgVec, common.DebugAllocator)
		}
		result.AddVector(attr, vec)
	}
	success = true
	return result, nil
}

func convertSnapshotSchemaBatch(
	source *batch.Batch,
	attrs []string,
	schemaTypes []types.Type,
) (*containers.Batch, error) {
	if source == nil || len(attrs) == 0 || len(attrs) != len(schemaTypes) ||
		len(source.Vecs) != len(attrs) {
		return nil, moerr.NewInternalErrorNoCtx("snapshot metadata block has an invalid schema")
	}
	if source.Vecs[0] == nil {
		return nil, moerr.NewInternalErrorNoCtx("snapshot metadata block has no leading column")
	}
	rowCount := source.Vecs[0].Length()
	result := containers.NewBatch()
	success := false
	defer func() {
		if !success {
			result.Close()
		}
	}()
	for i, attr := range attrs {
		pkgVec := source.Vecs[i]
		if pkgVec == nil || pkgVec.Length() != rowCount || pkgVec.GetNulls().Any() ||
			pkgVec.GetType().Oid != schemaTypes[i].Oid {
			return nil, moerr.NewInternalErrorNoCtxf(
				"snapshot metadata column %d is malformed", i,
			)
		}
		var vec containers.Vector
		if rowCount == 0 {
			vec = containers.MakeVector(schemaTypes[i], common.DebugAllocator)
		} else {
			vec = containers.ToTNVector(pkgVec, common.DebugAllocator)
		}
		result.AddVector(attr, vec)
	}
	success = true
	return result, nil
}

func (sm *SnapshotMeta) ReadMeta(
	ctx context.Context,
	name string,
	fs fileservice.FileService,
) (err error) {
	if sm == nil || ctx == nil || fs == nil {
		return moerr.NewInvalidInputNoCtx("snapshot metadata read requires destination, context, and file service")
	}
	sm.updateMu.Lock()
	defer sm.updateMu.Unlock()
	defer func() {
		if recovered := recover(); recovered != nil {
			err = moerr.NewInternalErrorf(
				ctx, "snapshot metadata rebuild failed: %v", recovered,
			)
		}
	}()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	default:
	}

	reader, err := ioutil.NewFileReaderNoCache(fs, name)
	if err != nil {
		return err
	}
	bs, err := reader.LoadAllBlocks(ctx, common.DebugAllocator)
	if err != nil {
		return err
	}
	if len(bs) == 0 || len(bs) > 2 {
		return moerr.NewInternalErrorf(
			ctx, "snapshot metadata file has %d blocks, expected one or two", len(bs),
		)
	}
	blockIndexes := make(map[uint16]int, len(bs))
	for index, block := range bs {
		id := block.GetID()
		if id > 1 {
			return moerr.NewInternalErrorf(
				ctx, "snapshot metadata file has unknown block %d", id,
			)
		}
		if _, duplicate := blockIndexes[id]; duplicate {
			return moerr.NewInternalErrorf(
				ctx, "snapshot metadata file has duplicate block %d", id,
			)
		}
		blockIndexes[id] = index
	}
	dataIndex, ok := blockIndexes[0]
	if !ok {
		return moerr.NewInternalError(ctx, "snapshot metadata file has no object block")
	}
	idxes := make([]uint16, len(objectInfoSchemaAttr))
	for i := range objectInfoSchemaAttr {
		idxes[i] = uint16(i)
	}
	mobat, release, err := reader.LoadColumns(
		ctx, idxes, nil, bs[dataIndex].GetID(), common.DebugAllocator,
	)
	if err != nil {
		return err
	}
	if release != nil {
		defer release()
	}
	bat, err := convertSnapshotObjectInfoBatch(mobat)
	if err != nil {
		return err
	}
	defer bat.Close()
	objectMutations, err := parseSnapshotObjectMutations(bat)
	if err != nil {
		return err
	}

	var tombstoneMutations []snapshotObjectMutation
	tombstoneIndex, hasTombstoneBlock := blockIndexes[1]
	if !hasTombstoneBlock {
		tombstoneMutations = nil
	} else {
		moDeltaBat, releaseDelta, loadErr := reader.LoadColumns(
			ctx, idxes, nil, bs[tombstoneIndex].GetID(), common.DebugAllocator,
		)
		if loadErr != nil {
			return loadErr
		}
		if releaseDelta != nil {
			defer releaseDelta()
		}
		deltaBat, convertErr := convertSnapshotObjectInfoBatch(moDeltaBat)
		if convertErr != nil {
			return convertErr
		}
		defer deltaBat.Close()
		tombstoneMutations, err = parseSnapshotObjectMutations(deltaBat)
		if err != nil {
			return err
		}
	}

	objects := make(map[uint64]map[objectio.Segmentid]*objectInfo)
	tombstones := make(map[uint64]map[objectio.Segmentid]*objectInfo)
	pitrObjects := make(map[objectio.Segmentid]*objectInfo)
	pitrTombstones := make(map[objectio.Segmentid]*objectInfo)
	iscpObjects := make(map[objectio.Segmentid]*objectInfo)
	iscpTombstones := make(map[objectio.Segmentid]*objectInfo)
	pendingSnapshotTableIDs := make(map[uint64]struct{})
	sm.Lock()
	defer sm.Unlock()
	sm.rebuildObjectMutationsLocked(
		objectMutations, &objects, &pitrObjects, &iscpObjects, pendingSnapshotTableIDs,
	)
	sm.rebuildObjectMutationsLocked(
		tombstoneMutations, &tombstones, &pitrTombstones, &iscpTombstones, pendingSnapshotTableIDs,
	)
	if sm.snapshotTableIDs == nil {
		sm.snapshotTableIDs = make(map[uint64]struct{}, len(pendingSnapshotTableIDs))
	}
	for tid := range pendingSnapshotTableIDs {
		sm.snapshotTableIDs[tid] = struct{}{}
	}
	sm.objects = objects
	sm.tombstones = tombstones
	sm.pitr.objects = pitrObjects
	sm.pitr.tombstones = pitrTombstones
	sm.iscp.objects = iscpObjects
	sm.iscp.tombstones = iscpTombstones
	return nil
}

func (sm *SnapshotMeta) ReadTableInfo(
	ctx context.Context,
	name string,
	fs fileservice.FileService,
) (err error) {
	if sm == nil || ctx == nil || fs == nil {
		return moerr.NewInvalidInputNoCtx(
			"snapshot table metadata read requires destination, context, and file service",
		)
	}
	sm.updateMu.Lock()
	defer sm.updateMu.Unlock()
	defer func() {
		if recovered := recover(); recovered != nil {
			err = moerr.NewInternalErrorf(
				ctx, "snapshot table metadata rebuild failed: %v", recovered,
			)
		}
	}()
	reader, err := ioutil.NewFileReaderNoCache(fs, name)
	if err != nil {
		return err
	}
	bs, err := reader.LoadAllBlocks(ctx, common.DebugAllocator)
	if err != nil {
		return err
	}
	if len(bs) == 0 || len(bs) > int(IscpTidIdx)+1 {
		return moerr.NewInternalErrorf(
			ctx, "snapshot table metadata file has unsupported block count %d", len(bs),
		)
	}
	type loadedTableMetadataBlock struct {
		id  int
		bat *containers.Batch
	}
	loaded := make([]loadedTableMetadataBlock, 0, len(bs))
	seenBlocks := make(map[int]struct{}, len(bs))
	for _, block := range bs {
		id := int(block.GetID())
		if id < int(TableInfoTypeIdx) || id > int(IscpTidIdx) {
			return moerr.NewInternalErrorf(ctx, "unknown snapshot table metadata block %d", id)
		}
		if _, duplicate := seenBlocks[id]; duplicate {
			return moerr.NewInternalErrorf(ctx, "duplicate snapshot table metadata block %d", id)
		}
		seenBlocks[id] = struct{}{}
		var attrs []string
		var schemaTypes []types.Type
		if id == int(AObjectDelIdx) {
			attrs = aObjectDelSchemaAttr
			schemaTypes = aObjectDelSchemaTypes
		} else {
			switch id {
			case int(TableInfoTypeIdx), int(SnapshotTidIdx), int(PitrTidIdx), int(IscpTidIdx):
				attrs = tableInfoSchemaAttr
				schemaTypes = tableInfoSchemaTypes
			default:
				return moerr.NewInternalErrorf(ctx, "unknown snapshot table metadata block %d", id)
			}
		}
		idxes := make([]uint16, len(attrs))
		for i := range attrs {
			idxes[i] = uint16(i)
		}
		mobat, release, loadErr := reader.LoadColumns(
			ctx, idxes, nil, block.GetID(), common.DebugAllocator,
		)
		err = loadErr
		if err != nil {
			return err
		}
		if release != nil {
			defer release()
		}
		converted, convertErr := convertSnapshotSchemaBatch(mobat, attrs, schemaTypes)
		if convertErr != nil {
			return convertErr
		}
		defer converted.Close()
		loaded = append(loaded, loadedTableMetadataBlock{id: id, bat: converted})
	}
	if _, ok := seenBlocks[int(TableInfoTypeIdx)]; !ok {
		return moerr.NewInternalError(ctx, "snapshot table metadata has no table-info block")
	}
	// Every block is loaded and validated before any state is published.
	staged := NewSnapshotMeta()
	for _, block := range loaded {
		switch block.id {
		case int(TableInfoTypeIdx):
			staged.RebuildTableInfo(block.bat)
		case int(SnapshotTidIdx):
			staged.RebuildTid(block.bat)
		case int(AObjectDelIdx):
			staged.RebuildAObjectDel(block.bat)
		case int(PitrTidIdx):
			staged.RebuildPitr(block.bat)
		case int(IscpTidIdx):
			staged.RebuildIscp(block.bat)
		}
	}
	sm.Lock()
	sm.tables = staged.tables
	sm.tableIDIndex = staged.tableIDIndex
	sm.tablePKIndex = staged.tablePKIndex
	sm.snapshotTableIDs = staged.snapshotTableIDs
	sm.aobjDelTsMap = staged.aobjDelTsMap
	sm.pitr.tid = staged.pitr.tid
	sm.iscp.tid = staged.iscp.tid
	sm.Unlock()
	return nil
}

func (sm *SnapshotMeta) InitTableInfo(
	ctx context.Context,
	fs fileservice.FileService,
	data *CKPReader,
	startts, endts types.TS,
) {
	if sm == nil {
		return
	}
	sm.updateMu.Lock()
	defer sm.updateMu.Unlock()
	if err := sm.updateTableInfo(ctx, fs, data, startts, endts); err != nil {
		logutil.Error("snapshot table metadata initialization failed", zap.Error(err))
	}
}

func (sm *SnapshotMeta) TableInfoString() string {
	sm.RLock()
	defer sm.RUnlock()
	var buf bytes.Buffer
	for accID, tables := range sm.tables {
		buf.WriteString(fmt.Sprintf("accountID: %d\n", accID))
		for tid, table := range tables {
			if table == nil {
				buf.WriteString(fmt.Sprintf("tableID: %d, metadata: <nil>\n", tid))
				continue
			}
			buf.WriteString(fmt.Sprintf("tableID: %d, create: %s, deleteAt: %s\n",
				tid, table.createAt.ToString(), table.deleteAt.ToString()))
		}
	}
	return buf.String()
}

func (sm *SnapshotMeta) GetSnapshotListLocked(snapshots *SnapshotInfo, tid uint64) []types.TS {
	if sm == nil || snapshots == nil || sm.tableIDIndex[tid] == nil {
		return nil
	}
	accID := sm.tableIDIndex[tid].accountID
	return snapshots.account[accID]
}

// AccountToTableSnapshots returns a map from table id to its snapshots.
// The snapshots parameter contains all levels of snapshots.
// The pitr is the pitr info.
func (sm *SnapshotMeta) AccountToTableSnapshots(
	snapshots *SnapshotInfo,
	pitr *PitrInfo,
) (
	tableSnapshots map[uint64][]types.TS,
	tablePitrs map[uint64]*types.TS,
) {
	tableSnapshots = make(map[uint64][]types.TS, 100)
	tablePitrs = make(map[uint64]*types.TS, 100)
	if sm == nil {
		return
	}
	if snapshots == nil {
		snapshots = NewSnapshotInfo()
	}
	if pitr == nil {
		pitr = NewPitrInfo()
	}
	sm.RLock()
	defer sm.RUnlock()

	// 1. for system tables, flatten all snapshots to tableSnapshots
	var flattenSnapshots []types.TS
	{
		allSnapshots := snapshots.ToTsList()
		flattenSnapshots = compute.SortAndDedup(
			allSnapshots,
			func(a, b *types.TS) bool {
				return a.LT(b)
			},
			func(a, b *types.TS) bool {
				return a.EQ(b)
			},
		)
	}

	// 2. get the pitr.MinTS as the pitr for system tables
	sysPitr := pitr.MinTS()

	tableSnapshots[catalog2.MO_DATABASE_ID] = flattenSnapshots
	tableSnapshots[catalog2.MO_TABLES_ID] = flattenSnapshots
	tableSnapshots[catalog2.MO_COLUMNS_ID] = flattenSnapshots
	tablePitrs[catalog2.MO_DATABASE_ID] = &sysPitr
	tablePitrs[catalog2.MO_TABLES_ID] = &sysPitr
	tablePitrs[catalog2.MO_COLUMNS_ID] = &sysPitr

	// First, collect all table snapshots that should be applied to all tables in the same database
	dbTableSnapshots := make(map[uint64][]types.TS) // dbID -> []types.TS
	for tableID, tableTSList := range snapshots.tables {
		if len(tableTSList) > 0 {
			if info := sm.tableIDIndex[tableID]; info != nil {
				dbID := info.dbID
				if dbTableSnapshots[dbID] == nil {
					dbTableSnapshots[dbID] = make([]types.TS, 0)
				}
				dbTableSnapshots[dbID] = append(dbTableSnapshots[dbID], tableTSList...)
			}
		}
	}

	// Sort and deduplicate database-level table snapshots
	for dbID, tsList := range dbTableSnapshots {
		dbTableSnapshots[dbID] = compute.SortAndDedup(
			tsList,
			func(a, b *types.TS) bool {
				return a.LT(b)
			},
			func(a, b *types.TS) bool {
				return a.EQ(b)
			},
		)
	}

	for tid, info := range sm.tableIDIndex {
		if catalog2.IsSystemTable(tid) {
			continue
		}

		// Collect all applicable snapshots for this table (table + database + account + cluster)
		var allApplicableSnapshots []types.TS

		// 1. Add table-specific snapshots
		//if tableTSList := snapshots.tables[tid]; len(tableTSList) > 0 {
		//	logutil.Warn("GC-PANIC-DUP-TABLE-SNAP",
		//		zap.String("level", "table"),
		//		zap.Uint64("id", tid),
		//		zap.Int("count", len(tableTSList)),
		//	)
		//	allApplicableSnapshots = append(allApplicableSnapshots, tableTSList...)
		//}

		// 2. Add snapshots from other tables in the same database (if any table in this DB has snapshots)
		if dbTableTSList := dbTableSnapshots[info.dbID]; len(dbTableTSList) > 0 {
			allApplicableSnapshots = append(allApplicableSnapshots, dbTableTSList...)
		}

		// 3. Add database-specific snapshots
		if dbTSList := snapshots.database[info.dbID]; len(dbTSList) > 0 {
			allApplicableSnapshots = append(allApplicableSnapshots, dbTSList...)
		}

		// 4. Add account-specific snapshots
		accountID := info.accountID
		if accountTSList := snapshots.account[accountID]; len(accountTSList) > 0 {
			allApplicableSnapshots = append(allApplicableSnapshots, accountTSList...)
		}

		// 5. Add cluster snapshots
		if clusterTSList := snapshots.cluster; len(clusterTSList) > 0 {
			allApplicableSnapshots = append(allApplicableSnapshots, clusterTSList...)
		}

		// Sort and deduplicate the combined snapshots
		if len(allApplicableSnapshots) > 0 {
			tableSnapshots[tid] = compute.SortAndDedup(
				allApplicableSnapshots,
				func(a, b *types.TS) bool {
					return a.LT(b)
				},
				func(a, b *types.TS) bool {
					return a.EQ(b)
				},
			)
		}

		// get the pitr for the table
		ts := pitr.GetTS(info.accountID, info.dbID, tid)
		tablePitrs[tid] = &ts
	}
	return
}

func (sm *SnapshotMeta) GetPitrByTable(
	pitr *PitrInfo, dbID, tableID uint64,
) *types.TS {
	if sm == nil {
		return &types.TS{}
	}
	sm.RLock()
	defer sm.RUnlock()
	return sm.getPitrByTableLocked(pitr, dbID, tableID)
}

func (sm *SnapshotMeta) getPitrByTableLocked(
	pitr *PitrInfo, dbID, tableID uint64,
) *types.TS {
	var accountID uint32
	if tableInfo := sm.tableIDIndex[tableID]; tableInfo != nil {
		accountID = tableInfo.accountID
	}
	ts := pitr.GetTS(accountID, dbID, tableID)
	return &ts
}

func (sm *SnapshotMeta) MergeTableInfo(
	snapshots *SnapshotInfo,
	pitr *PitrInfo,
) error {
	if sm == nil || snapshots == nil || pitr == nil {
		return moerr.NewInvalidInputNoCtx(
			"snapshot table merge requires metadata, snapshots, and PITR state",
		)
	}
	sm.Lock()
	defer sm.Unlock()
	if len(sm.tables) == 0 {
		return nil
	}

	// First, collect all table snapshots that should be applied to all tables in the same database
	dbTableSnapshots := make(map[uint64][]types.TS) // dbID -> []types.TS
	for tableID, tableTSList := range snapshots.tables {
		if len(tableTSList) > 0 {
			if info := sm.tableIDIndex[tableID]; info != nil {
				dbID := info.dbID
				if dbTableSnapshots[dbID] == nil {
					dbTableSnapshots[dbID] = make([]types.TS, 0)
				}
				dbTableSnapshots[dbID] = append(dbTableSnapshots[dbID], tableTSList...)
			}
		}
	}

	// Sort and deduplicate database-level table snapshots
	for dbID, tsList := range dbTableSnapshots {
		dbTableSnapshots[dbID] = compute.SortAndDedup(
			tsList,
			func(a, b *types.TS) bool {
				return a.LT(b)
			},
			func(a, b *types.TS) bool {
				return a.EQ(b)
			},
		)
	}

	for accID, tables := range sm.tables {
		for _, table := range tables {
			// Get a list of snapshots available for the table
			// (including snapshots from other tables in the same database)
			var applicableSnapshots []types.TS

			// 1. Add table-specific snapshots
			//if tableSnapshots := snapshots.tables[table.tid]; len(tableSnapshots) > 0 {
			//	applicableSnapshots = append(applicableSnapshots, tableSnapshots...)
			//}

			// 2. Add snapshots from other tables in the same database (if any table in this DB has snapshots)
			if dbTableTSList := dbTableSnapshots[table.dbID]; len(dbTableTSList) > 0 {
				applicableSnapshots = append(applicableSnapshots, dbTableTSList...)
			}

			// 3. Add database-specific snapshots
			if dbSnapshots := snapshots.database[table.dbID]; len(dbSnapshots) > 0 {
				applicableSnapshots = append(applicableSnapshots, dbSnapshots...)
			}

			// 4. Add account-specific snapshots
			if accountSnapshots := snapshots.account[accID]; len(accountSnapshots) > 0 {
				applicableSnapshots = append(applicableSnapshots, accountSnapshots...)
			}

			// 5. Add cluster snapshots
			if clusterSnapshots := snapshots.cluster; len(clusterSnapshots) > 0 {
				applicableSnapshots = append(applicableSnapshots, clusterSnapshots...)
			}
			// Sort and deduplicate the combined snapshots
			if len(applicableSnapshots) > 0 {
				applicableSnapshots = compute.SortAndDedup(
					applicableSnapshots,
					func(a, b *types.TS) bool {
						return a.LT(b)
					},
					func(a, b *types.TS) bool {
						return a.EQ(b)
					},
				)
			}

			// If there is no snapshot and PITR is empty, delete the deleted table
			if len(applicableSnapshots) == 0 && pitr.IsEmpty() {
				if !table.deleteAt.IsEmpty() {
					delete(sm.tables[accID], table.tid)
					delete(sm.tableIDIndex, table.tid)
					if sm.objects[table.tid] != nil {
						delete(sm.objects, table.tid)
					}
				}
				continue
			}

			// Check if the table is referenced by the snapshot
			ts := sm.getPitrByTableLocked(pitr, table.dbID, table.tid)
			if !table.deleteAt.IsEmpty() &&
				!isSnapshotRefers(table, applicableSnapshots, ts) {
				delete(sm.tables[accID], table.tid)
				delete(sm.tableIDIndex, table.tid)
				if sm.objects[table.tid] != nil {
					delete(sm.objects, table.tid)
				}
			}
		}
	}
	hoursAgo := types.BuildTS(time.Now().UnixNano()-int64(3*time.Hour), 0)
	for key := range sm.aobjDelTsMap {
		if key.LT(&hoursAgo) {
			delete(sm.aobjDelTsMap, key)
		}
	}
	return nil
}

func (sm *SnapshotMeta) GetTableDropAt(tid uint64) (types.TS, bool) {
	sm.RLock()
	defer sm.RUnlock()
	if sm.tableIDIndex[tid] == nil {
		return types.TS{}, false
	}
	return sm.tableIDIndex[tid].deleteAt, true
}

func (sm *SnapshotMeta) GetAccountId(tid uint64) (uint32, bool) {
	sm.RLock()
	defer sm.RUnlock()
	if sm.tableIDIndex[tid] == nil {
		return 0, false
	}
	return sm.tableIDIndex[tid].accountID, true
}

// GetAllTableIDs returns a copy of all table IDs in the snapshot meta
func (sm *SnapshotMeta) GetAllTableIDs() map[uint64]bool {
	sm.RLock()
	defer sm.RUnlock()
	result := make(map[uint64]bool, len(sm.tableIDIndex))
	for tableID := range sm.tableIDIndex {
		result[tableID] = true
	}
	return result
}

// for test
func (sm *SnapshotMeta) GetTablePK(tid uint64) string {
	sm.RLock()
	defer sm.RUnlock()
	if sm.tableIDIndex[tid] == nil {
		return ""
	}
	return sm.tableIDIndex[tid].pk
}

func (sm *SnapshotMeta) String() string {
	sm.RLock()
	defer sm.RUnlock()
	return fmt.Sprintf("account count: %d, table count: %d, object count: %d",
		len(sm.tables), len(sm.tableIDIndex), len(sm.objects))
}

func isSnapshotRefers(table *tableInfo, snapVec []types.TS, pitr *types.TS) bool {
	if !pitr.IsEmpty() {
		if table.deleteAt.GT(pitr) {
			return true
		}
	}
	if len(snapVec) == 0 {
		return false
	}
	left, right := 0, len(snapVec)-1
	for left <= right {
		mid := left + (right-left)/2
		snapTS := snapVec[mid]
		if snapTS.GE(&table.createAt) && snapTS.LT(&table.deleteAt) {
			common.DoIfDebugEnabled(func() {
				logutil.Debug(
					"isSnapshotRefers",
					zap.String("snap-ts", snapTS.ToString()),
					zap.String("create-ts", table.createAt.ToString()),
					zap.String("drop-ts", table.deleteAt.ToString()),
					zap.Uint64("tid", table.tid),
				)
			})
			return true
		} else if snapTS.LT(&table.createAt) {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return false
}

func ObjectIsSnapshotRefers(
	obj *objectio.ObjectStats,
	pitr, createTS, dropTS *types.TS,
	snapshots []types.TS,
) bool {
	// no snapshot and no pitr
	if len(snapshots) == 0 && (pitr == nil || pitr.IsEmpty()) {
		return false
	}

	// if dropTS is empty, it means the object is not dropped
	if dropTS.IsEmpty() {
		common.DoIfDebugEnabled(func() {
			logutil.Debug(
				"GCJOB-DEBUG-1",
				zap.String("obj", obj.ObjectName().String()),
				zap.String("create-ts", createTS.ToString()),
				zap.String("drop-ts", createTS.ToString()),
			)
		})
		return true
	}

	// if pitr is not empty, and pitr is greater than dropTS, it means the object is not dropped
	if pitr != nil && !pitr.IsEmpty() {
		if dropTS.GT(pitr) {
			common.DoIfDebugEnabled(func() {
				logutil.Debug(
					"GCJOB-PITR-PIN",
					zap.String("name", obj.ObjectName().String()),
					zap.String("pitr", pitr.ToString()),
					zap.String("create-ts", createTS.ToString()),
					zap.String("drop-ts", dropTS.ToString()),
				)
			})
			return true
		}
	}

	left, right := 0, len(snapshots)-1
	for left <= right {
		mid := left + (right-left)/2
		snapTS := snapshots[mid]
		if snapTS.GE(createTS) && snapTS.LT(dropTS) {
			common.DoIfDebugEnabled(func() {
				logutil.Debug(
					"GCJOB-DEBUG-2",
					zap.String("name", obj.ObjectName().String()),
					zap.String("pitr", snapTS.ToString()),
					zap.String("create-ts", createTS.ToString()),
					zap.String("drop-ts", dropTS.ToString()),
				)
			})
			return true
		} else if snapTS.LT(createTS) {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return false
}
