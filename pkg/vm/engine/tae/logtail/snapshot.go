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
	"sort"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"

	"go.uber.org/zap"

	catalog2 "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

const (
	SnapshotTypeIdx types.Enum = iota
	SnapshotTypeCluster
	SnapshotTypeAccount
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

type PitrInfo struct {
	cluster  types.TS
	account  map[uint32]types.TS
	database map[uint64]types.TS
	tables   map[uint64]types.TS
}

func (p *PitrInfo) IsEmpty() bool {
	return p.cluster.IsEmpty() &&
		len(p.account) == 0 &&
		len(p.database) == 0 &&
		len(p.tables) == 0
}

func (p *PitrInfo) GetTS(
	accountID uint32,
	dbID uint64,
	tableID uint64,
) (ts types.TS) {
	ts = p.cluster
	accountTS := p.account[accountID]
	if !accountTS.IsEmpty() && (ts.IsEmpty() || accountTS.LT(&ts)) {
		ts = accountTS
	}

	dbTS := p.database[dbID]
	if !dbTS.IsEmpty() && (ts.IsEmpty() || dbTS.LT(&ts)) {
		ts = dbTS
	}

	tableTS := p.tables[tableID]
	if !tableTS.IsEmpty() && (ts.IsEmpty() || tableTS.LT(&ts)) {
		ts = tableTS
	}
	return
}

func (p *PitrInfo) MinTS() (ts types.TS) {
	if !p.cluster.IsEmpty() {
		ts = p.cluster
	}

	// find the minimum account ts
	for _, p := range p.account {
		if ts.IsEmpty() || p.LT(&ts) {
			ts = p
		}
	}

	// find the minimum database ts
	for _, p := range p.database {
		if ts.IsEmpty() || p.LT(&ts) {
			ts = p
		}
	}

	// find the minimum table ts
	for _, p := range p.tables {
		if ts.IsEmpty() || p.LT(&ts) {
			ts = p
		}
	}
	return
}

func (p *PitrInfo) ToTsList() []types.TS {
	tsList := make([]types.TS, 0, len(p.account)+len(p.database)+len(p.tables)+1)
	for _, ts := range p.account {
		tsList = append(tsList, ts)
	}
	for _, ts := range p.database {
		tsList = append(tsList, ts)
	}
	for _, ts := range p.tables {
		tsList = append(tsList, ts)
	}
	if !p.cluster.IsEmpty() {
		tsList = append(tsList, p.cluster)
	}
	return tsList
}

type SnapshotMeta struct {
	sync.RWMutex

	// all objects&tombstones in the mo_snapshots table, because there
	// will be multiple mo_snapshots, so here is a map, the key is tid.
	objects    map[uint64]map[objectio.Segmentid]*objectInfo
	tombstones map[uint64]map[objectio.Segmentid]*objectInfo

	aobjDelTsMap map[types.TS]struct{} // used for filering out transferred tombstones

	pitr struct {
		tid        uint64
		objects    map[objectio.Segmentid]*objectInfo
		tombstones map[objectio.Segmentid]*objectInfo
	}

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
	meta.pitr.objects = make(map[objectio.Segmentid]*objectInfo)
	meta.pitr.tombstones = make(map[objectio.Segmentid]*objectInfo)
	return meta
}

func copyObjectsLocked(
	objects map[uint64]map[objectio.Segmentid]*objectInfo,
) map[uint64]map[objectio.Segmentid]*objectInfo {
	newMap := make(map[uint64]map[objectio.Segmentid]*objectInfo)
	for k, v := range objects {
		newMap[k] = make(map[objectio.Segmentid]*objectInfo)
		for kk, vv := range v {
			newMap[k][kk] = vv
		}
	}
	return newMap
}

func (sm *SnapshotMeta) copyTablesLocked() map[uint32]map[uint64]*tableInfo {
	tables := make(map[uint32]map[uint64]*tableInfo)
	for k, v := range sm.tables {
		tables[k] = make(map[uint64]*tableInfo)
		for kk, vv := range v {
			tables[k][kk] = vv
		}
	}
	return tables
}

func IsMoTable(tid uint64) bool {
	return tid == catalog2.MO_TABLES_ID
}

type tombstone struct {
	rowid types.Rowid
	pk    types.Tuple
	ts    types.TS
}

func (sm *SnapshotMeta) updateTableInfo(
	ctx context.Context,
	fs fileservice.FileService,
	data *CheckpointData, startts, endts types.TS,
) error {
	var objects map[uint64]map[objectio.Segmentid]*objectInfo
	var tombstones map[uint64]map[objectio.Segmentid]*objectInfo
	objects = make(map[uint64]map[objectio.Segmentid]*objectInfo, 1)
	tombstones = make(map[uint64]map[objectio.Segmentid]*objectInfo, 1)
	objects[catalog2.MO_TABLES_ID] = make(map[objectio.Segmentid]*objectInfo)
	tombstones[catalog2.MO_TABLES_ID] = make(map[objectio.Segmentid]*objectInfo)
	collector := func(
		objects *map[uint64]map[objectio.Segmentid]*objectInfo,
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
		moTable[id] = &objectInfo{
			stats:    stats,
			createAt: createTS,
			deleteAt: deleteTS,
		}
	}
	collectObjects(&objects, nil, data.GetObjectBatchs(), collector)
	collectObjects(&tombstones, nil, data.GetTombstoneObjectBatchs(), collector)
	tObjects := objects[catalog2.MO_TABLES_ID]
	tTombstones := tombstones[catalog2.MO_TABLES_ID]
	orderedInfos := make([]*objectInfo, 0, len(tObjects))
	for _, info := range tObjects {
		orderedInfos = append(orderedInfos, info)
	}
	sort.Slice(orderedInfos, func(i, j int) bool {
		return orderedInfos[i].createAt.LT(&orderedInfos[j].createAt)
	})

	for _, info := range orderedInfos {
		if info.stats.BlkCnt() != 1 {
			panic(fmt.Sprintf("mo_table object %v blk cnt %v",
				info.stats.ObjectName(), info.stats.BlkCnt()))
		}
		if !info.deleteAt.IsEmpty() {
			sm.aobjDelTsMap[info.deleteAt] = struct{}{}
		}
		objectBat, _, err := blockio.LoadOneBlock(
			ctx,
			fs,
			info.stats.ObjectLocation(),
			objectio.SchemaData,
		)
		if err != nil {
			return err
		}
		// 0 is table id
		// 1 is table name
		// 11 is account id
		// len(objectBat.Vecs)-1 is commit ts
		ids := vector.MustFixedColWithTypeCheck[uint64](objectBat.Vecs[0])
		nameVarlena := vector.MustFixedColWithTypeCheck[types.Varlena](objectBat.Vecs[1])
		nameArea := objectBat.Vecs[1].GetArea()
		dbs := vector.MustFixedColWithTypeCheck[uint64](objectBat.Vecs[3])
		accounts := vector.MustFixedColWithTypeCheck[uint32](objectBat.Vecs[11])
		creates := vector.MustFixedColWithTypeCheck[types.TS](objectBat.Vecs[len(objectBat.Vecs)-1])
		for i := 0; i < len(ids); i++ {
			createAt := creates[i]
			if createAt.LT(&startts) || createAt.GT(&endts) {
				continue
			}
			name := string(nameVarlena[i].GetByteSlice(nameArea))
			tid := ids[i]
			account := accounts[i]
			db := dbs[i]
			tuple, _, _, err := types.DecodeTuple(
				objectBat.Vecs[len(objectBat.Vecs)-3].GetRawBytesAt(i))
			if err != nil {
				return err
			}
			pk := tuple.ErrString(nil)
			if name == catalog2.MO_SNAPSHOTS {
				sm.snapshotTableIDs[tid] = struct{}{}
				logutil.Info(
					"UpdateSnapTable-P1",
					zap.Uint64("tid", tid),
					zap.Uint32("account", account),
					zap.String("create-at", createAt.ToString()),
				)
			}
			if name == catalog2.MO_PITR {
				if sm.pitr.tid > 0 && sm.pitr.tid != tid {
					panic(fmt.Sprintf("pitr table %v is not unique", tid))
				}
				sm.pitr.tid = tid
			}
			if sm.tables[account] == nil {
				sm.tables[account] = make(map[uint64]*tableInfo)
			}
			table := sm.tables[account][tid]
			if table != nil {
				if table.createAt.GT(&createAt) {
					panic(fmt.Sprintf("table %v %v create at %v is greater than %v",
						tid, tuple.ErrString(nil), table.createAt.ToString(), createAt.ToString()))
				}
				if table.pk == pk {
					sm.tablePKIndex[pk] = append(sm.tablePKIndex[pk], table)
					continue
				}
				createAt = table.createAt
			}
			table = &tableInfo{
				accountID: account,
				dbID:      db,
				tid:       tid,
				createAt:  createAt,
				pk:        pk,
			}
			sm.tables[account][tid] = table
			sm.tableIDIndex[tid] = table
			if sm.tablePKIndex[pk] == nil {
				sm.tablePKIndex[pk] = make([]*tableInfo, 0)
			}
			sm.tablePKIndex[pk] = append(sm.tablePKIndex[pk], table)
		}
	}

	deleteRows := make([]tombstone, 0)
	for _, info := range tTombstones {
		if info.stats.BlkCnt() != 1 {
			panic(fmt.Sprintf("mo_table tombstone %v blk cnt %v",
				info.stats.ObjectName(), info.stats.BlkCnt()))
		}
		objectBat, _, err := blockio.LoadOneBlock(
			ctx,
			fs,
			info.stats.ObjectLocation(),
			objectio.SchemaData,
		)
		if err != nil {
			return err
		}

		commitTsVec := vector.MustFixedColWithTypeCheck[types.TS](objectBat.Vecs[len(objectBat.Vecs)-1])
		rowIDVec := vector.MustFixedColWithTypeCheck[types.Rowid](objectBat.Vecs[0])
		for i := 0; i < len(commitTsVec); i++ {
			pk, _, _, _ := types.DecodeTuple(objectBat.Vecs[1].GetRawBytesAt(i))
			commitTs := commitTsVec[i]
			if commitTs.LT(&startts) || commitTs.GT(&endts) {
				continue
			}
			if _, ok := sm.aobjDelTsMap[commitTs]; ok {
				logutil.Infof("yyyy skip table %v @ %v", pk.ErrString(nil), commitTs.ToString())
				continue
			}
			deleteRows = append(deleteRows, tombstone{
				rowid: rowIDVec[i],
				pk:    pk,
				ts:    commitTs,
			})
		}
	}
	sort.Slice(deleteRows, func(i, j int) bool {
		ts2 := deleteRows[j].ts
		return deleteRows[i].ts.LT(&ts2)
	})

	for _, del := range deleteRows {
		pk := del.pk.ErrString(nil)
		if sm.tablePKIndex[pk] == nil {
			continue
		}
		if len(sm.tablePKIndex[pk]) == 0 {
			logutil.Warnf("[UpdateTableInfoWarn] delete table %v not found @ rowid %v, commit %v, start is %v, end is %v",
				del.pk.ErrString(nil), del.rowid.String(), del.ts.ToString(), startts.ToString(), endts.ToString())
			continue
		}
		table := sm.tablePKIndex[pk][0]
		if !table.deleteAt.IsEmpty() && table.deleteAt.GT(&del.ts) {
			panic(fmt.Sprintf("table %v delete at %v is greater than %v", table.tid, table.deleteAt, del.ts))
		}
		table.deleteAt = del.ts
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
		sm.tables[table.accountID][table.tid] = table
	}

	for pk, tables := range sm.tablePKIndex {
		if len(tables) > 1 {
			logutil.Warn("UpdateSnapTable-Error",
				zap.String("table", pk),
				zap.Int("len", len(tables)),
			)
		}
		if len(tables) == 0 {
			continue
		}
		tables[0].deleteAt = types.TS{}
	}
	return nil
}

func collectObjects(
	objects *map[uint64]map[objectio.Segmentid]*objectInfo,
	objects2 *map[objectio.Segmentid]*objectInfo,
	ins *containers.Batch,
	collector func(
		*map[uint64]map[objectio.Segmentid]*objectInfo,
		*map[objectio.Segmentid]*objectInfo,
		uint64,
		objectio.ObjectStats,
		types.TS, types.TS,
	),
) {
	insDeleteTSs := vector.MustFixedColWithTypeCheck[types.TS](
		ins.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector())
	insCreateTSs := vector.MustFixedColWithTypeCheck[types.TS](
		ins.GetVectorByName(catalog.EntryNode_CreateAt).GetDownstreamVector())
	insTableIDs := vector.MustFixedColWithTypeCheck[uint64](
		ins.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector())
	insStats := ins.GetVectorByName(catalog.ObjectAttr_ObjectStats).GetDownstreamVector()

	for i := 0; i < ins.Length(); i++ {
		table := insTableIDs[i]
		deleteTS := insDeleteTSs[i]
		createTS := insCreateTSs[i]
		objectStats := (objectio.ObjectStats)(insStats.GetBytesAt(i))
		collector(objects, objects2, table, objectStats, createTS, deleteTS)
	}
}

func (sm *SnapshotMeta) Update(
	ctx context.Context,
	fs fileservice.FileService,
	data *CheckpointData,
	startts, endts types.TS,
	taskName string,
) (err error) {
	sm.Lock()
	defer sm.Unlock()

	now := time.Now()
	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		logger(
			"GC-SnapshotMeta-Update",
			zap.Error(err),
			zap.Duration("cost", time.Since(now)),
			zap.String("start-ts", startts.ToString()),
			zap.String("end-ts", endts.ToString()),
			zap.String("task", taskName),
		)
	}()

	if err = sm.updateTableInfo(
		ctx,
		fs,
		data,
		startts,
		endts,
	); err != nil {
		return
	}

	if len(sm.snapshotTableIDs) == 0 && sm.pitr.tid == 0 {
		return
	}

	collector := func(
		objects1 *map[uint64]map[objectio.Segmentid]*objectInfo,
		objects2 *map[objectio.Segmentid]*objectInfo,
		tid uint64,
		stats objectio.ObjectStats,
		createTS types.TS, deleteTS types.TS,
	) {
		mapFun := func(
			objects1 map[objectio.Segmentid]*objectInfo,
		) {
			if objects1 == nil {
				objects1 = make(map[objectio.Segmentid]*objectInfo)
			}
			id := stats.ObjectName().SegmentId()
			if objects1[id] == nil {
				if !deleteTS.IsEmpty() {
					return
				}
				objects1[id] = &objectInfo{
					stats:    stats,
					createAt: createTS,
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
			if deleteTS.IsEmpty() {
				// Compatible with the cluster restored by backup
				logutil.Warn(
					"GC-SnapshotMeta-Update-Collector-Skip",
					zap.Uint64("table-id", tid),
					zap.String("object-name", stats.ObjectName().String()),
					zap.String("create-at", createTS.ToString()),
					zap.String("task", taskName),
					zap.String("start", startts.ToString()),
					zap.String("end", endts.ToString()),
				)
				return
			}
			logutil.Info(
				"GC-SnapshotMeta-Update-Collector",
				zap.Uint64("table-id", tid),
				zap.String("object-name", id.String()),
				zap.String("delete-at", deleteTS.ToString()),
			)

			delete(objects1, id)
		}
		if tid == sm.pitr.tid {
			mapFun(*objects2)
		}
		if _, ok := sm.snapshotTableIDs[tid]; !ok {
			return
		}
		if (*objects1)[tid] == nil {
			(*objects1)[tid] = make(map[objectio.Segmentid]*objectInfo)
		}
		mapFun((*objects1)[tid])
	}
	collectObjects(
		&sm.objects,
		&sm.pitr.objects,
		data.GetObjectBatchs(),
		collector,
	)
	collectObjects(
		&sm.tombstones,
		&sm.pitr.tombstones,
		data.GetTombstoneObjectBatchs(),
		collector,
	)
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
	}
}

func (sm *SnapshotMeta) GetSnapshot(
	ctx context.Context,
	sid string,
	fs fileservice.FileService,
	mp *mpool.MPool,
) (map[uint32]containers.Vector, error) {
	var err error

	now := time.Now()
	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		logger(
			"GetSnapshot",
			zap.Error(err),
			zap.Duration("cost", time.Since(now)),
		)
	}()

	sm.RLock()
	objects := copyObjectsLocked(sm.objects)
	tombstones := copyObjectsLocked(sm.tombstones)
	tables := sm.copyTablesLocked()
	sm.RUnlock()
	snapshotList := make(map[uint32]containers.Vector)
	idxes := []uint16{ColTS, ColLevel, ColObjId}
	colTypes := []types.Type{
		snapshotSchemaTypes[ColTS],
		snapshotSchemaTypes[ColLevel],
		snapshotSchemaTypes[ColObjId],
	}
	for tid, objectMap := range objects {
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		default:
		}
		tombstonesStats := make([]objectio.ObjectStats, 0)
		for ttid, tombstoneMap := range tombstones {
			if ttid != tid {
				continue
			}
			for _, object := range tombstoneMap {
				tombstonesStats = append(tombstonesStats, object.stats)
			}
			break
		}
		checkpointTS := types.BuildTS(time.Now().UTC().UnixNano(), 0)
		ds := NewSnapshotDataSource(ctx, fs, checkpointTS, tombstonesStats)
		for _, object := range objectMap {
			for i := uint32(0); i < object.stats.BlkCnt(); i++ {
				blk := object.stats.ConstructBlockInfo(uint16(i))
				buildBatch := func() *batch.Batch {
					result := batch.NewWithSize(len(colTypes))
					for i, typ := range colTypes {
						result.Vecs[i] = vector.NewVec(typ)
					}
					return result
				}

				bat := buildBatch()
				defer bat.Clean(mp)
				if bat, _, err = blockio.BlockDataReadBackup(
					ctx, &blk, ds, idxes, types.TS{}, fs,
				); err != nil {
					return nil, err
				}
				tsList := vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0])
				typeList := vector.MustFixedColWithTypeCheck[types.Enum](bat.Vecs[1])
				acctList := vector.MustFixedColWithTypeCheck[uint64](bat.Vecs[2])
				for r := 0; r < bat.Vecs[0].Length(); r++ {
					ts := tsList[r]
					snapTs := types.BuildTS(ts, 0)
					acct := acctList[r]
					snapshotType := typeList[r]
					if snapshotType == SnapshotTypeCluster {
						for account := range tables {
							if snapshotList[account] == nil {
								snapshotList[account] = containers.MakeVector(types.T_TS.ToType(), mp)
							}
							if err = vector.AppendFixed[types.TS](
								snapshotList[account].GetDownstreamVector(), snapTs, false, mp,
							); err != nil {
								return nil, err
							}
							// TODO: info to debug
							logutil.Info(
								"GetSnapshot-P1",
								zap.String("ts", snapTs.ToString()),
								zap.Uint32("account", account),
							)
						}
						continue
					}
					id := uint32(acct)
					if snapshotList[id] == nil {
						snapshotList[id] = containers.MakeVector(types.T_TS.ToType(), mp)
					}
					// TODO: info to debug
					logutil.Info(
						"GetSnapshot-P2",
						zap.String("ts", snapTs.ToString()),
						zap.Uint32("account", id),
					)

					if err = vector.AppendFixed[types.TS](
						snapshotList[id].GetDownstreamVector(), snapTs, false, mp,
					); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	for i := range snapshotList {
		snapshotList[i].GetDownstreamVector().InplaceSort()
	}
	return snapshotList, nil
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

func (sm *SnapshotMeta) GetPITR(
	ctx context.Context,
	sid string,
	gcTime time.Time,
	fs fileservice.FileService,
	mp *mpool.MPool,
) (*PitrInfo, error) {
	idxes := []uint16{ColPitrLevel, ColPitrObjId, ColPitrLength, ColPitrUnit}
	tombstonesStats := make([]objectio.ObjectStats, 0)
	for _, tombstone := range sm.pitr.tombstones {
		tombstonesStats = append(tombstonesStats, tombstone.stats)
	}
	checkpointTS := types.BuildTS(time.Now().UTC().UnixNano(), 0)
	ds := NewSnapshotDataSource(ctx, fs, checkpointTS, tombstonesStats)
	pitr := &PitrInfo{
		cluster:  types.TS{},
		account:  make(map[uint32]types.TS),
		database: make(map[uint64]types.TS),
		tables:   make(map[uint64]types.TS),
	}
	for _, object := range sm.pitr.objects {
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		default:
		}
		location := object.stats.ObjectLocation()
		name := object.stats.ObjectName()
		for i := uint32(0); i < object.stats.BlkCnt(); i++ {
			loc := objectio.BuildLocation(name, location.Extent(), 0, uint16(i))
			blk := objectio.BlockInfo{
				BlockID: *objectio.BuildObjectBlockid(name, uint16(i)),
				MetaLoc: objectio.ObjectLocation(loc),
			}

			bat, _, err := blockio.BlockDataReadBackup(ctx, &blk, ds, idxes, types.TS{}, fs)
			if err != nil {
				return nil, err
			}
			defer bat.Clean(mp)
			objIDList := vector.MustFixedColWithTypeCheck[uint64](bat.Vecs[1])
			lengList := vector.MustFixedColWithTypeCheck[uint8](bat.Vecs[2])
			for r := 0; r < bat.Vecs[0].Length(); r++ {
				length := lengList[r]
				leng := int(length)
				unit := bat.Vecs[3].GetStringAt(r)
				var ts time.Time
				if unit == PitrUnitYear {
					ts = AddDate(gcTime, 1-leng, 0, 0)
				} else if unit == PitrUnitMonth {
					ts = AddDate(gcTime, 0, 1-leng, 0)
				} else if unit == PitrUnitDay {
					ts = gcTime.AddDate(0, 0, 1-leng)
				} else if unit == PitrUnitHour {
					ts = gcTime.Add(-time.Duration(leng) * time.Hour)
				} else if unit == PitrUnitMinute {
					ts = gcTime.Add(-time.Duration(leng) * time.Minute)
				}
				pitrTs := types.BuildTS(ts.UnixNano(), 0)
				account := objIDList[r]
				level := bat.Vecs[0].GetStringAt(r)
				if level == PitrLevelCluster {
					if !pitr.cluster.IsEmpty() {
						panic("cluster duplicate pitr ")
					}
					pitr.cluster = pitrTs

				} else if level == PitrLevelAccount {
					id := uint32(account)
					p := pitr.account[id]
					if !p.IsEmpty() && p.LT(&pitrTs) {
						continue
					}
					pitr.account[id] = pitrTs
				} else if level == PitrLevelDatabase {
					id := uint64(account)
					p := pitr.database[id]
					if !p.IsEmpty() {
						panic("db duplicate pitr ")
					}
					pitr.database[id] = pitrTs
				} else if level == PitrLevelTable {
					id := uint64(account)
					p := pitr.tables[id]
					if !p.IsEmpty() {
						panic("table duplicate pitr ")
					}
					pitr.tables[id] = pitrTs
				}
				// TODO: info to debug
				logutil.Info(
					"GC-GetPITR",
					zap.String("level", level),
					zap.Uint64("id", account),
					zap.String("ts", pitrTs.ToString()),
				)
			}
		}
	}
	return pitr, nil
}

func (sm *SnapshotMeta) SetTid(tid uint64) {
	sm.snapshotTableIDs[tid] = struct{}{}
}

func (sm *SnapshotMeta) SaveMeta(name string, fs fileservice.FileService) (uint32, error) {
	if len(sm.objects) == 0 && len(sm.pitr.objects) == 0 {
		return 0, nil
	}
	bat := containers.NewBatch()
	deltaBat := containers.NewBatch()
	for i, attr := range objectInfoSchemaAttr {
		bat.AddVector(attr, containers.MakeVector(objectInfoSchemaTypes[i], common.DebugAllocator))
		deltaBat.AddVector(attr, containers.MakeVector(objectInfoSchemaTypes[i], common.DebugAllocator))
	}
	appendBatForMap := func(
		bat *containers.Batch,
		tid uint64,
		objectMap map[objectio.Segmentid]*objectInfo) {
		for _, entry := range objectMap {
			vector.AppendBytes(
				bat.GetVectorByName(catalog.ObjectAttr_ObjectStats).GetDownstreamVector(),
				entry.stats[:], false, common.DebugAllocator)
			vector.AppendFixed[types.TS](
				bat.GetVectorByName(catalog.EntryNode_CreateAt).GetDownstreamVector(),
				entry.createAt, false, common.DebugAllocator)
			vector.AppendFixed[types.TS](
				bat.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector(),
				entry.deleteAt, false, common.DebugAllocator)
			vector.AppendFixed[uint64](
				bat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector(),
				tid, false, common.DebugAllocator)
		}
	}
	appendBat := func(
		bat *containers.Batch,
		objects map[uint64]map[objectio.Segmentid]*objectInfo) {
		for tid, objectMap := range objects {
			appendBatForMap(bat, tid, objectMap)
		}
	}
	appendBat(bat, sm.objects)
	appendBatForMap(bat, sm.pitr.tid, sm.pitr.objects)
	appendBat(deltaBat, sm.tombstones)
	appendBatForMap(deltaBat, sm.pitr.tid, sm.pitr.tombstones)
	defer bat.Close()
	defer deltaBat.Close()
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
	if len(sm.tables) == 0 {
		return 0, nil
	}
	bat := containers.NewBatch()
	snapTableBat := containers.NewBatch()
	pitrTableBat := containers.NewBatch()
	for i, attr := range tableInfoSchemaAttr {
		bat.AddVector(attr, containers.MakeVector(tableInfoSchemaTypes[i], common.DebugAllocator))
		snapTableBat.AddVector(attr, containers.MakeVector(tableInfoSchemaTypes[i], common.DebugAllocator))
		pitrTableBat.AddVector(attr, containers.MakeVector(tableInfoSchemaTypes[i], common.DebugAllocator))
	}
	appendBat := func(bat *containers.Batch, table *tableInfo) {
		vector.AppendFixed[uint32](
			bat.GetVectorByName(catalog2.SystemColAttr_AccID).GetDownstreamVector(),
			table.accountID, false, common.DebugAllocator)
		vector.AppendFixed[uint64](
			bat.GetVectorByName(catalog2.SystemRelAttr_DBID).GetDownstreamVector(),
			table.dbID, false, common.DebugAllocator)
		vector.AppendFixed[uint64](
			bat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector(),
			table.tid, false, common.DebugAllocator)
		vector.AppendFixed[types.TS](
			bat.GetVectorByName(catalog2.SystemRelAttr_CreateAt).GetDownstreamVector(),
			table.createAt, false, common.DebugAllocator)
		vector.AppendFixed[types.TS](
			bat.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector(),
			table.deleteAt, false, common.DebugAllocator)
		vector.AppendBytes(bat.GetVectorByName(MoTablesPK).GetDownstreamVector(),
			[]byte(table.pk), false, common.DebugAllocator)
	}
	for _, entry := range sm.tables {
		for _, table := range entry {
			appendBat(bat, table)

			if table.tid == sm.pitr.tid {
				appendBat(pitrTableBat, table)
				continue
			}

			if _, ok := sm.snapshotTableIDs[table.tid]; ok {
				appendBat(snapTableBat, table)
			}
		}
	}
	defer bat.Close()
	defer snapTableBat.Close()
	defer pitrTableBat.Close()

	aObjDelTsBat := containers.NewBatch()
	for i, attr := range aObjectDelSchemaAttr {
		aObjDelTsBat.AddVector(attr, containers.MakeVector(aObjectDelSchemaTypes[i], common.DebugAllocator))
	}

	for ts := range sm.aobjDelTsMap {
		vector.AppendFixed[types.TS](
			aObjDelTsBat.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector(),
			ts, false, common.DebugAllocator)
	}
	defer aObjDelTsBat.Close()

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

	_, err = writer.WriteEnd(context.Background())
	if err != nil {
		return 0, err
	}
	stats := writer.GetObjectStats()
	size := stats.OriginSize()
	return size, err
}

func (sm *SnapshotMeta) RebuildTableInfo(ins *containers.Batch) {
	sm.Lock()
	defer sm.Unlock()
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
			logutil.Warn("RebuildTableInfo-PK-Exists",
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
	sm.Lock()
	defer sm.Unlock()
	insTIDs := vector.MustFixedColWithTypeCheck[uint64](
		ins.GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector())
	if ins.Length() < 1 {
		logutil.Warnf("RebuildPitr unexpected length %d", ins.Length())
		return
	}
	logutil.Infof("RebuildPitr tid %d", insTIDs[0])
	for i := 0; i < ins.Length(); i++ {
		tid := insTIDs[i]
		sm.pitr.tid = tid
	}
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

func (sm *SnapshotMeta) Rebuild(
	ins *containers.Batch,
	objects *map[uint64]map[objectio.Segmentid]*objectInfo,
	objects2 *map[objectio.Segmentid]*objectInfo,
) {
	sm.Lock()
	defer sm.Unlock()
	insCreateTSs := vector.MustFixedColWithTypeCheck[types.TS](ins.GetVectorByName(catalog.EntryNode_CreateAt).GetDownstreamVector())
	insTides := vector.MustFixedColWithTypeCheck[uint64](ins.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector())
	for i := 0; i < ins.Length(); i++ {
		var objectStats objectio.ObjectStats
		buf := ins.GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
		objectStats.UnMarshal(buf)
		createTS := insCreateTSs[i]
		tid := insTides[i]
		if tid == sm.pitr.tid {
			if (*objects2)[objectStats.ObjectName().SegmentId()] == nil {
				(*objects2)[objectStats.ObjectName().SegmentId()] = &objectInfo{
					stats:    objectStats,
					createAt: createTS,
				}
				logutil.Info(
					"GC-Rebuild-P1",
					zap.String("object-name", objectStats.ObjectName().String()),
					zap.String("create-at", createTS.ToString()),
				)
			}
			continue
		}
		if _, ok := sm.snapshotTableIDs[tid]; !ok {
			sm.snapshotTableIDs[tid] = struct{}{}
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
}

func (sm *SnapshotMeta) ReadMeta(ctx context.Context, name string, fs fileservice.FileService) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	default:
	}

	reader, err := blockio.NewFileReaderNoCache(fs, name)
	if err != nil {
		return err
	}
	bs, err := reader.LoadAllBlocks(ctx, common.DebugAllocator)
	if err != nil {
		return err
	}
	idxes := make([]uint16, len(objectInfoSchemaAttr))
	for i := range objectInfoSchemaAttr {
		idxes[i] = uint16(i)
	}
	mobat, release, err := reader.LoadColumns(ctx, idxes, nil, bs[0].GetID(), common.DebugAllocator)
	if err != nil {
		return err
	}
	defer release()
	bat := containers.NewBatch()
	defer bat.Close()
	for i := range objectInfoSchemaAttr {
		pkgVec := mobat.Vecs[i]
		var vec containers.Vector
		if pkgVec.Length() == 0 {
			vec = containers.MakeVector(objectInfoSchemaTypes[i], common.DebugAllocator)
		} else {
			vec = containers.ToTNVector(pkgVec, common.DebugAllocator)
		}
		bat.AddVector(objectInfoSchemaAttr[i], vec)
	}
	sm.Rebuild(bat, &sm.objects, &sm.pitr.objects)

	if len(bs) == 1 {
		return nil
	}

	idxes = make([]uint16, len(objectInfoSchemaAttr))
	for i := range objectInfoSchemaAttr {
		idxes[i] = uint16(i)
	}
	moDeltaBat, releaseDelta, err := reader.LoadColumns(ctx, idxes, nil, bs[1].GetID(), common.DebugAllocator)
	if err != nil {
		return err
	}
	defer releaseDelta()
	deltaBat := containers.NewBatch()
	defer deltaBat.Close()
	for i := range objectInfoSchemaAttr {
		pkgVec := moDeltaBat.Vecs[i]
		var vec containers.Vector
		if pkgVec.Length() == 0 {
			vec = containers.MakeVector(objectInfoSchemaTypes[i], common.DebugAllocator)
		} else {
			vec = containers.ToTNVector(pkgVec, common.DebugAllocator)
		}
		deltaBat.AddVector(objectInfoSchemaAttr[i], vec)
	}
	sm.Rebuild(deltaBat, &sm.tombstones, &sm.pitr.tombstones)
	return nil
}

func (sm *SnapshotMeta) ReadTableInfo(ctx context.Context, name string, fs fileservice.FileService) error {
	reader, err := blockio.NewFileReaderNoCache(fs, name)
	if err != nil {
		return err
	}
	bs, err := reader.LoadAllBlocks(ctx, common.DebugAllocator)
	if err != nil {
		return err
	}
	idxes := make([]uint16, len(tableInfoSchemaAttr))
	for i := range tableInfoSchemaAttr {
		idxes[i] = uint16(i)
	}
	for id, block := range bs {
		var mobat *batch.Batch
		var release func()
		if id == int(AObjectDelIdx) {
			mobat, release, err = reader.LoadColumns(ctx, []uint16{0}, nil, block.GetID(), common.DebugAllocator)
		} else {
			mobat, release, err = reader.LoadColumns(ctx, idxes, nil, block.GetID(), common.DebugAllocator)
		}
		if err != nil {
			return err
		}
		defer release()
		bat := containers.NewBatch()
		defer bat.Close()
		if id == int(AObjectDelIdx) {
			for i := range aObjectDelSchemaAttr {
				pkgVec := mobat.Vecs[i]
				var vec containers.Vector
				if pkgVec.Length() == 0 {
					vec = containers.MakeVector(aObjectDelSchemaTypes[i], common.DebugAllocator)
				} else {
					vec = containers.ToTNVector(pkgVec, common.DebugAllocator)
				}
				bat.AddVector(aObjectDelSchemaAttr[i], vec)
			}
			sm.RebuildAObjectDel(bat)
			continue
		}
		for i := range tableInfoSchemaAttr {
			pkgVec := mobat.Vecs[i]
			var vec containers.Vector
			if pkgVec.Length() == 0 {
				vec = containers.MakeVector(tableInfoSchemaTypes[i], common.DebugAllocator)
			} else {
				vec = containers.ToTNVector(pkgVec, common.DebugAllocator)
			}
			bat.AddVector(tableInfoSchemaAttr[i], vec)
		}

		if id == int(TableInfoTypeIdx) {
			sm.RebuildTableInfo(bat)
		} else if id == int(SnapshotTidIdx) {
			sm.RebuildTid(bat)
		} else if id == int(PitrTidIdx) {
			sm.RebuildPitr(bat)
		} else {
			panic("unknown table info type")
		}
	}
	return nil
}

func (sm *SnapshotMeta) InitTableInfo(
	ctx context.Context,
	fs fileservice.FileService,
	data *CheckpointData,
	startts, endts types.TS,
) {
	sm.Lock()
	defer sm.Unlock()
	sm.updateTableInfo(ctx, fs, data, startts, endts)
}

func (sm *SnapshotMeta) TableInfoString() string {
	sm.RLock()
	defer sm.RUnlock()
	var buf bytes.Buffer
	for accID, tables := range sm.tables {
		buf.WriteString(fmt.Sprintf("accountID: %d\n", accID))
		for tid, table := range tables {
			buf.WriteString(fmt.Sprintf("tableID: %d, create: %s, deleteAt: %s\n",
				tid, table.createAt.ToString(), table.deleteAt.ToString()))
		}
	}
	return buf.String()
}

func (sm *SnapshotMeta) GetSnapshotListLocked(snapshotList map[uint32][]types.TS, tid uint64) []types.TS {
	if sm.tableIDIndex[tid] == nil {
		return nil
	}
	accID := sm.tableIDIndex[tid].accountID
	return snapshotList[accID]
}

// AccountToTableSnapshots returns a map from table id to its snapshots.
// The snapshotList is a map from account id to its snapshots.
// The pitr is the pitr info.
func (sm *SnapshotMeta) AccountToTableSnapshots(
	accountSnapshots map[uint32][]types.TS,
	pitr *PitrInfo,
) (
	tableSnapshots map[uint64][]types.TS,
	tablePitrs map[uint64]*types.TS,
) {
	tableSnapshots = make(map[uint64][]types.TS, 100)
	tablePitrs = make(map[uint64]*types.TS, 100)

	// 1. for system tables, flatten the accountSnapshots to tableSnapshots
	var flattenSnapshots []types.TS
	{
		var cnt int
		for _, tss := range accountSnapshots {
			cnt += len(tss)
		}
		flattenSnapshots = make([]types.TS, 0, cnt)

		for _, tss := range accountSnapshots {
			flattenSnapshots = append(flattenSnapshots, tss...)
		}
		flattenSnapshots = compute.SortAndDedup(
			flattenSnapshots,
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

	for tid, info := range sm.tableIDIndex {
		if catalog2.IsSystemTable(tid) {
			continue
		}
		// use the account snapshots as the table snapshots
		accountID := info.accountID
		tableSnapshots[tid] = accountSnapshots[accountID]

		// get the pitr for the table
		ts := pitr.GetTS(accountID, info.dbID, tid)
		tablePitrs[tid] = &ts
	}
	return
}

func (sm *SnapshotMeta) GetPitrByTable(
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
	accountSnapshots map[uint32][]types.TS,
	pitr *PitrInfo,
) error {
	sm.Lock()
	defer sm.Unlock()
	if len(sm.tables) == 0 {
		return nil
	}
	for accID, tables := range sm.tables {
		if accountSnapshots[accID] == nil && pitr.IsEmpty() {
			for _, table := range tables {
				if !table.deleteAt.IsEmpty() {
					delete(sm.tables[accID], table.tid)
					delete(sm.tableIDIndex, table.tid)
					if sm.objects[table.tid] != nil {
						delete(sm.objects, table.tid)
					}
				}
			}
			continue
		}
		for _, table := range tables {
			ts := sm.GetPitrByTable(pitr, table.dbID, table.tid)
			if !table.deleteAt.IsEmpty() &&
				!isSnapshotRefers(table, accountSnapshots[accID], ts) {
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

// for test
func (sm *SnapshotMeta) GetTablePK(tid uint64) string {
	sm.RLock()
	defer sm.RUnlock()
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
			logutil.Info(
				"isSnapshotRefers",
				zap.String("snap-ts", snapTS.ToString()),
				zap.String("create-ts", table.createAt.ToString()),
				zap.String("drop-ts", table.deleteAt.ToString()),
				zap.Uint64("tid", table.tid),
			)
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

func CloseSnapshotList(snapshots map[uint32]containers.Vector) {
	for _, snapshot := range snapshots {
		snapshot.Close()
	}
}
