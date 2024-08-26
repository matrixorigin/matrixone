// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logtail

import (
	"context"
	"fmt"
	"sort"
	"time"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
	"go.uber.org/zap"
)

const DefaultCheckpointBlockRows = 10000
const DefaultCheckpointSize = 512 * 1024 * 1024

const (
	CheckpointVersion12 uint32 = 12

	CheckpointCurrentVersion = CheckpointVersion12
)

const (
	MetaIDX uint16 = iota

	DBInsertIDX
	DBInsertTxnIDX
	DBDeleteIDX
	DBDeleteTxnIDX

	TBLInsertIDX
	TBLInsertTxnIDX
	TBLDeleteIDX
	TBLDeleteTxnIDX
	TBLColInsertIDX
	TBLColDeleteIDX

	TNMetaIDX

	// supporting `show accounts` by recording extra
	// account related info in checkpoint

	StorageUsageInsIDX

	ObjectInfoIDX

	StorageUsageDelIDX

	TombstoneObjectInfoIDX
)

const MaxIDX = TombstoneObjectInfoIDX + 1

const (
	Checkpoint_Meta_TID_IDX                  = 2
	Checkpoint_Meta_Insert_Block_LOC_IDX     = 3
	Checkpoint_Meta_Delete_Block_LOC_IDX     = 4
	Checkpoint_Meta_Data_Object_LOC_IDX      = 5
	Checkpoint_Meta_Tombstone_Object_LOC_IDX = 6
	Checkpoint_Meta_Usage_Ins_LOC_IDX        = 7
	Checkpoint_Meta_Usage_Del_LOC_IDX        = 8
)

type checkpointDataItem struct {
	schema *catalog.Schema
	types  []types.Type
	attrs  []string
}

var checkpointDataSchemas_V12 [MaxIDX]*catalog.Schema
var checkpointDataSchemas_Curr [MaxIDX]*catalog.Schema

var checkpointDataReferVersions map[uint32][MaxIDX]*checkpointDataItem

func init() {

	checkpointDataSchemas_V12 = [MaxIDX]*catalog.Schema{
		MetaSchema,
		catalog.SystemDBSchema,
		TxnNodeSchema,
		DBDelSchema, // 3
		DBTNSchema,
		catalog.SystemTableSchema,
		TblTNSchema,
		TblDelSchema, // 7
		TblTNSchema,
		catalog.SystemColumnSchema,
		ColumnDelSchema,
		TNMetaSchema, //11
		StorageUsageSchema,
		ObjectInfoSchema,
		StorageUsageSchema,
		ObjectInfoSchema, //15
	}

	checkpointDataReferVersions = make(map[uint32][MaxIDX]*checkpointDataItem)
	registerCheckpointDataReferVersion(CheckpointVersion12, checkpointDataSchemas_V12[:])
	checkpointDataSchemas_Curr = checkpointDataSchemas_V12
}

func IDXString(idx uint16) string {
	switch idx {
	case MetaIDX:
		return "MetaIDX"
	case DBInsertIDX:
		return "DBInsertIDX"
	case DBInsertTxnIDX:
		return "DBInsertTxnIDX"
	case DBDeleteIDX:
		return "DBDeleteIDX"
	case DBDeleteTxnIDX:
		return "DBDeleteTxnIDX"
	case TBLInsertIDX:
		return "TBLInsertIDX"
	case TBLInsertTxnIDX:
		return "TBLInsertTxnIDX"
	case TBLDeleteIDX:
		return "TBLDeleteIDX"
	case TBLDeleteTxnIDX:
		return "TBLDeleteTxnIDX"
	case TBLColInsertIDX:
		return "TBLColInsertIDX"
	case TBLColDeleteIDX:
		return "TBLColDeleteIDX"

	case TNMetaIDX:
		return "TNMetaIDX"

	case StorageUsageInsIDX:
		return "StorageUsageInsIDX"

	case ObjectInfoIDX:
		return "ObjectInfoIDX"
	case TombstoneObjectInfoIDX:
		return "TombstoneObjectInfoIDX"
	case StorageUsageDelIDX:
		return "StorageUsageDelIDX"
	default:
		return fmt.Sprintf("UnknownIDX(%d)", idx)
	}
}

func registerCheckpointDataReferVersion(version uint32, schemas []*catalog.Schema) {
	var checkpointDataRefer [MaxIDX]*checkpointDataItem
	for idx, schema := range schemas {
		checkpointDataRefer[idx] = &checkpointDataItem{
			schema,
			append(BaseTypes, schema.Types()...),
			append(BaseAttr, schema.Attrs()...),
		}
	}
	checkpointDataReferVersions[version] = checkpointDataRefer
}

func IncrementalCheckpointDataFactory(
	sid string,
	start, end types.TS,
	collectUsage bool,
) func(c *catalog.Catalog) (*CheckpointData, error) {
	return func(c *catalog.Catalog) (data *CheckpointData, err error) {
		collector := NewIncrementalCollector(sid, start, end)
		defer collector.Close()
		err = c.RecurLoop(collector)
		if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
			err = nil
		}
		if err != nil {
			return
		}

		if collectUsage {
			collector.UsageMemo = c.GetUsageMemo().(*TNUsageMemo)
			// collecting usage happens only when do ckp
			FillUsageBatOfIncremental(collector)
		}

		data = collector.OrphanData()
		return
	}
}

func BackupCheckpointDataFactory(
	sid string,
	start, end types.TS,
) func(c *catalog.Catalog) (*CheckpointData, error) {
	return func(c *catalog.Catalog) (data *CheckpointData, err error) {
		collector := NewBackupCollector(sid, start, end)
		defer collector.Close()
		err = c.RecurLoop(collector)
		if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
			err = nil
		}
		data = collector.OrphanData()
		return
	}
}

func GlobalCheckpointDataFactory(
	sid string,
	end types.TS,
	versionInterval time.Duration,
) func(c *catalog.Catalog) (*CheckpointData, error) {
	return func(c *catalog.Catalog) (data *CheckpointData, err error) {
		collector := NewGlobalCollector(sid, end, versionInterval)
		defer collector.Close()
		err = c.RecurLoop(collector)
		if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
			err = nil
		}

		if err != nil {
			return
		}
		collector.UsageMemo = c.GetUsageMemo().(*TNUsageMemo)
		FillUsageBatOfGlobal(collector)

		data = collector.OrphanData()

		return
	}
}

type BlockLocationsIterator struct {
	offset int
	*BlockLocations
}

func (i *BlockLocationsIterator) HasNext() bool {
	return i.offset < len(*i.BlockLocations)
}

func (i *BlockLocationsIterator) Next() BlockLocation {
	loc := (BlockLocation)(*i.BlockLocations)[i.offset : i.offset+BlockLocationLength]
	i.offset += BlockLocationLength
	return loc
}

type BlockLocations []byte

func NewEmptyBlockLocations() BlockLocations {
	return make([]byte, 0)
}

func (l *BlockLocations) Append(loc BlockLocation) {
	*l = append(*l, loc...)
}

func (l BlockLocations) MakeIterator() *BlockLocationsIterator {
	return &BlockLocationsIterator{
		offset:         0,
		BlockLocations: &l,
	}
}
func (l BlockLocations) String() string {
	s := ""
	it := l.MakeIterator()
	for it.HasNext() {
		loc := it.Next()
		s += loc.String()
	}
	return s
}

// func (l BlockLocations) append iterator
/*
Location is a fixed-length unmodifiable byte array.
Layout:  Location(objectio.Location) | StartOffset(uint64) | EndOffset(uint64)
*/
const (
	LocationOffset      = 0
	LocationLength      = objectio.LocationLen
	StartOffsetOffset   = LocationOffset + LocationLength
	StartOffsetLength   = 8
	EndOffsetOffset     = StartOffsetOffset + StartOffsetLength
	EndOffsetLength     = 8
	BlockLocationLength = EndOffsetOffset + EndOffsetLength
)

type BlockLocation []byte

func BuildBlockLoaction(id uint16, start, end uint64) BlockLocation {
	buf := make([]byte, BlockLocationLength)
	copy(buf[StartOffsetOffset:StartOffsetOffset+StartOffsetLength], types.EncodeUint64(&start))
	copy(buf[EndOffsetOffset:EndOffsetOffset+EndOffsetLength], types.EncodeUint64(&end))
	blkLoc := BlockLocation(buf)
	blkLoc.SetID(id)
	return buf
}
func BuildBlockLoactionWithLocation(name objectio.ObjectName, extent objectio.Extent, rows uint32, id uint16, start, end uint64) BlockLocation {
	buf := make([]byte, BlockLocationLength)
	location := objectio.BuildLocation(name, extent, rows, id)
	copy(buf[LocationOffset:LocationOffset+LocationLength], location)
	copy(buf[StartOffsetOffset:StartOffsetOffset+StartOffsetLength], types.EncodeUint64(&start))
	copy(buf[EndOffsetOffset:EndOffsetOffset+EndOffsetLength], types.EncodeUint64(&end))
	return buf
}
func (l BlockLocation) GetID() uint16 {
	return l.GetLocation().ID()
}
func (l BlockLocation) GetLocation() objectio.Location {
	return (objectio.Location)(l[LocationOffset : LocationOffset+LocationLength])
}
func (l BlockLocation) GetStartOffset() uint64 {
	return types.DecodeUint64(l[StartOffsetOffset : StartOffsetOffset+StartOffsetLength])
}
func (l BlockLocation) GetEndOffset() uint64 {
	return types.DecodeUint64(l[EndOffsetOffset : EndOffsetOffset+EndOffsetLength])
}
func (l BlockLocation) SetID(id uint16) {
	l.GetLocation().SetID(id)
}
func (l BlockLocation) SetLocation(location objectio.Location) {
	copy(l[LocationOffset:LocationOffset+LocationLength], location)
}
func (l BlockLocation) SetStartOffset(start uint64) {
	copy(l[StartOffsetOffset:StartOffsetOffset+StartOffsetLength], types.EncodeUint64(&start))
}
func (l BlockLocation) SetEndOffset(end uint64) {
	copy(l[EndOffsetOffset:EndOffsetOffset+EndOffsetLength], types.EncodeUint64(&end))
}
func (l BlockLocation) Contains(i common.ClosedInterval) bool {
	return l.GetStartOffset() <= i.Start && l.GetEndOffset() >= i.End
}

func (l BlockLocation) String() string {
	if len(l) == 0 {
		return ""
	}
	if len(l) != BlockLocationLength {
		return string(l)
	}
	return fmt.Sprintf("%v_start:%d_end:%d", l.GetLocation().String(), l.GetStartOffset(), l.GetEndOffset())
}

type TableMeta struct {
	common.ClosedInterval
	locations BlockLocations
}

const (
	BlockInsert = iota
	BlockDelete
	DataObject
	TombstoneObject
	StorageUsageIns
	StorageUsageDel
)

func (m *TableMeta) String() string {
	if m == nil {
		return ""
	}
	return fmt.Sprintf("interval:%v, locations:%v", m.ClosedInterval, m.locations)
}

const MetaMaxIdx = StorageUsageDel + 1

type CheckpointMeta struct {
	tables [MetaMaxIdx]*TableMeta
}

func NewCheckpointMeta() *CheckpointMeta {
	return &CheckpointMeta{}
}

func NewTableMeta() *TableMeta {
	return &TableMeta{}
}

func (m *CheckpointMeta) DecodeFromString(keys [][]byte) (err error) {
	for i, key := range keys {
		tableMate := NewTableMeta()
		tableMate.locations = key
		m.tables[i] = tableMate
	}
	return
}
func (m *CheckpointMeta) String() string {
	s := ""
	if m == nil {
		return "nil"
	}
	for idx, table := range m.tables {
		s += fmt.Sprintf("idx:%d, table:%s\n", idx, table)
	}
	return s
}

type CheckpointData struct {
	sid       string
	meta      map[uint64]*CheckpointMeta
	locations map[string]objectio.Location
	bats      [MaxIDX]*containers.Batch
	allocator *mpool.MPool
}

func NewCheckpointData(
	sid string,
	mp *mpool.MPool,
) *CheckpointData {
	data := &CheckpointData{
		sid:       sid,
		meta:      make(map[uint64]*CheckpointMeta),
		allocator: mp,
	}
	for idx, schema := range checkpointDataSchemas_Curr {
		data.bats[idx] = makeRespBatchFromSchema(schema, mp)
	}
	return data
}

// for test
func NewCheckpointDataWithVersion(ver uint32, mp *mpool.MPool) *CheckpointData {
	data := &CheckpointData{
		meta:      make(map[uint64]*CheckpointMeta),
		allocator: mp,
	}

	for idx, item := range checkpointDataReferVersions[ver] {
		data.bats[idx] = makeRespBatchFromSchema(item.schema, mp)
	}
	return data
}

type BaseCollector struct {
	*catalog.LoopProcessor
	start, end types.TS

	data                *CheckpointData
	skipLoadObjectStats bool

	// to prefetch object meta when fill in object info batch

	// true for prefech object meta
	isPrefetch bool

	Objects []*catalog.ObjectEntry
	// for storage usage
	Usage struct {
		// db, tbl deletes
		Deletes        []interface{}
		ObjInserts     []*catalog.ObjectEntry
		ObjDeletes     []*catalog.ObjectEntry
		ReservedAccIds map[uint64]struct{}
	}

	UsageMemo *TNUsageMemo
}

type IncrementalCollector struct {
	*BaseCollector
}

func NewIncrementalCollector(
	sid string,
	start, end types.TS,
) *IncrementalCollector {
	collector := &IncrementalCollector{
		BaseCollector: &BaseCollector{
			LoopProcessor: new(catalog.LoopProcessor),
			data:          NewCheckpointData(sid, common.CheckpointAllocator),
			start:         start,
			end:           end,
		},
	}
	collector.DatabaseFn = collector.VisitDB
	collector.TableFn = collector.VisitTable
	collector.ObjectFn = collector.VisitObj
	collector.TombstoneFn = collector.VisitObj
	return collector
}

func NewBackupCollector(
	sid string,
	start, end types.TS) *IncrementalCollector {
	collector := &IncrementalCollector{
		BaseCollector: &BaseCollector{
			LoopProcessor: new(catalog.LoopProcessor),
			data:          NewCheckpointData(sid, common.CheckpointAllocator),
			start:         start,
			end:           end,
		},
	}
	// TODO
	collector.TombstoneFn = collector.VisitObjForBackup
	collector.ObjectFn = collector.VisitObjForBackup
	return collector
}

type GlobalCollector struct {
	*BaseCollector
	versionThershold types.TS
}

func NewGlobalCollector(
	sid string,
	end types.TS,
	versionInterval time.Duration,
) *GlobalCollector {
	versionThresholdTS := types.BuildTS(end.Physical()-versionInterval.Nanoseconds(), end.Logical())
	collector := &GlobalCollector{
		BaseCollector: &BaseCollector{
			LoopProcessor: new(catalog.LoopProcessor),
			data:          NewCheckpointData(sid, common.CheckpointAllocator),
			end:           end,
		},
		versionThershold: versionThresholdTS,
	}

	collector.DatabaseFn = collector.VisitDB
	collector.TableFn = collector.VisitTable
	collector.ObjectFn = collector.VisitObj
	collector.TombstoneFn = collector.VisitObj

	collector.Usage.ReservedAccIds = make(map[uint64]struct{})

	return collector
}

func (data *CheckpointData) ApplyReplayTo(
	c *catalog.Catalog,
	dataFactory catalog.DataFactory,
) (err error) {
	c.OnReplayDatabaseBatch(data.GetDBBatchs())
	ins, colins, tnins, del, tndel := data.GetTblBatchs()
	c.OnReplayTableBatch(ins, colins, tnins, del, tndel, dataFactory)
	objectInfo := data.GetTombstoneObjectBatchs()
	c.OnReplayObjectBatch(objectInfo, true, dataFactory)
	objectInfo = data.GetObjectBatchs()
	c.OnReplayObjectBatch(objectInfo, false, dataFactory)
	return
}

type CNCheckpointData struct {
	sid  string
	meta map[uint64]*CheckpointMeta
	bats [MaxIDX]*batch.Batch
}

func NewCNCheckpointData(sid string) *CNCheckpointData {
	return &CNCheckpointData{
		sid:  sid,
		meta: make(map[uint64]*CheckpointMeta),
	}
}

// checkpoint table meta idx to ckp batch idx
func switchCheckpointIdx(i uint16, _ uint64) uint16 {
	idx := uint16(i)

	if i == DataObject {
		idx = ObjectInfoIDX
	} else if i == TombstoneObject {
		idx = TombstoneObjectInfoIDX
	} else if i == StorageUsageIns {
		idx = StorageUsageInsIDX
	} else if i == StorageUsageDel {
		idx = StorageUsageDelIDX
	}
	// switch tableID {
	// case pkgcatalog.MO_DATABASE_ID:
	// 	if i == BlockInsert {
	// 		idx = DBInsertIDX
	// 	} else if i == BlockDelete {
	// 		idx = DBDeleteIDX
	// 	}
	// case pkgcatalog.MO_TABLES_ID:
	// 	if i == BlockInsert {
	// 		idx = TBLInsertIDX
	// 	} else if i == BlockDelete {
	// 		idx = TBLDeleteIDX
	// 	}
	// case pkgcatalog.MO_COLUMNS_ID:
	// 	if i == BlockInsert {
	// 		idx = TBLColInsertIDX
	// 	} else if i == BlockDelete {
	// 		idx = TBLColDeleteIDX
	// 	}
	// }
	return idx
}

func (data *CNCheckpointData) InitMetaIdx(
	ctx context.Context, version uint32, reader *blockio.BlockReader,
	location objectio.Location, m *mpool.MPool,
) error {
	if data.bats[MetaIDX] == nil {
		metaIdx := checkpointDataReferVersions[version][MetaIDX]
		metaBats, err := LoadCNSubBlkColumnsByMeta(version, ctx, metaIdx.types, metaIdx.attrs, MetaIDX, reader, m)
		if err != nil {
			return err
		}
		data.bats[MetaIDX] = metaBats[0]
	}
	return nil
}

func (data *CNCheckpointData) PrefetchMetaIdx(
	ctx context.Context,
	version uint32,
	idxes []uint16,
	key objectio.Location,
	service fileservice.FileService,
) (err error) {
	var pref blockio.PrefetchParams
	pref, err = blockio.BuildPrefetchParams(service, key)
	if err != nil {
		return
	}
	pref.AddBlockWithType(idxes, []uint16{0}, uint16(objectio.ConvertToSchemaType(MetaIDX)))

	return blockio.PrefetchWithMerged(data.sid, pref)
}

func (data *CNCheckpointData) PrefetchMetaFrom(
	ctx context.Context,
	version uint32,
	location objectio.Location,
	service fileservice.FileService,
	tableID uint64,
) (err error) {
	meta := data.GetTableMeta(tableID, version, location)
	if meta == nil {
		return
	}
	locations := make(map[string]objectio.Location)
	for _, table := range meta.tables {
		if table == nil {
			continue
		}
		it := table.locations.MakeIterator()
		for it.HasNext() {
			block := it.Next()
			name := block.GetLocation().Name().String()
			if locations[name] != nil {
				locations[name] = block.GetLocation()
			}
		}
	}
	for _, location := range locations {
		err = blockio.PrefetchMeta(data.sid, service, location)
	}
	return err
}

func (data *CNCheckpointData) PrefetchFrom(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
	key objectio.Location,
	tableID uint64,
) (err error) {
	// if version < CheckpointVersion4 {
	// 	return prefetchCheckpointData(ctx, version, service, key)
	// }
	meta := data.GetTableMeta(tableID, version, key)
	if meta == nil {
		return
	}
	// for ver less than 5, some tablemeta is empty
	empty := true
	files := make(map[string]*blockio.PrefetchParams)
	for i, table := range meta.tables {
		if table == nil {
			continue
		}

		idx := switchCheckpointIdx(uint16(i), tableID)
		schema := checkpointDataReferVersions[version][uint32(idx)]
		idxes := make([]uint16, len(schema.attrs))
		for attr := range schema.attrs {
			idxes[attr] = uint16(attr)
		}
		it := table.locations.MakeIterator()
		for it.HasNext() {
			block := it.Next()
			location := block.GetLocation()
			if files[location.Name().String()] == nil {
				var pref blockio.PrefetchParams
				pref, err = blockio.BuildPrefetchParams(service, location)
				if err != nil {
					return
				}
				files[location.Name().String()] = &pref
			}
			pref := *files[location.Name().String()]
			pref.AddBlockWithType(idxes, []uint16{block.GetID()}, uint16(objectio.ConvertToSchemaType(idx)))
			empty = false
		}
	}
	if empty {
		return
	}
	for _, pref := range files {
		err = blockio.PrefetchWithMerged(data.sid, *pref)
		if err != nil {
			return
		}
	}
	return nil
}

func (data *CNCheckpointData) isMOCatalogTables(tid uint64) bool {
	return tid == pkgcatalog.MO_DATABASE_ID || tid == pkgcatalog.MO_TABLES_ID || tid == pkgcatalog.MO_COLUMNS_ID
}
func (data *CNCheckpointData) GetTableMeta(tableID uint64, version uint32, loc objectio.Location) (meta *CheckpointMeta) {
	if len(data.meta) != 0 {
		meta = data.meta[tableID]
		return
	}
	tidVec := vector.MustFixedCol[uint64](data.bats[MetaIDX].Vecs[Checkpoint_Meta_TID_IDX])
	dataObj := data.bats[MetaIDX].Vecs[Checkpoint_Meta_Data_Object_LOC_IDX]
	tombstoneObj := data.bats[MetaIDX].Vecs[Checkpoint_Meta_Tombstone_Object_LOC_IDX]

	var usageInsVec, usageDelVec *vector.Vector
	usageInsVec = data.bats[MetaIDX].Vecs[Checkpoint_Meta_Usage_Ins_LOC_IDX]
	usageDelVec = data.bats[MetaIDX].Vecs[Checkpoint_Meta_Usage_Del_LOC_IDX]

	var i int
	i = vector.OrderedFindFirstIndexInSortedSlice[uint64](tableID, tidVec)
	if i < 0 {
		return
	}
	tid := tidVec[i]
	dataObjStr := dataObj.GetBytesAt(i)
	tombstoneObjStr := tombstoneObj.GetBytesAt(i)
	tableMeta := NewCheckpointMeta()
	if len(dataObjStr) > 0 {
		dataObjectTableMeta := NewTableMeta()
		dataObjectTableMeta.locations = dataObjStr
		tableMeta.tables[DataObject] = dataObjectTableMeta
	}
	if len(tombstoneObjStr) > 0 {
		tombstoneObjectTableMeta := NewTableMeta()
		tombstoneObjectTableMeta.locations = tombstoneObjStr
		tableMeta.tables[TombstoneObject] = tombstoneObjectTableMeta
	}

	if usageInsVec != nil {
		usageInsTableMeta := NewTableMeta()
		usageDelTableMeta := NewTableMeta()

		usageInsTableMeta.locations = usageInsVec.GetBytesAt(i)
		usageDelTableMeta.locations = usageDelVec.GetBytesAt(i)

		tableMeta.tables[StorageUsageIns] = usageInsTableMeta
		tableMeta.tables[StorageUsageDel] = usageDelTableMeta
	}

	data.meta[tid] = tableMeta
	meta = data.meta[tableID]
	return
}

func (data *CNCheckpointData) ReadFromData(
	ctx context.Context,
	tableID uint64,
	location objectio.Location,
	reader *blockio.BlockReader,
	version uint32,
	m *mpool.MPool,
) (dataBats []*batch.Batch, err error) {
	meta := data.GetTableMeta(tableID, version, location)
	if meta == nil {
		return
	}
	dataBats = make([]*batch.Batch, MetaMaxIdx)
	for i, table := range meta.tables {
		if table == nil {
			continue
		}

		idx := switchCheckpointIdx(uint16(i), tableID)
		it := table.locations.MakeIterator()
		for it.HasNext() {
			block := it.Next()
			var bat *batch.Batch
			schema := checkpointDataReferVersions[version][uint32(idx)]
			reader, err = blockio.NewObjectReader(data.sid, reader.GetObjectReader().GetObject().GetFs(), block.GetLocation())
			if err != nil {
				return
			}
			bat, err = LoadCNSubBlkColumnsByMetaWithId(ctx, schema.types, schema.attrs, uint16(idx), block.GetID(), version, reader, m)
			if err != nil {
				return
			}
			if block.GetEndOffset() == 0 {
				continue
			}
			windowCNBatch(bat, block.GetStartOffset(), block.GetEndOffset())
			if dataBats[uint32(i)] == nil {
				cnBatch := batch.NewWithSize(len(bat.Vecs))
				cnBatch.Attrs = make([]string, len(bat.Attrs))
				copy(cnBatch.Attrs, bat.Attrs)
				for n := range cnBatch.Vecs {
					cnBatch.Vecs[n] = vector.NewVec(*bat.Vecs[n].GetType())
					if err = cnBatch.Vecs[n].UnionBatch(bat.Vecs[n], 0, bat.Vecs[n].Length(), nil, m); err != nil {
						return
					}
				}
				dataBats[uint32(i)] = cnBatch
			} else {
				dataBats[uint32(i)], err = dataBats[uint32(i)].Append(ctx, m, bat)
				if err != nil {
					return
				}
			}

		}
	}

	return
}

func (data *CNCheckpointData) GetTableDataFromBats(tid uint64, bats []*batch.Batch) (ins, del, dataObject, tombstoneObject *api.Batch, err error) {
	var dataObjectTaeBat, tombstoneObjectTaeBat *batch.Batch
	if len(bats) == 0 {
		return
	}
	// if tid == pkgcatalog.MO_DATABASE_ID || tid == pkgcatalog.MO_TABLES_ID || tid == pkgcatalog.MO_COLUMNS_ID {
	// 	insTaeBat = bats[BlockInsert]
	// 	delTaeBat = bats[BlockDelete]
	// 	if insTaeBat != nil {
	// 		ins, err = batch.BatchToProtoBatch(insTaeBat)
	// 		if err != nil {
	// 			return
	// 		}
	// 	}
	// 	if delTaeBat != nil {
	// 		del, err = batch.BatchToProtoBatch(delTaeBat)
	// 		if err != nil {
	// 			return
	// 		}
	// 	}
	// 	return
	// }

	dataObjectTaeBat = bats[DataObject]
	if dataObjectTaeBat != nil {
		dataObject, err = batch.BatchToProtoBatch(dataObjectTaeBat)
		if err != nil {
			return
		}
	}
	tombstoneObjectTaeBat = bats[TombstoneObject]
	if tombstoneObjectTaeBat != nil {
		tombstoneObject, err = batch.BatchToProtoBatch(tombstoneObjectTaeBat)
		if err != nil {
			return
		}
	}
	return
}

func (data *CNCheckpointData) GetCloseCB(version uint32, m *mpool.MPool) func() {
	return nil
}

func (data *CNCheckpointData) closeVector(batIdx uint16, colIdx int, m *mpool.MPool) {
	bat := data.bats[batIdx]
	if bat == nil {
		return
	}
	if len(bat.Vecs) <= colIdx {
		return
	}
	vec := data.bats[batIdx].Vecs[colIdx]
	vec.Free(m)

}

func windowCNBatch(bat *batch.Batch, start, end uint64) {
	var err error
	for i, vec := range bat.Vecs {
		bat.Vecs[i], err = vec.Window(int(start), int(end))
		if err != nil {
			panic(err)
		}
	}
}

func (data *CheckpointData) Allocator() *mpool.MPool { return data.allocator }

func (data *CheckpointData) prepareMeta() {
	bat := data.bats[MetaIDX]
	blkInsLoc := bat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchLocation).GetDownstreamVector()
	blkDelLoc := bat.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchLocation).GetDownstreamVector()
	dataObjectLoc := bat.GetVectorByName(SnapshotMetaAttr_DataObjectBatchLocation).GetDownstreamVector()
	tombstoneObjectLoc := bat.GetVectorByName(SnapshotMetaAttr_TombstoneObjectBatchLocation).GetDownstreamVector()
	tidVec := bat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector()
	usageInsLoc := bat.GetVectorByName(CheckpointMetaAttr_StorageUsageInsLocation).GetDownstreamVector()
	usageDelLoc := bat.GetVectorByName(CheckpointMetaAttr_StorageUsageDelLocation).GetDownstreamVector()

	sortMeta := make([]int, 0)
	for tid := range data.meta {
		sortMeta = append(sortMeta, int(tid))
	}
	sort.Ints(sortMeta)
	for _, tid := range sortMeta {
		vector.AppendFixed[uint64](tidVec, uint64(tid), false, data.allocator)
		if data.meta[uint64(tid)].tables[BlockInsert] == nil {
			vector.AppendBytes(blkInsLoc, nil, true, data.allocator)
		} else {
			vector.AppendBytes(blkInsLoc, []byte(data.meta[uint64(tid)].tables[BlockInsert].locations), false, data.allocator)
		}
		if data.meta[uint64(tid)].tables[BlockDelete] == nil {
			vector.AppendBytes(blkDelLoc, nil, true, data.allocator)
		} else {
			vector.AppendBytes(blkDelLoc, []byte(data.meta[uint64(tid)].tables[BlockDelete].locations), false, data.allocator)
		}
		if data.meta[uint64(tid)].tables[DataObject] == nil {
			vector.AppendBytes(dataObjectLoc, nil, true, data.allocator)
		} else {
			vector.AppendBytes(dataObjectLoc, []byte(data.meta[uint64(tid)].tables[DataObject].locations), false, data.allocator)
		}
		if data.meta[uint64(tid)].tables[TombstoneObject] == nil {
			vector.AppendBytes(tombstoneObjectLoc, nil, true, data.allocator)
		} else {
			vector.AppendBytes(tombstoneObjectLoc, []byte(data.meta[uint64(tid)].tables[TombstoneObject].locations), false, data.allocator)
		}

		if data.meta[uint64(tid)].tables[StorageUsageIns] == nil {
			vector.AppendBytes(usageInsLoc, nil, true, data.allocator)
		} else {
			vector.AppendBytes(usageInsLoc, data.meta[uint64(tid)].tables[StorageUsageIns].locations, false, data.allocator)
		}

		if data.meta[uint64(tid)].tables[StorageUsageDel] == nil {
			vector.AppendBytes(usageDelLoc, nil, true, data.allocator)
		} else {
			vector.AppendBytes(usageDelLoc, data.meta[uint64(tid)].tables[StorageUsageDel].locations, false, data.allocator)
		}
	}
}
func (data *CheckpointData) updateTableMeta(tid uint64, metaIdx int, start, end int32) {
	meta, ok := data.meta[tid]
	if !ok {
		meta = NewCheckpointMeta()
		data.meta[tid] = meta
	}

	if end > start {
		if meta.tables[metaIdx] == nil {
			meta.tables[metaIdx] = NewTableMeta()
			meta.tables[metaIdx].Start = uint64(start)
			meta.tables[metaIdx].End = uint64(end)
		} else {
			if !meta.tables[metaIdx].TryMerge(common.ClosedInterval{Start: uint64(start), End: uint64(end)}) {
				panic(fmt.Sprintf("logic error interval %v, start %d, end %d", meta.tables[metaIdx].ClosedInterval, start, end))
			}
		}
	}
}

func (data *CheckpointData) UpdateBlkMeta(tid uint64, insStart, insEnd, delStart, delEnd int32) {
	if delEnd <= delStart && insEnd <= insStart {
		return
	}
	data.updateTableMeta(tid, BlockInsert, insStart, insEnd)
	data.updateTableMeta(tid, BlockDelete, delStart, delEnd)
}

func (data *CheckpointData) UpdateDataObjectMeta(tid uint64, delStart, delEnd int32) {
	if delEnd <= delStart {
		return
	}
	data.updateTableMeta(tid, DataObject, delStart, delEnd)
}

func (data *CheckpointData) UpdateTombstoneObjectMeta(tid uint64, delStart, delEnd int32) {
	if delEnd <= delStart {
		return
	}
	data.updateTableMeta(tid, TombstoneObject, delStart, delEnd)
}

func (data *CheckpointData) UpdateObjectInsertMeta(tid uint64, delStart, delEnd int32) {
	if delEnd <= delStart {
		return
	}
	data.resetTableMeta(tid, DataObject, delStart, delEnd)
}

func (data *CheckpointData) UpdateTombstoneInsertMeta(tid uint64, delStart, delEnd int32) {
	if delEnd <= delStart {
		return
	}
	data.resetTableMeta(tid, TombstoneObject, delStart, delEnd)
}

func (data *CheckpointData) resetTableMeta(tid uint64, metaIdx int, start, end int32) {
	meta, ok := data.meta[tid]
	if !ok {
		meta = NewCheckpointMeta()
		data.meta[tid] = meta
	}
	if end > start {
		if meta.tables[metaIdx] == nil {
			meta.tables[metaIdx] = NewTableMeta()
			meta.tables[metaIdx].Start = uint64(start)
			meta.tables[metaIdx].End = uint64(end)
		} else {
			meta.tables[metaIdx].Start = uint64(start)
			meta.tables[metaIdx].End = uint64(end)
			return
		}
	}
}

func (data *CheckpointData) UpdateBlockInsertBlkMeta(tid uint64, insStart, insEnd int32) {
	if insEnd <= insStart {
		return
	}
	data.resetTableMeta(tid, BlockInsert, insStart, insEnd)
}

func (data *CheckpointData) UpdateBlockDeleteBlkMeta(tid uint64, insStart, insEnd int32) {
	if insEnd <= insStart {
		return
	}
	data.resetTableMeta(tid, BlockDelete, insStart, insEnd)
}

func formatBatch(bat *containers.Batch) {
	length := bat.Length()
	if length == 0 {
		for _, vec := range bat.Vecs {
			if vec.Length() > 0 {
				length = vec.Length()
				break
			}
		}
		if length == 0 {
			return
		}
	}
	for i := range bat.Vecs {
		if bat.Vecs[i].Length() == 0 {
			bat.Vecs[i] = containers.NewConstNullVector(*bat.Vecs[i].GetType(), length, common.DefaultAllocator)
		}
	}
}

func (data *CheckpointData) prepareTNMetaBatch(
	checkpointNames []objectio.ObjectName,
	objectBlocks [][]objectio.BlockObject,
	schemaTypes [][]uint16,
) {
	for i, blks := range objectBlocks {
		for y, blk := range blks {
			location := objectio.BuildLocation(checkpointNames[i], blk.GetExtent(), 0, blk.GetID())
			data.bats[TNMetaIDX].GetVectorByName(CheckpointMetaAttr_BlockLocation).Append([]byte(location), false)
			data.bats[TNMetaIDX].GetVectorByName(CheckpointMetaAttr_SchemaType).Append(schemaTypes[i][y], false)
		}
	}
}

func (data *CheckpointData) FormatData(mp *mpool.MPool) (err error) {
	for idx := range data.bats {
		for i, col := range data.bats[idx].Vecs {
			vec := col.CloneWindow(0, col.Length(), mp)
			col.Close()
			data.bats[idx].Vecs[i] = vec
		}
	}
	data.bats[MetaIDX].Close()
	data.bats[TNMetaIDX].Close()
	data.bats[MetaIDX] = makeRespBatchFromSchema(checkpointDataSchemas_Curr[MetaIDX], mp)
	data.bats[TNMetaIDX] = makeRespBatchFromSchema(checkpointDataSchemas_Curr[TNMetaIDX], mp)
	for tid := range data.meta {
		for idx := range data.meta[tid].tables {
			if data.meta[tid].tables[idx] != nil {
				location := data.meta[tid].tables[idx].locations.MakeIterator()
				if location.HasNext() {
					loc := location.Next()
					if data.meta[tid].tables[idx].Start == 0 && data.meta[tid].tables[idx].End == 0 {
						data.meta[tid].tables[idx].Start = loc.GetStartOffset()
						data.meta[tid].tables[idx].End = loc.GetEndOffset()
					} else {
						data.meta[tid].tables[idx].TryMerge(common.ClosedInterval{Start: loc.GetStartOffset(), End: loc.GetEndOffset()})
					}
				}
				data.meta[tid].tables[idx].locations = make([]byte, 0)
			}
		}
	}
	return
}

type blockIndexes struct {
	fileNum uint16
	indexes *BlockLocation
}

func (data *CheckpointData) WriteTo(
	fs fileservice.FileService,
	blockRows int,
	checkpointSize int,
) (CNLocation, TNLocation objectio.Location, checkpointFiles []string, err error) {
	checkpointNames := make([]objectio.ObjectName, 1)
	segmentid := objectio.NewSegmentid()
	fileNum := uint16(0)
	name := objectio.BuildObjectName(segmentid, fileNum)
	writer, err := blockio.NewBlockWriterNew(fs, name, 0, nil)
	if err != nil {
		return
	}
	checkpointNames[0] = name
	objectBlocks := make([][]objectio.BlockObject, 0)
	indexes := make([][]blockIndexes, MaxIDX)
	schemas := make([][]uint16, 0)
	schemaTypes := make([]uint16, 0)
	checkpointFiles = make([]string, 0)
	var objectSize int
	for i := range checkpointDataSchemas_Curr {
		if i == int(MetaIDX) || i == int(TNMetaIDX) {
			continue
		}
		offset := 0
		formatBatch(data.bats[i])
		var block objectio.BlockObject
		var bat *containers.Batch
		var size int
		var blks []objectio.BlockObject
		if objectSize > checkpointSize {
			fileNum++
			blks, _, err = writer.Sync(context.Background())
			if err != nil {
				return
			}
			checkpointFiles = append(checkpointFiles, name.String())
			name = objectio.BuildObjectName(segmentid, fileNum)
			writer, err = blockio.NewBlockWriterNew(fs, name, 0, nil)
			if err != nil {
				return
			}
			checkpointNames = append(checkpointNames, name)
			objectBlocks = append(objectBlocks, blks)
			schemas = append(schemas, schemaTypes)
			schemaTypes = make([]uint16, 0)
			objectSize = 0
		}
		if data.bats[i].Length() == 0 {
			if block, size, err = writer.WriteSubBatch(containers.ToCNBatch(data.bats[i]), objectio.ConvertToSchemaType(uint16(i))); err != nil {
				return
			}
			blockLoc := BuildBlockLoaction(block.GetID(), uint64(offset), uint64(0))
			indexes[i] = append(indexes[i], blockIndexes{
				fileNum: fileNum,
				indexes: &blockLoc,
			})
			schemaTypes = append(schemaTypes, uint16(i))
			objectSize += size
		} else {
			split := containers.NewBatchSplitter(data.bats[i], blockRows)
			for {
				bat, err = split.Next()
				if err != nil {
					break
				}
				defer bat.Close()
				if block, size, err = writer.WriteSubBatch(containers.ToCNBatch(bat), objectio.ConvertToSchemaType(uint16(i))); err != nil {
					return
				}
				Endoffset := offset + bat.Length()
				blockLoc := BuildBlockLoaction(block.GetID(), uint64(offset), uint64(Endoffset))
				indexes[i] = append(indexes[i], blockIndexes{
					fileNum: fileNum,
					indexes: &blockLoc,
				})
				schemaTypes = append(schemaTypes, uint16(i))
				offset += bat.Length()
				objectSize += size
			}
		}
	}
	blks, _, err := writer.Sync(context.Background())
	if err != nil {
		return
	}
	checkpointFiles = append(checkpointFiles, name.String())
	schemas = append(schemas, schemaTypes)
	objectBlocks = append(objectBlocks, blks)

	data.prepareTNMetaBatch(checkpointNames, objectBlocks, schemas)

	for tid, mata := range data.meta {
		for i, table := range mata.tables {
			if table == nil || table.ClosedInterval.Start == table.ClosedInterval.End {
				continue
			}

			// if i > BlockDelete {
			// 	if tid == pkgcatalog.MO_DATABASE_ID ||
			// 		tid == pkgcatalog.MO_TABLES_ID ||
			// 		tid == pkgcatalog.MO_COLUMNS_ID {
			// 		break
			// 	}
			// }
			idx := switchCheckpointIdx(uint16(i), tid)
			for _, blockIdx := range indexes[idx] {
				block := blockIdx.indexes
				name = checkpointNames[blockIdx.fileNum]
				if table.End <= block.GetStartOffset() {
					break
				}
				if table.Start >= block.GetEndOffset() {
					continue
				}
				blks = objectBlocks[blockIdx.fileNum]
				//blockLoc1 := objectio.BuildLocation(name, blks[block.GetID()].GetExtent(), 0, block.GetID())
				//logutil.Infof("write block %v to %d-%d, table is %d-%d", blockLoc1.String(), block.GetStartOffset(), block.GetEndOffset(), table.Start, table.End)
				if table.Uint64Contains(block.GetStartOffset(), block.GetEndOffset()) {
					blockLoc := BuildBlockLoactionWithLocation(
						name, blks[block.GetID()].GetExtent(), 0, block.GetID(),
						0, block.GetEndOffset()-block.GetStartOffset())
					table.locations.Append(blockLoc)
				} else if block.Contains(table.ClosedInterval) {
					blockLoc := BuildBlockLoactionWithLocation(
						name, blks[block.GetID()].GetExtent(), 0, block.GetID(),
						table.Start-block.GetStartOffset(), table.End-block.GetStartOffset())
					table.locations.Append(blockLoc)
				} else if table.Start <= block.GetEndOffset() && table.Start >= block.GetStartOffset() {
					blockLoc := BuildBlockLoactionWithLocation(
						name, blks[block.GetID()].GetExtent(), 0, block.GetID(),
						table.Start-block.GetStartOffset(), block.GetEndOffset()-block.GetStartOffset())
					table.locations.Append(blockLoc)
				} else if table.End <= block.GetEndOffset() && table.End >= block.GetStartOffset() {
					blockLoc := BuildBlockLoactionWithLocation(
						name, blks[block.GetID()].GetExtent(), 0, block.GetID(),
						0, table.End-block.GetStartOffset())
					table.locations.Append(blockLoc)
				}
			}
		}
	}

	data.meta[0] = NewCheckpointMeta()
	data.meta[0].tables[0] = NewTableMeta()
	for num, fileName := range checkpointNames {
		loc := objectBlocks[num][0]
		blockLoc := BuildBlockLoactionWithLocation(
			fileName, loc.GetExtent(), 0, loc.GetID(),
			0, 0)
		data.meta[0].tables[0].locations.Append(blockLoc)
	}
	data.prepareMeta()
	if err != nil {
		return
	}

	segmentid2 := objectio.NewSegmentid()
	name2 := objectio.BuildObjectName(segmentid2, 0)
	writer2, err := blockio.NewBlockWriterNew(fs, name2, 0, nil)
	if err != nil {
		return
	}
	if _, _, err = writer2.WriteSubBatch(
		containers.ToCNBatch(data.bats[MetaIDX]),
		objectio.ConvertToSchemaType(uint16(MetaIDX))); err != nil {
		return
	}
	if err != nil {
		return
	}
	if _, _, err = writer2.WriteSubBatch(
		containers.ToCNBatch(data.bats[TNMetaIDX]),
		objectio.ConvertToSchemaType(uint16(TNMetaIDX))); err != nil {
		return
	}
	if err != nil {
		return
	}
	blks2, _, err := writer2.Sync(context.Background())
	CNLocation = objectio.BuildLocation(name2, blks2[0].GetExtent(), 0, blks2[0].GetID())
	TNLocation = objectio.BuildLocation(name2, blks2[1].GetExtent(), 0, blks2[1].GetID())
	return
}

func validateBeforeLoadBlkCol(version uint32, idxs []uint16, colNames []string) []uint16 {
	// in version 11, the storage usage ins/del was added into the ckp meta batch
	return idxs
}

func LoadBlkColumnsByMeta(
	version uint32,
	cxt context.Context,
	colTypes []types.Type,
	colNames []string,
	id uint16,
	reader *blockio.BlockReader,
	mp *mpool.MPool,
) ([]*containers.Batch, error) {
	idxs := make([]uint16, len(colNames))
	for i := range colNames {
		idxs[i] = uint16(i)
	}
	var err error
	var ioResults []*batch.Batch
	var releases func()
	defer func() {
		if releases != nil {
			releases()
		}
	}()
	idxs = validateBeforeLoadBlkCol(version, idxs, colNames)
	ioResults, releases, err = reader.LoadSubColumns(cxt, idxs, nil, id, nil)
	if err != nil {
		return nil, err
	}
	bats := make([]*containers.Batch, 0)
	for _, ioResult := range ioResults {
		bat := containers.NewBatch()
		for i, idx := range idxs {
			pkgVec := ioResult.Vecs[i]
			var vec containers.Vector
			if pkgVec.Length() == 0 {
				vec = containers.MakeVector(colTypes[i], mp)
			} else {
				srcVec := containers.ToTNVector(pkgVec, mp)
				defer srcVec.Close()
				vec = srcVec.CloneWindow(0, srcVec.Length(), mp)
			}
			bat.AddVector(colNames[idx], vec)
			bat.Vecs[i] = vec

		}
		bats = append(bats, bat)
	}
	return bats, nil
}

func LoadCNSubBlkColumnsByMeta(
	version uint32,
	cxt context.Context,
	colTypes []types.Type,
	colNames []string,
	id uint16,
	reader *blockio.BlockReader,
	m *mpool.MPool,
) ([]*batch.Batch, error) {
	idxs := make([]uint16, len(colNames))
	for i := range colNames {
		idxs[i] = uint16(i)
	}
	var err error
	var ioResults []*batch.Batch
	var release func()
	bats := make([]*batch.Batch, 0)
	defer func() {
		if release != nil {
			release()
		}
	}()
	idxs = validateBeforeLoadBlkCol(version, idxs, colNames)
	ioResults, release, err = reader.LoadSubColumns(cxt, idxs, nil, id, m)
	if err != nil {
		return nil, err
	}
	for i := range ioResults {
		ioResults[i].Attrs = make([]string, len(colNames))
		copy(ioResults[i].Attrs, colNames)
		var bat *batch.Batch
		bat, err = ioResults[i].Dup(m)
		if err != nil {
			return nil, err
		}
		bats = append(bats, bat)
	}
	return bats, nil
}

func LoadCNSubBlkColumnsByMetaWithId(
	cxt context.Context,
	colTypes []types.Type,
	colNames []string,
	dataType uint16,
	id uint16,
	version uint32,
	reader *blockio.BlockReader,
	m *mpool.MPool,
) (bat *batch.Batch, err error) {
	idxs := make([]uint16, len(colNames))
	for i := range colNames {
		idxs[i] = uint16(i)
	}
	var ioResult *batch.Batch
	var release func()
	defer func() {
		if release != nil {
			release()
		}
	}()
	idxs = validateBeforeLoadBlkCol(version, idxs, colNames)
	ioResult, release, err = reader.LoadOneSubColumns(cxt, idxs, nil, dataType, id, m)
	if err != nil {
		return nil, err
	}
	ioResult.Attrs = make([]string, len(colNames))
	copy(ioResult.Attrs, colNames)
	return ioResult.Dup(m)
}
func (data *CheckpointData) ReadTNMetaBatch(
	ctx context.Context,
	version uint32,
	location objectio.Location,
	reader *blockio.BlockReader,
) (err error) {
	if data.bats[TNMetaIDX].Length() == 0 {
		var bats []*containers.Batch
		item := checkpointDataReferVersions[version][TNMetaIDX]
		bats, err = LoadBlkColumnsByMeta(version, ctx, item.types, item.attrs, TNMetaIDX, reader, data.allocator)
		if err != nil {
			return
		}
		data.bats[TNMetaIDX] = bats[0]
	}
	return
}

func (data *CheckpointData) PrefetchMeta(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
	key objectio.Location) (err error) {
	var pref blockio.PrefetchParams
	pref, err = blockio.BuildPrefetchParams(service, key)
	if err != nil {
		return
	}
	meteIdxSchema := checkpointDataReferVersions[version][MetaIDX]
	tnMeteIdxSchema := checkpointDataReferVersions[version][TNMetaIDX]
	idxes := make([]uint16, 0)
	tnIdxes := make([]uint16, 0)
	for attr := range meteIdxSchema.attrs {
		idxes = append(idxes, uint16(attr))
	}
	for attr := range tnMeteIdxSchema.attrs {
		tnIdxes = append(tnIdxes, uint16(attr))
	}
	pref.AddBlockWithType(idxes, []uint16{0}, uint16(objectio.ConvertToSchemaType(MetaIDX)))
	pref.AddBlockWithType(tnIdxes, []uint16{1}, uint16(objectio.ConvertToSchemaType(TNMetaIDX)))
	return blockio.PrefetchWithMerged(data.sid, pref)
}

type blockIdx struct {
	location objectio.Location
	dataType uint16
}

func (data *CheckpointData) PrefetchFrom(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
	key objectio.Location) (err error) {
	blocks, area := vector.MustVarlenaRawData(data.bats[TNMetaIDX].GetVectorByName(CheckpointMetaAttr_BlockLocation).GetDownstreamVector())
	dataType := vector.MustFixedCol[uint16](data.bats[TNMetaIDX].GetVectorByName(CheckpointMetaAttr_SchemaType).GetDownstreamVector())
	var pref blockio.PrefetchParams
	locations := make(map[string][]blockIdx)
	checkpointSize := uint64(0)
	for i := range blocks {
		location := objectio.Location(blocks[i].GetByteSlice(area))
		if location.IsEmpty() {
			continue
		}
		name := location.Name()
		if locations[name.String()] == nil {
			locations[name.String()] = make([]blockIdx, 0)
		}
		locations[name.String()] = append(locations[name.String()], blockIdx{location: location, dataType: dataType[i]})
	}
	for _, blockIdxes := range locations {
		pref, err = blockio.BuildPrefetchParams(service, blockIdxes[0].location)
		if err != nil {
			return
		}
		checkpointSize += uint64(blockIdxes[0].location.Extent().End())
		logutil.Info("prefetch-read-checkpoint", common.OperationField("prefetch read"),
			common.OperandField("checkpoint"),
			common.AnyField("location", blockIdxes[0].location.String()),
			common.AnyField("size", checkpointSize))
		for _, idx := range blockIdxes {
			schema := checkpointDataReferVersions[version][idx.dataType]
			idxes := make([]uint16, len(schema.attrs))
			for attr := range schema.attrs {
				idxes[attr] = uint16(attr)
			}
			pref.AddBlockWithType(idxes, []uint16{idx.location.ID()}, uint16(objectio.ConvertToSchemaType(idx.dataType)))
		}
		err = blockio.PrefetchWithMerged(data.sid, pref)
		if err != nil {
			logutil.Warnf("PrefetchFrom PrefetchWithMerged error %v", err)
		}
	}
	logutil.Info("prefetch-checkpoint",
		common.AnyField("size", checkpointSize),
		common.AnyField("count", len(locations)))
	return
}

func prefetchCheckpointData(
	ctx context.Context,
	sid string,
	version uint32,
	service fileservice.FileService,
	key objectio.Location,
) (err error) {
	var pref blockio.PrefetchParams
	pref, err = blockio.BuildPrefetchParams(service, key)
	if err != nil {
		return
	}
	for idx, item := range checkpointDataReferVersions[version] {
		idxes := make([]uint16, len(item.attrs))
		for i := range item.attrs {
			idxes[i] = uint16(i)
		}
		pref.AddBlock(idxes, []uint16{uint16(idx)})
	}
	return blockio.PrefetchWithMerged(sid, pref)
}

// TODO:
// There need a global io pool
func (data *CheckpointData) ReadFrom(
	ctx context.Context,
	version uint32,
	location objectio.Location,
	reader *blockio.BlockReader,
	fs fileservice.FileService,
) (err error) {
	err = data.readMetaBatch(ctx, version, reader, data.allocator)
	if err != nil {
		return
	}
	err = data.readAll(ctx, version, fs)
	if err != nil {
		return
	}

	return
}

func LoadCheckpointLocations(
	ctx context.Context,
	sid string,
	location objectio.Location,
	version uint32,
	fs fileservice.FileService,
) (map[string]objectio.Location, error) {
	var err error
	data := NewCheckpointData(sid, common.CheckpointAllocator)
	defer data.Close()

	var reader *blockio.BlockReader
	if reader, err = blockio.NewObjectReader(sid, fs, location); err != nil {
		return nil, err
	}

	if err = data.readMetaBatch(ctx, version, reader, nil); err != nil {
		return nil, err
	}

	data.replayMetaBatch(version)
	return data.locations, nil
}

// LoadSpecifiedCkpBatch loads a specified checkpoint data batch
func LoadSpecifiedCkpBatch(
	ctx context.Context,
	sid string,
	location objectio.Location,
	version uint32,
	batchIdx uint16,
	fs fileservice.FileService,
) (data *CheckpointData, err error) {
	data = NewCheckpointData(sid, common.CheckpointAllocator)
	defer func() {
		if err != nil {
			data.Close()
			data = nil
		}
	}()

	if batchIdx >= MaxIDX {
		err = moerr.NewInvalidArgNoCtx("out of bound batchIdx", batchIdx)
		return
	}
	var reader *blockio.BlockReader
	if reader, err = blockio.NewObjectReader(sid, fs, location); err != nil {
		return
	}

	if err = data.readMetaBatch(ctx, version, reader, nil); err != nil {
		return
	}

	data.replayMetaBatch(version)
	for _, val := range data.locations {
		if reader, err = blockio.NewObjectReader(sid, fs, val); err != nil {
			return
		}
		var bats []*containers.Batch
		item := checkpointDataReferVersions[version][batchIdx]
		if bats, err = LoadBlkColumnsByMeta(
			version, ctx, item.types, item.attrs, batchIdx, reader, data.allocator,
		); err != nil {
			return
		}

		for i := range bats {
			if err = data.bats[batchIdx].Append(bats[i]); err != nil {
				return
			}
			defer bats[i].Close()
		}
	}

	return data, nil
}

func (data *CheckpointData) readMetaBatch(
	ctx context.Context,
	version uint32,
	reader *blockio.BlockReader,
	_ *mpool.MPool,
) (err error) {
	if data.bats[MetaIDX].Length() == 0 {
		var bats []*containers.Batch
		item := checkpointDataReferVersions[version][MetaIDX]
		if bats, err = LoadBlkColumnsByMeta(
			version, ctx, item.types, item.attrs, uint16(0), reader, data.allocator,
		); err != nil {
			return
		}
		data.bats[MetaIDX] = bats[0]
	}
	return
}

func (data *CheckpointData) replayMetaBatch(version uint32) {
	bat := data.bats[MetaIDX]
	data.locations = make(map[string]objectio.Location)
	tidVec := vector.MustFixedCol[uint64](bat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector())
	insVec := bat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchLocation).GetDownstreamVector()
	dataObjectVec := bat.GetVectorByName(SnapshotMetaAttr_DataObjectBatchLocation).GetDownstreamVector()
	tombstoneObjectVec := bat.GetVectorByName(SnapshotMetaAttr_TombstoneObjectBatchLocation).GetDownstreamVector()

	var usageInsVec, usageDelVec *vector.Vector
	usageInsVec = bat.GetVectorByName(CheckpointMetaAttr_StorageUsageInsLocation).GetDownstreamVector()
	usageDelVec = bat.GetVectorByName(CheckpointMetaAttr_StorageUsageDelLocation).GetDownstreamVector()

	for i := 0; i < len(tidVec); i++ {
		tid := tidVec[i]
		if tid == 0 {
			bl := BlockLocations(insVec.GetBytesAt(i))
			it := bl.MakeIterator()
			for it.HasNext() {
				block := it.Next()
				if !block.GetLocation().IsEmpty() {
					data.locations[block.GetLocation().Name().String()] = block.GetLocation()
				}
			}
			continue
		}
		dataObjectLocation := dataObjectVec.GetBytesAt(i)
		tombstoneObjectLocation := tombstoneObjectVec.GetBytesAt(i)

		tmp := [][]byte{dataObjectLocation, tombstoneObjectLocation}
		if usageInsVec != nil {
			tmp = append(tmp, usageInsVec.GetBytesAt(i))
			tmp = append(tmp, usageDelVec.GetBytesAt(i))
		}

		tableMeta := NewCheckpointMeta()
		tableMeta.DecodeFromString(tmp)
		data.meta[tid] = tableMeta
	}

	for _, meta := range data.meta {
		for _, table := range meta.tables {
			if table == nil {
				continue
			}

			it := table.locations.MakeIterator()
			for it.HasNext() {
				block := it.Next()
				if !block.GetLocation().IsEmpty() {
					data.locations[block.GetLocation().Name().String()] = block.GetLocation()
				}
			}
		}
	}
}

func (data *CheckpointData) readAll(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
) (err error) {
	data.replayMetaBatch(version)
	checkpointDataSize := uint64(0)
	readDuration := time.Now()
	for _, val := range data.locations {
		var reader *blockio.BlockReader
		reader, err = blockio.NewObjectReader(data.sid, service, val)
		if err != nil {
			return
		}
		var bats []*containers.Batch
		now := time.Now()
		for idx := range checkpointDataReferVersions[version] {
			if uint16(idx) == MetaIDX || uint16(idx) == TNMetaIDX {
				continue
			}
			item := checkpointDataReferVersions[version][idx]
			if bats, err = LoadBlkColumnsByMeta(
				version, ctx, item.types, item.attrs, uint16(idx), reader, data.allocator,
			); err != nil {
				return
			}
			for i := range bats {
				data.bats[idx].Append(bats[i])
				bats[i].Close()
			}
		}
		logutil.Info("read-checkpoint", common.OperationField("read"),
			common.OperandField("checkpoint"),
			common.AnyField("location", val.String()),
			common.AnyField("size", val.Extent().End()),
			common.AnyField("read cost", time.Since(now)))
		checkpointDataSize += uint64(val.Extent().End())
	}
	logutil.Info("read-all", common.OperationField("read"),
		common.OperandField("checkpoint"),
		common.AnyField("size", checkpointDataSize),
		common.AnyField("duration", time.Since(readDuration)))
	return
}

func (data *CheckpointData) ExportStats(prefix string) []zap.Field {
	fields := make([]zap.Field, 0, len(data.bats)+2)
	totalSize := 0
	totalRow := 0
	for idx := range data.bats {
		if data.bats[idx] == nil || data.bats[idx].Length() == 0 {
			continue
		}
		size := data.bats[idx].Allocated()
		rows := data.bats[idx].Length()
		totalSize += size
		totalRow += rows
		fields = append(fields, zap.Int(fmt.Sprintf("%s%s-Size", prefix, IDXString(uint16(idx))), size))
		fields = append(fields, zap.Int(fmt.Sprintf("%s%s-Row", prefix, IDXString(uint16(idx))), rows))
	}
	fields = append(fields, zap.Int(fmt.Sprintf("%stotalSize", prefix), totalSize))
	fields = append(fields, zap.Int(fmt.Sprintf("%stotalRow", prefix), totalRow))
	return fields
}

func (data *CheckpointData) Close() {
	for idx := range data.bats {
		if data.bats[idx] != nil {
			data.bats[idx].Close()
			data.bats[idx] = nil
		}
	}
	data.allocator = nil
}

func (data *CheckpointData) CloseWhenLoadFromCache(version uint32) {
}
func (data *CheckpointData) GetBatches() []*containers.Batch {
	return data.bats[:]
}
func (data *CheckpointData) GetDBBatchs() (
	*containers.Batch,
	*containers.Batch,
	*containers.Batch,
	*containers.Batch) {
	return data.bats[DBInsertIDX],
		data.bats[DBInsertTxnIDX],
		data.bats[DBDeleteIDX],
		data.bats[DBDeleteTxnIDX]
}
func (data *CheckpointData) GetTblBatchs() (
	*containers.Batch,
	*containers.Batch,
	*containers.Batch,
	*containers.Batch,
	*containers.Batch) {
	return data.bats[TBLInsertIDX],
		data.bats[TBLInsertTxnIDX],
		data.bats[TBLColInsertIDX],
		data.bats[TBLDeleteIDX],
		data.bats[TBLDeleteTxnIDX]
}
func (data *CheckpointData) GetTombstoneObjectBatchs() *containers.Batch {
	return data.bats[TombstoneObjectInfoIDX]
}
func (data *CheckpointData) GetObjectBatchs() *containers.Batch {
	return data.bats[ObjectInfoIDX]
}

type tableinfo struct {
	tid    uint64
	add    uint64
	delete uint64
}

type TableInfoJson struct {
	ID             uint64 `json:"id"`
	Add            uint64 `json:"add,omitempty"`
	Delete         uint64 `json:"delete,omitempty"`
	TombstoneRows  uint64 `json:"tombstone_rows,omitempty"`
	TombstoneCount uint64 `json:"tombstone_count,omitempty"`
}

type ObjectInfoJson struct {
	TableCnt     int    `json:"table_count,omitempty"`
	ObjectCnt    uint64 `json:"object_count"`
	ObjectAddCnt uint64 `json:"object_add_count"`
	ObjectDelCnt uint64 `json:"object_del_count"`
	TombstoneCnt int    `json:"tombstone_count"`

	Tables []TableInfoJson `json:"tables,omitempty"`
}

type CheckpointInfoJson struct {
	CheckpointDataCount int              `json:"checkpoint_data_count"`
	Data                []ObjectInfoJson `json:"data"`
}

const (
	invalid = 0xffffff
)

func (data *CheckpointData) GetCheckpointMetaInfo(id uint64, limit int) (res *ObjectInfoJson, err error) {
	tombstone := make(map[string]struct{})
	tombstoneInfo := make(map[uint64]*tableinfo)

	insTableIDs := vector.MustFixedCol[uint64](
		data.bats[ObjectInfoIDX].GetVectorByName(SnapshotAttr_TID).GetDownstreamVector())
	insDeleteTSs := vector.MustFixedCol[types.TS](
		data.bats[ObjectInfoIDX].GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector())
	files := make(map[uint64]*tableinfo)
	for i := range data.bats[ObjectInfoIDX].Length() {
		if files[insTableIDs[i]] == nil {
			files[insTableIDs[i]] = &tableinfo{
				tid: insTableIDs[i],
			}
		}
		deleteTs := insDeleteTSs[i]
		if deleteTs.IsEmpty() {
			files[insTableIDs[i]].add++
		} else {
			files[insTableIDs[i]].delete++
		}
	}

	tableinfos := make([]*tableinfo, 0)
	objectCount := uint64(0)
	addCount := uint64(0)
	deleteCount := uint64(0)
	for _, count := range files {
		tableinfos = append(tableinfos, count)
		objectCount += count.add
		addCount += count.add
		objectCount += count.delete
		deleteCount += count.delete
	}
	sort.Slice(tableinfos, func(i, j int) bool {
		return tableinfos[i].add > tableinfos[j].add
	})
	tableJsons := make([]TableInfoJson, 0, data.bats[ObjectInfoIDX].Length())
	tables := make(map[uint64]int)
	for i := range len(tableinfos) {
		tablejson := TableInfoJson{
			ID:     tableinfos[i].tid,
			Add:    tableinfos[i].add,
			Delete: tableinfos[i].delete,
		}
		if id == 0 || tablejson.ID == id {
			tables[tablejson.ID] = len(tableJsons)
			tableJsons = append(tableJsons, tablejson)
		}
	}
	tableinfos2 := make([]*tableinfo, 0)
	objectCount2 := uint64(0)
	addCount2 := uint64(0)
	for _, count := range tombstoneInfo {
		tableinfos2 = append(tableinfos2, count)
		objectCount2 += count.add
		addCount2 += count.add
	}
	sort.Slice(tableinfos2, func(i, j int) bool {
		return tableinfos2[i].add > tableinfos2[j].add
	})

	for i := range len(tableinfos2) {
		if idx, ok := tables[tableinfos2[i].tid]; ok {
			tablejson := &tableJsons[idx]
			tablejson.TombstoneRows = tableinfos2[i].add
			tablejson.TombstoneCount = tableinfos2[i].delete
			continue
		}
		tablejson := TableInfoJson{
			ID:             tableinfos2[i].tid,
			TombstoneRows:  tableinfos2[i].add,
			TombstoneCount: tableinfos2[i].delete,
		}
		if id == 0 || tablejson.ID == id {
			tableJsons = append(tableJsons, tablejson)
		}
	}

	res = &ObjectInfoJson{
		TableCnt:     len(tableJsons),
		ObjectCnt:    objectCount,
		ObjectAddCnt: addCount,
		ObjectDelCnt: deleteCount,
		TombstoneCnt: len(tombstone),
	}

	if id != invalid {
		if limit < len(tableJsons) {
			tableJsons = tableJsons[:limit]
		}
		res.Tables = tableJsons
	}

	return
}

func (data *CheckpointData) GetTableIds() []uint64 {
	input := vector.MustFixedCol[uint64](data.bats[ObjectInfoIDX].GetVectorByName(SnapshotAttr_TID).GetDownstreamVector())
	seen := make(map[uint64]struct{})
	var result []uint64

	for _, v := range input {
		if _, ok := seen[v]; !ok {
			result = append(result, v)
			seen[v] = struct{}{}
		}
	}

	return result
}

func (collector *BaseCollector) LoadAndCollectObject(c *catalog.Catalog, visitObject func(*catalog.ObjectEntry) error) error {
	if collector.isPrefetch {
		collector.isPrefetch = false
	} else {
		return nil
	}
	collector.data.bats[ObjectInfoIDX] = makeRespBatchFromSchema(ObjectInfoSchema, common.CheckpointAllocator)
	err := collector.loadObjectInfo()
	if err != nil {
		return err
	}
	p := &catalog.LoopProcessor{}
	p.ObjectFn = visitObject
	err = c.RecurLoop(p)
	if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
		err = nil
	}
	return err
}

func (data *CheckpointData) GetOneBatch(idx uint16) *containers.Batch {
	return data.bats[idx]
}

func (collector *BaseCollector) VisitDB(entry *catalog.DBEntry) error {
	if shouldIgnoreDBInLogtail(entry.ID) {
		return nil
	}
	mvccNodes := entry.ClonePreparedInRange(collector.start, collector.end)
	// delStart := collector.data.bats[DBDeleteIDX].GetVectorByName(catalog.AttrRowID).Length()
	// insStart := collector.data.bats[DBInsertIDX].GetVectorByName(catalog.AttrRowID).Length()
	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		dbNode := node
		var created, dropped bool
		if dbNode.HasDropCommitted() {
			dropped = true
			if dbNode.CreatedAt.Equal(&dbNode.DeletedAt) {
				created = true
			}
		} else {
			created = true
		}
		if dropped {
			collector.Usage.Deletes = append(collector.Usage.Deletes, entry)
			// delScehma is empty, it will just fill rowid / commit ts
			catalogEntry2Batch(
				collector.data.bats[DBDeleteIDX],
				entry,
				node,
				DBDelSchema,
				txnimpl.FillDBRow,
				objectio.HackU64ToRowid(entry.GetID()),
				dbNode.GetEnd())
			dbNode.TxnMVCCNode.AppendTuple(collector.data.bats[DBDeleteTxnIDX])
			collector.data.bats[DBDeleteTxnIDX].GetVectorByName(SnapshotAttr_DBID).Append(entry.GetID(), false)
		}
		if created {
			catalogEntry2Batch(collector.data.bats[DBInsertIDX],
				entry,
				node,
				catalog.SystemDBSchema,
				txnimpl.FillDBRow,
				objectio.HackU64ToRowid(entry.GetID()),
				dbNode.GetEnd())
			dbNode.TxnMVCCNode.AppendTuple(collector.data.bats[DBInsertTxnIDX])
		}
	}
	// delEnd := collector.data.bats[DBDeleteIDX].GetVectorByName(catalog.AttrRowID).Length()
	// insEnd := collector.data.bats[DBInsertIDX].GetVectorByName(catalog.AttrRowID).Length()
	// collector.data.updateMOCatalog(pkgcatalog.MO_DATABASE_ID, int32(insStart), int32(insEnd), int32(delStart), int32(delEnd))
	return nil
}
func (collector *GlobalCollector) isEntryDeletedBeforeThreshold(entry catalog.BaseEntry) bool {
	entry.RLock()
	defer entry.RUnlock()
	return entry.DeleteBeforeLocked(collector.versionThershold)
}
func (collector *GlobalCollector) VisitDB(entry *catalog.DBEntry) error {
	if collector.isEntryDeletedBeforeThreshold(entry.BaseEntryImpl) {
		collector.Usage.Deletes = append(collector.Usage.Deletes, entry)
		return nil
	}

	currAccId := uint32(entry.GetTenantID())
	collector.Usage.ReservedAccIds[uint64(currAccId)] = struct{}{}

	return collector.BaseCollector.VisitDB(entry)
}

func (collector *BaseCollector) Allocator() *mpool.MPool { return collector.data.allocator }
func (collector *BaseCollector) VisitTable(entry *catalog.TableEntry) (err error) {
	if shouldIgnoreTblInLogtail(entry.ID) {
		return nil
	}
	mvccNodes := entry.ClonePreparedInRange(collector.start, collector.end)
	tableInsBat := collector.data.bats[TBLInsertIDX]
	tableInsTxnBat := collector.data.bats[TBLInsertTxnIDX]
	tableDelBat := collector.data.bats[TBLDeleteIDX]
	tableDelTxnBat := collector.data.bats[TBLDeleteTxnIDX]
	tableColInsBat := collector.data.bats[TBLColInsertIDX]
	tableColDelBat := collector.data.bats[TBLColDeleteIDX]
	// tblDelStart := tableDelBat.GetVectorByName(catalog.AttrRowID).Length()
	// tblInsStart := tableInsBat.GetVectorByName(catalog.AttrRowID).Length()
	// colDelStart := tableColDelBat.GetVectorByName(catalog.AttrRowID).Length()
	// colInsStart := tableColInsBat.GetVectorByName(catalog.AttrRowID).Length()
	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		tblNode := node
		var created, dropped bool
		if tblNode.HasDropCommitted() {
			dropped = true
			if tblNode.CreatedAt.Equal(&tblNode.DeletedAt) {
				created = true
			}
		} else {
			created = true
		}
		if created {
			for _, syscol := range catalog.SystemColumnSchema.ColDefs {
				txnimpl.FillColumnRow(
					entry,
					tblNode,
					syscol.Name,
					tableColInsBat.GetVectorByName(syscol.Name),
				)
			}
			// send dropped column del
			for _, name := range tblNode.BaseNode.Schema.Extra.DroppedAttrs {
				tableColDelBat.GetVectorByName(catalog.PhyAddrColumnName).Append(objectio.HackBytes2Rowid([]byte(fmt.Sprintf("%d-%s", entry.GetID(), name))), false)
				tableColDelBat.GetVectorByName(catalog.AttrCommitTs).Append(tblNode.GetEnd(), false)
				tableColDelBat.GetVectorByName(pkgcatalog.SystemColAttr_UniqName).Append([]byte(fmt.Sprintf("%d-%s", entry.GetID(), name)), false)
			}
			rowidVec := tableColInsBat.GetVectorByName(catalog.PhyAddrColumnName)
			commitVec := tableColInsBat.GetVectorByName(catalog.AttrCommitTs)
			for _, usercol := range tblNode.BaseNode.Schema.ColDefs {
				rowidVec.Append(objectio.HackBytes2Rowid([]byte(fmt.Sprintf("%d-%s", entry.GetID(), usercol.Name))), false)
				commitVec.Append(tblNode.GetEnd(), false)
			}

			tableInsTxnBat.GetVectorByName(
				SnapshotAttr_BlockMaxRow).Append(entry.GetLastestSchemaLocked(false).BlockMaxRows, false)
			tableInsTxnBat.GetVectorByName(
				SnapshotAttr_ObjectMaxBlock).Append(entry.GetLastestSchemaLocked(false).ObjectMaxBlocks, false)
			tableInsTxnBat.GetVectorByName(
				SnapshotAttr_SchemaExtra).Append(tblNode.BaseNode.Schema.MustGetExtraBytes(), false)

			catalogEntry2Batch(
				tableInsBat,
				entry,
				tblNode,
				catalog.SystemTableSchema,
				txnimpl.FillTableRow,
				objectio.HackU64ToRowid(entry.GetID()),
				tblNode.GetEnd(),
			)

			tblNode.TxnMVCCNode.AppendTuple(tableInsTxnBat)
		}
		if dropped {
			collector.Usage.Deletes = append(collector.Usage.Deletes, entry)
			tableDelTxnBat.GetVectorByName(
				SnapshotAttr_DBID).Append(entry.GetDB().GetID(), false)
			tableDelTxnBat.GetVectorByName(
				SnapshotAttr_TID).Append(entry.GetID(), false)

			rowidVec := tableColDelBat.GetVectorByName(catalog.PhyAddrColumnName)
			commitVec := tableColDelBat.GetVectorByName(catalog.AttrCommitTs)
			pkVec := tableColDelBat.GetVectorByName(pkgcatalog.SystemColAttr_UniqName)
			for _, usercol := range tblNode.BaseNode.Schema.ColDefs {
				rowidVec.Append(objectio.HackBytes2Rowid([]byte(fmt.Sprintf("%d-%s", entry.GetID(), usercol.Name))), false)
				commitVec.Append(tblNode.GetEnd(), false)
				pkVec.Append([]byte(fmt.Sprintf("%d-%s", entry.GetID(), usercol.Name)), false)
			}

			catalogEntry2Batch(
				tableDelBat,
				entry,
				tblNode,
				TblDelSchema,
				txnimpl.FillTableRow,
				objectio.HackU64ToRowid(entry.GetID()),
				tblNode.GetEnd(),
			)
			tblNode.TxnMVCCNode.AppendTuple(tableDelTxnBat)
		}
	}
	// tblDelEnd := tableDelBat.GetVectorByName(catalog.AttrRowID).Length()
	// tblInsEnd := tableInsBat.GetVectorByName(catalog.AttrRowID).Length()
	// colDelEnd := tableColDelBat.GetVectorByName(catalog.AttrRowID).Length()
	// colInsEnd := tableColInsBat.GetVectorByName(catalog.AttrRowID).Length()
	// collector.data.updateMOCatalog(pkgcatalog.MO_TABLES_ID, int32(tblInsStart), int32(tblInsEnd), int32(tblDelStart), int32(tblDelEnd))
	// collector.data.updateMOCatalog(pkgcatalog.MO_COLUMNS_ID, int32(colInsStart), int32(colInsEnd), int32(colDelStart), int32(colDelEnd))
	return nil
}

func (collector *GlobalCollector) VisitTable(entry *catalog.TableEntry) error {
	if collector.isEntryDeletedBeforeThreshold(entry.BaseEntryImpl) {
		collector.Usage.Deletes = append(collector.Usage.Deletes, entry)
		return nil
	}
	if collector.isEntryDeletedBeforeThreshold(entry.GetDB().BaseEntryImpl) {
		return nil
	}
	return collector.BaseCollector.VisitTable(entry)
}

func (collector *BaseCollector) visitObjectEntry(entry *catalog.ObjectEntry) error {
	mvccNodes := entry.GetMVCCNodeInRange(collector.start, collector.end)
	if len(mvccNodes) == 0 {
		return nil
	}

	err := collector.fillObjectInfoBatch(entry, mvccNodes)
	if err != nil {
		return err
	}
	return nil
}
func (collector *BaseCollector) loadObjectInfo() error {
	panic("not support")
}
func (collector *BaseCollector) fillObjectInfoBatch(entry *catalog.ObjectEntry, mvccNodes []*txnbase.TxnMVCCNode) error {
	if len(mvccNodes) == 0 {
		return nil
	}
	dataStart := collector.data.bats[ObjectInfoIDX].GetVectorByName(catalog.ObjectAttr_ObjectStats).Length()
	tombstoneStart := collector.data.bats[TombstoneObjectInfoIDX].GetVectorByName(catalog.ObjectAttr_ObjectStats).Length()

	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		create := node.End.Equal(&entry.CreatedAt)
		if entry.IsTombstone {
			visitObject(collector.data.bats[TombstoneObjectInfoIDX], entry, node, create, false, types.TS{})
		} else {
			visitObject(collector.data.bats[ObjectInfoIDX], entry, node, create, false, types.TS{})
		}
		objNode := node

		// collect usage info
		if !entry.DeletedAt.IsEmpty() && objNode.End.Equal(&entry.DeletedAt) {
			// deleted and non-append, record into the usage del bat
			if !entry.IsAppendable() && objNode.IsCommitted() {
				collector.Usage.ObjDeletes = append(collector.Usage.ObjDeletes, entry)
			}
		} else {
			// create and non-append, record into the usage ins bat
			if !entry.IsAppendable() && objNode.IsCommitted() {
				collector.Usage.ObjInserts = append(collector.Usage.ObjInserts, entry)
			}
		}

	}
	dataEnd := collector.data.bats[ObjectInfoIDX].GetVectorByName(catalog.ObjectAttr_ObjectStats).Length()
	collector.data.UpdateDataObjectMeta(entry.GetTable().ID, int32(dataStart), int32(dataEnd))
	tombstoneEnd := collector.data.bats[TombstoneObjectInfoIDX].GetVectorByName(catalog.ObjectAttr_ObjectStats).Length()
	collector.data.UpdateTombstoneObjectMeta(entry.GetTable().ID, int32(tombstoneStart), int32(tombstoneEnd))
	return nil
}

func (collector *BaseCollector) VisitObjForBackup(entry *catalog.ObjectEntry) (err error) {
	createTS := entry.GetCreatedAt()
	if createTS.Greater(&collector.start) {
		return nil
	}
	return collector.visitObjectEntry(entry)
}

func (collector *BaseCollector) VisitObj(entry *catalog.ObjectEntry) (err error) {
	collector.visitObjectEntry(entry)
	return nil
}

func (collector *GlobalCollector) VisitObj(entry *catalog.ObjectEntry) error {
	if entry.DeleteBefore(collector.versionThershold) {
		return nil
	}
	if collector.isEntryDeletedBeforeThreshold(entry.GetTable().BaseEntryImpl) {
		return nil
	}
	if collector.isEntryDeletedBeforeThreshold(entry.GetTable().GetDB().BaseEntryImpl) {
		return nil
	}
	return collector.BaseCollector.VisitObj(entry)
}

// TODO
// func (collector *BaseCollector) VisitBlkForBackup(entry *catalog.BlockEntry) (err error) {
// 	entry.RLock()
// 	if entry.GetCreatedAtLocked().Greater(collector.start) {
// 		entry.RUnlock()
// 		return nil
// 	}
// 	entry.RUnlock()
// 	collector.visitBlockEntry(entry)
// 	return nil
// }

func (collector *BaseCollector) OrphanData() *CheckpointData {
	data := collector.data
	collector.data = nil
	return data
}

func (collector *BaseCollector) Close() {
	if collector.data != nil {
		collector.data.Close()
	}
}
