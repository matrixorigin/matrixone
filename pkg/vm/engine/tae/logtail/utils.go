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
	"bytes"
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
)

const DefaultCheckpointBlockRows = 10000

const (
	CheckpointVersion1 uint32 = 1
	CheckpointVersion2 uint32 = 2
	CheckpointVersion3 uint32 = 3
	CheckpointVersion4 uint32 = 4
	CheckpointVersion5 uint32 = 5

	CheckpointCurrentVersion = CheckpointVersion5
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

	SEGInsertIDX
	SEGInsertTxnIDX
	SEGDeleteIDX
	SEGDeleteTxnIDX

	BLKMetaInsertIDX
	BLKMetaInsertTxnIDX
	BLKMetaDeleteIDX
	BLKMetaDeleteTxnIDX

	BLKDNMetaInsertIDX
	BLKDNMetaInsertTxnIDX
	BLKDNMetaDeleteIDX
	BLKDNMetaDeleteTxnIDX

	BLKCNMetaInsertIDX

	DNMetaIDX
)

const MaxIDX = DNMetaIDX + 1

const (
	Checkpoint_Meta_TID_IDX                 = 2
	Checkpoint_Meta_Insert_Block_LOC_IDX    = 3
	Checkpoint_Meta_CN_Delete_Block_LOC_IDX = 4
	Checkpoint_Meta_Delete_Block_LOC_IDX    = 5
	Checkpoint_Meta_Segment_LOC_IDX         = 6
)

// for ver1-3
const (
	Checkpoint_Meta_Insert_Block_Start_IDX = 3
	Checkpoint_Meta_Insert_Block_End_IDX   = 4
	Checkpoint_Meta_Delete_Block_Start_IDX = 5
	Checkpoint_Meta_Delete_Block_End_IDX   = 6
	Checkpoint_Meta_Segment_Start_IDX      = 7
	Checkpoint_Meta_Segment_End_IDX        = 8
)

type checkpointDataItem struct {
	schema *catalog.Schema
	types  []types.Type
	attrs  []string
}

var checkpointDataSchemas_V1 [MaxIDX]*catalog.Schema
var checkpointDataSchemas_V2 [MaxIDX]*catalog.Schema
var checkpointDataSchemas_V3 [MaxIDX]*catalog.Schema
var checkpointDataSchemas_V4 [MaxIDX]*catalog.Schema
var checkpointDataSchemas_V5 [MaxIDX]*catalog.Schema
var checkpointDataSchemas_Curr [MaxIDX]*catalog.Schema

var checkpointDataReferVersions map[uint32][MaxIDX]*checkpointDataItem

func init() {
	checkpointDataSchemas_V1 = [MaxIDX]*catalog.Schema{
		MetaSchema_V1,
		catalog.SystemDBSchema,
		TxnNodeSchema,
		DelSchema, // 3
		DBDNSchema,
		catalog.SystemTableSchema_V1,
		TblDNSchema,
		DelSchema, // 7
		TblDNSchema,
		catalog.SystemColumnSchema_V1,
		DelSchema,
		SegSchema, // 11
		SegDNSchema,
		DelSchema,
		SegDNSchema,
		BlkMetaSchema, // 15
		BlkDNSchema,
		DelSchema,
		BlkDNSchema,
		BlkMetaSchema, // 19
		BlkDNSchema,
		DelSchema,
		BlkDNSchema,
		BlkMetaSchema, // 23
		DNMetaSchema,
	}
	checkpointDataSchemas_V2 = [MaxIDX]*catalog.Schema{
		MetaSchema_V1,
		catalog.SystemDBSchema,
		TxnNodeSchema,
		DelSchema, // 3
		DBDNSchema,
		catalog.SystemTableSchema,
		TblDNSchema,
		DelSchema, // 7
		TblDNSchema,
		catalog.SystemColumnSchema_V1,
		DelSchema,
		SegSchema, // 11
		SegDNSchema,
		DelSchema,
		SegDNSchema,
		BlkMetaSchema, // 15
		BlkDNSchema,
		DelSchema,
		BlkDNSchema,
		BlkMetaSchema, // 19
		BlkDNSchema,
		DelSchema,
		BlkDNSchema,
		BlkMetaSchema, // 23
		DNMetaSchema,
	}
	checkpointDataSchemas_V3 = [MaxIDX]*catalog.Schema{
		MetaSchema_V1,
		catalog.SystemDBSchema,
		TxnNodeSchema,
		DBDelSchema, // 3
		DBDNSchema,
		catalog.SystemTableSchema,
		TblDNSchema,
		TblDelSchema, // 7
		TblDNSchema,
		catalog.SystemColumnSchema_V1,
		ColumnDelSchema,
		SegSchema, // 11
		SegDNSchema,
		DelSchema,
		SegDNSchema,
		BlkMetaSchema, // 15
		BlkDNSchema,
		DelSchema,
		BlkDNSchema,
		BlkMetaSchema, // 19
		BlkDNSchema,
		DelSchema,
		BlkDNSchema,
		BlkMetaSchema, // 23
		DNMetaSchema,
	}
	checkpointDataSchemas_V4 = [MaxIDX]*catalog.Schema{
		MetaSchema_V1,
		catalog.SystemDBSchema,
		TxnNodeSchema,
		DBDelSchema, // 3
		DBDNSchema,
		catalog.SystemTableSchema,
		TblDNSchema,
		TblDelSchema, // 7
		TblDNSchema,
		catalog.SystemColumnSchema,
		ColumnDelSchema,
		SegSchema, // 11
		SegDNSchema,
		DelSchema,
		SegDNSchema,
		BlkMetaSchema, // 15
		BlkDNSchema,
		DelSchema,
		BlkDNSchema,
		BlkMetaSchema, // 19
		BlkDNSchema,
		DelSchema,
		BlkDNSchema,
		BlkMetaSchema, // 23
		DNMetaSchema,
	}
	checkpointDataSchemas_V5 = [MaxIDX]*catalog.Schema{
		MetaSchema,
		catalog.SystemDBSchema,
		TxnNodeSchema,
		DBDelSchema, // 3
		DBDNSchema,
		catalog.SystemTableSchema,
		TblDNSchema,
		TblDelSchema, // 7
		TblDNSchema,
		catalog.SystemColumnSchema,
		ColumnDelSchema,
		SegSchema, // 11
		SegDNSchema,
		DelSchema,
		SegDNSchema,
		BlkMetaSchema, // 15
		BlkDNSchema,
		DelSchema,
		BlkDNSchema,
		BlkMetaSchema, // 19
		BlkDNSchema,
		DelSchema,
		BlkDNSchema,
		BlkMetaSchema, // 23
		DNMetaSchema,
	}

	checkpointDataReferVersions = make(map[uint32][MaxIDX]*checkpointDataItem)

	registerCheckpointDataReferVersion(CheckpointVersion1, checkpointDataSchemas_V1[:])
	registerCheckpointDataReferVersion(CheckpointVersion2, checkpointDataSchemas_V2[:])
	registerCheckpointDataReferVersion(CheckpointVersion3, checkpointDataSchemas_V3[:])
	registerCheckpointDataReferVersion(CheckpointVersion4, checkpointDataSchemas_V4[:])
	registerCheckpointDataReferVersion(CheckpointVersion5, checkpointDataSchemas_V5[:])
	checkpointDataSchemas_Curr = checkpointDataSchemas_V5
}

func registerCheckpointDataReferVersion(version uint32, schemas []*catalog.Schema) {
	var checkpointDataRefer [MaxIDX]*checkpointDataItem
	for idx, schema := range schemas {
		checkpointDataRefer[idx] = &checkpointDataItem{
			schema,
			append(BaseTypes, schema.Types()...),
			append(BaseAttr, schema.AllNames()...),
		}
	}
	checkpointDataReferVersions[version] = checkpointDataRefer
}

func IncrementalCheckpointDataFactory(start, end types.TS) func(c *catalog.Catalog) (*CheckpointData, error) {
	return func(c *catalog.Catalog) (data *CheckpointData, err error) {
		collector := NewIncrementalCollector(start, end)
		defer collector.Close()
		err = c.RecurLoop(collector)
		if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
			err = nil
		}
		data = collector.OrphanData()
		return
	}
}

func GlobalCheckpointDataFactory(end types.TS, versionInterval time.Duration) func(c *catalog.Catalog) (*CheckpointData, error) {
	return func(c *catalog.Catalog) (data *CheckpointData, err error) {
		collector := NewGlobalCollector(end, versionInterval)
		defer collector.Close()
		err = c.RecurLoop(collector)
		if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
			err = nil
		}
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
	CNBlockInsert
	SegmentDelete
)

func (m *TableMeta) String() string {
	if m == nil {
		return ""
	}
	return fmt.Sprintf("interval:%v, locations:%v", m.ClosedInterval, m.locations)
}

const MetaMaxIdx = SegmentDelete + 1

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
	meta      map[uint64]*CheckpointMeta
	locations map[string]objectio.Location
	bats      [MaxIDX]*containers.Batch
}

func NewCheckpointData() *CheckpointData {
	data := &CheckpointData{
		meta: make(map[uint64]*CheckpointMeta),
	}
	for idx, schema := range checkpointDataSchemas_Curr {
		data.bats[idx] = makeRespBatchFromSchema(schema)
	}
	return data
}

type BaseCollector struct {
	*catalog.LoopProcessor
	start, end types.TS

	data *CheckpointData
}

type IncrementalCollector struct {
	*BaseCollector
}

func NewIncrementalCollector(start, end types.TS) *IncrementalCollector {
	collector := &IncrementalCollector{
		BaseCollector: &BaseCollector{
			LoopProcessor: new(catalog.LoopProcessor),
			data:          NewCheckpointData(),
			start:         start,
			end:           end,
		},
	}
	collector.DatabaseFn = collector.VisitDB
	collector.TableFn = collector.VisitTable
	collector.SegmentFn = collector.VisitSeg
	collector.BlockFn = collector.VisitBlk
	return collector
}

type GlobalCollector struct {
	*BaseCollector
	versionThershold types.TS
}

func NewGlobalCollector(end types.TS, versionInterval time.Duration) *GlobalCollector {
	versionThresholdTS := types.BuildTS(end.Physical()-versionInterval.Nanoseconds(), end.Logical())
	collector := &GlobalCollector{
		BaseCollector: &BaseCollector{
			LoopProcessor: new(catalog.LoopProcessor),
			data:          NewCheckpointData(),
			end:           end,
		},
		versionThershold: versionThresholdTS,
	}
	collector.DatabaseFn = collector.VisitDB
	collector.TableFn = collector.VisitTable
	collector.SegmentFn = collector.VisitSeg
	collector.BlockFn = collector.VisitBlk
	return collector
}

func (data *CheckpointData) ApplyReplayTo(
	c *catalog.Catalog,
	dataFactory catalog.DataFactory,
) (err error) {
	c.OnReplayDatabaseBatch(data.GetDBBatchs())
	ins, colins, dnins, del, dndel := data.GetTblBatchs()
	c.OnReplayTableBatch(ins, colins, dnins, del, dndel, dataFactory)
	ins, dnins, del, dndel = data.GetSegBatchs()
	c.OnReplaySegmentBatch(ins, dnins, del, dndel, dataFactory)
	ins, dnins, del, dndel = data.GetDNBlkBatchs()
	c.OnReplayBlockBatch(ins, dnins, del, dndel, dataFactory)
	ins, dnins, del, dndel = data.GetBlkBatchs()
	c.OnReplayBlockBatch(ins, dnins, del, dndel, dataFactory)
	return
}

type CNCheckpointData struct {
	meta map[uint64]*CheckpointMeta
	bats [MaxIDX]*batch.Batch
}

func NewCNCheckpointData() *CNCheckpointData {
	return &CNCheckpointData{
		meta: make(map[uint64]*CheckpointMeta),
	}
}

func swithCheckpointIdx(i uint16, tableID uint64) uint16 {
	idx := uint16(i)

	if i == BlockInsert {
		idx = BLKMetaInsertIDX
	} else if i == BlockDelete {
		idx = BLKMetaDeleteIDX
	} else if i == CNBlockInsert {
		idx = BLKCNMetaInsertIDX
	} else if i == SegmentDelete {
		idx = SEGDeleteIDX
	}
	switch tableID {
	case pkgcatalog.MO_DATABASE_ID:
		if i == BlockInsert {
			idx = DBInsertIDX
		} else if i == BlockDelete {
			idx = DBDeleteIDX
		}
	case pkgcatalog.MO_TABLES_ID:
		if i == BlockInsert {
			idx = TBLInsertIDX
		} else if i == BlockDelete {
			idx = TBLDeleteIDX
		}
	case pkgcatalog.MO_COLUMNS_ID:
		if i == BlockInsert {
			idx = TBLColInsertIDX
		} else if i == BlockDelete {
			idx = TBLColDeleteIDX
		}
	}
	return idx
}

func (data *CNCheckpointData) InitMetaIdx(ctx context.Context, version uint32, reader *blockio.BlockReader, location objectio.Location, m *mpool.MPool) error {
	if data.bats[MetaIDX] == nil {
		metaIdx := checkpointDataReferVersions[version][MetaIDX]
		metaBats, err := LoadCNSubBlkColumnsByMeta(version, ctx, metaIdx.types, metaIdx.attrs, MetaIDX, reader, nil)
		if err != nil {
			return err
		}
		data.bats[MetaIDX] = metaBats[0]
		if version < CheckpointVersion5 {
			err = data.fillInMetaBatchWithLocation(location, m)
			if err != nil {
				return err
			}
		}
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
	pref, err = blockio.BuildSubPrefetchParams(service, key)
	if err != nil {
		return
	}
	pref.AddBlockWithType(idxes, []uint16{0}, MetaIDX)

	return blockio.PrefetchWithMerged(pref)
}

func (data *CNCheckpointData) PrefetchMetaFrom(
	ctx context.Context,
	version uint32,
	location objectio.Location,
	service fileservice.FileService,
	tableID uint64) (err error) {
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
		err = blockio.PrefetchMeta(service, location)
	}
	return err
}

func (data *CNCheckpointData) PrefetchFrom(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
	key objectio.Location,
	tableID uint64) (err error) {
	// if version < CheckpointVersion4 {
	// 	return prefetchCheckpointData(ctx, version, service, key)
	// }
	meta := data.GetTableMeta(tableID, version, key)
	if meta == nil {
		return
	}
	var pref blockio.PrefetchParams
	var location objectio.Location
	// for ver less than 5, some tablemeta is empty
	empty := true
	for i, table := range meta.tables {
		if table == nil {
			continue
		}
		if i > BlockDelete {
			if tableID == pkgcatalog.MO_DATABASE_ID ||
				tableID == pkgcatalog.MO_TABLES_ID ||
				tableID == pkgcatalog.MO_COLUMNS_ID {
				break
			}
		}
		idx := swithCheckpointIdx(uint16(i), tableID)
		schema := checkpointDataReferVersions[version][uint32(idx)]
		idxes := make([]uint16, len(schema.attrs))
		for attr := range schema.attrs {
			idxes[attr] = uint16(attr)
		}
		it := table.locations.MakeIterator()
		for it.HasNext() {
			block := it.Next()
			if location.IsEmpty() {
				location = block.GetLocation()
				pref, err = blockio.BuildSubPrefetchParams(service, location)
				if err != nil {
					return
				}
			}
			pref.AddBlockWithType(idxes, []uint16{block.GetID()}, idx)
			empty = false
		}
	}
	if empty {
		return
	}
	return blockio.PrefetchWithMerged(pref)
}
func (data *CNCheckpointData) isMOCatalogTables(tid uint64) bool {
	return tid == pkgcatalog.MO_DATABASE_ID || tid == pkgcatalog.MO_TABLES_ID || tid == pkgcatalog.MO_COLUMNS_ID
}
func (data *CNCheckpointData) GetTableMeta(tableID uint64, version uint32, loc objectio.Location) (meta *CheckpointMeta) {
	if len(data.meta) != 0 {
		meta = data.meta[tableID]
		return
	}
	if version <= CheckpointVersion4 && data.isMOCatalogTables(tableID) {
		tableMeta := NewCheckpointMeta()
		switch tableID {
		case pkgcatalog.MO_DATABASE_ID:
			insertTableMeta := NewTableMeta()
			insertBlockLoc := BuildBlockLoactionWithLocation(loc.Name(), loc.Extent(), 0, DBInsertIDX, 0, 0)
			insertTableMeta.locations = make([]byte, 0)
			insertTableMeta.locations.Append(insertBlockLoc)
			tableMeta.tables[BlockInsert] = insertTableMeta

			deleteTableMeta := NewTableMeta()
			deleteBlockLoc := BuildBlockLoactionWithLocation(loc.Name(), loc.Extent(), 0, DBDeleteIDX, 0, 0)
			deleteTableMeta.locations = make([]byte, 0)
			deleteTableMeta.locations.Append(deleteBlockLoc)
			tableMeta.tables[BlockDelete] = deleteTableMeta
		case pkgcatalog.MO_TABLES_ID:
			insertTableMeta := NewTableMeta()
			insertBlockLoc := BuildBlockLoactionWithLocation(loc.Name(), loc.Extent(), 0, TBLInsertIDX, 0, 0)
			insertTableMeta.locations = make([]byte, 0)
			insertTableMeta.locations.Append(insertBlockLoc)
			tableMeta.tables[BlockInsert] = insertTableMeta

			deleteTableMeta := NewTableMeta()
			deleteBlockLoc := BuildBlockLoactionWithLocation(loc.Name(), loc.Extent(), 0, TBLDeleteIDX, 0, 0)
			deleteTableMeta.locations = make([]byte, 0)
			deleteTableMeta.locations.Append(deleteBlockLoc)
			tableMeta.tables[BlockDelete] = deleteTableMeta
		case pkgcatalog.MO_COLUMNS_ID:
			insertTableMeta := NewTableMeta()
			insertBlockLoc := BuildBlockLoactionWithLocation(loc.Name(), loc.Extent(), 0, TBLColInsertIDX, 0, 0)
			insertTableMeta.locations = make([]byte, 0)
			insertTableMeta.locations.Append(insertBlockLoc)
			tableMeta.tables[BlockInsert] = insertTableMeta

			deleteTableMeta := NewTableMeta()
			deleteBlockLoc := BuildBlockLoactionWithLocation(loc.Name(), loc.Extent(), 0, TBLColDeleteIDX, 0, 0)
			deleteTableMeta.locations = make([]byte, 0)
			deleteTableMeta.locations.Append(deleteBlockLoc)
			tableMeta.tables[BlockDelete] = deleteTableMeta
		}
		return tableMeta
	}
	tidVec := vector.MustFixedCol[uint64](data.bats[MetaIDX].Vecs[Checkpoint_Meta_TID_IDX])
	blkIns := data.bats[MetaIDX].Vecs[Checkpoint_Meta_Insert_Block_LOC_IDX]
	blkCNIns := data.bats[MetaIDX].Vecs[Checkpoint_Meta_CN_Delete_Block_LOC_IDX]
	blkDel := data.bats[MetaIDX].Vecs[Checkpoint_Meta_Delete_Block_LOC_IDX]
	segDel := data.bats[MetaIDX].Vecs[Checkpoint_Meta_Segment_LOC_IDX]

	var i int
	if version <= CheckpointVersion4 {
		i = -1
		for idx, id := range tidVec {
			if id == tableID {
				i = idx
			}
		}
		if i < 0 {
			return
		}
	} else {
		i = vector.OrderedFindFirstIndexInSortedSlice[uint64](tableID, tidVec)
		if i < 0 {
			return
		}
	}
	tid := tidVec[i]
	blkInsStr := blkIns.GetBytesAt(i)
	blkCNInsStr := blkCNIns.GetBytesAt(i)
	blkDelStr := blkDel.GetBytesAt(i)
	segDelStr := segDel.GetBytesAt(i)
	tableMeta := NewCheckpointMeta()
	if len(blkInsStr) > 0 {
		blkInsertTableMeta := NewTableMeta()
		blkInsertTableMeta.locations = blkInsStr
		// blkInsertOffset
		tableMeta.tables[BlockInsert] = blkInsertTableMeta
	}
	if len(blkCNInsStr) > 0 {
		blkDeleteTableMeta := NewTableMeta()
		blkDeleteTableMeta.locations = blkDelStr
		tableMeta.tables[BlockDelete] = blkDeleteTableMeta
		cnBlkInsTableMeta := NewTableMeta()
		cnBlkInsTableMeta.locations = blkCNInsStr
		tableMeta.tables[CNBlockInsert] = cnBlkInsTableMeta
	} else {
		if tableID == pkgcatalog.MO_DATABASE_ID ||
			tableID == pkgcatalog.MO_TABLES_ID ||
			tableID == pkgcatalog.MO_COLUMNS_ID {
			if len(blkDelStr) > 0 {
				blkDeleteTableMeta := NewTableMeta()
				blkDeleteTableMeta.locations = blkDelStr
				tableMeta.tables[BlockDelete] = blkDeleteTableMeta
			}
		}
	}
	if len(segDelStr) > 0 {
		segDeleteTableMeta := NewTableMeta()
		segDeleteTableMeta.locations = segDelStr
		tableMeta.tables[SegmentDelete] = segDeleteTableMeta
	}

	data.meta[tid] = tableMeta
	meta = data.meta[tableID]
	return
}
func (data *CNCheckpointData) fillInMetaBatchWithLocation(location objectio.Location, m *mpool.MPool) (err error) {
	length := data.bats[MetaIDX].Vecs[2].Length()
	insVec := vector.NewVec(types.T_varchar.ToType())
	cnInsVec := vector.NewVec(types.T_varchar.ToType())
	delVec := vector.NewVec(types.T_varchar.ToType())
	segVec := vector.NewVec(types.T_varchar.ToType())

	blkInsStart := data.bats[MetaIDX].Vecs[Checkpoint_Meta_Insert_Block_Start_IDX]
	blkInsEnd := data.bats[MetaIDX].Vecs[Checkpoint_Meta_Insert_Block_End_IDX]
	blkDelStart := data.bats[MetaIDX].Vecs[Checkpoint_Meta_Delete_Block_Start_IDX]
	blkDelEnd := data.bats[MetaIDX].Vecs[Checkpoint_Meta_Delete_Block_End_IDX]
	segDelStart := data.bats[MetaIDX].Vecs[Checkpoint_Meta_Segment_Start_IDX]
	segDelEnd := data.bats[MetaIDX].Vecs[Checkpoint_Meta_Segment_End_IDX]
	for i := 0; i < length; i++ {
		insStart := vector.GetFixedAt[int32](blkInsStart, i)
		insEnd := vector.GetFixedAt[int32](blkInsEnd, i)
		if insStart < insEnd {
			insLoc := BuildBlockLoactionWithLocation(location.Name(), location.Extent(), location.Rows(), BLKMetaInsertIDX, uint64(insStart), uint64(insEnd))
			err = vector.AppendAny(insVec, []byte(insLoc), false, m)
			if err != nil {
				return
			}
		} else {
			err = vector.AppendAny(insVec, nil, true, m)
			if err != nil {
				return
			}
		}

		delStart := vector.GetFixedAt[int32](blkDelStart, i)
		delEnd := vector.GetFixedAt[int32](blkDelEnd, i)
		if delStart < delEnd {
			delLoc := BuildBlockLoactionWithLocation(location.Name(), location.Extent(), location.Rows(), BLKMetaDeleteIDX, uint64(delStart), uint64(delEnd))
			err = vector.AppendAny(delVec, []byte(delLoc), false, m)
			if err != nil {
				return
			}
			cnInsLoc := BuildBlockLoactionWithLocation(location.Name(), location.Extent(), location.Rows(), BLKCNMetaInsertIDX, uint64(delStart), uint64(delEnd))
			err = vector.AppendAny(cnInsVec, []byte(cnInsLoc), false, m)
			if err != nil {
				return
			}
		} else {
			err = vector.AppendAny(delVec, nil, true, m)
			if err != nil {
				return
			}
			err = vector.AppendAny(cnInsVec, nil, true, m)
			if err != nil {
				return
			}
		}

		segStart := vector.GetFixedAt[int32](segDelStart, i)
		segEnd := vector.GetFixedAt[int32](segDelEnd, i)
		if segStart < segEnd {
			segLoc := BuildBlockLoactionWithLocation(location.Name(), location.Extent(), location.Rows(), SEGDeleteIDX, uint64(segStart), uint64(segEnd))
			err = vector.AppendAny(segVec, []byte(segLoc), false, m)
			if err != nil {
				return
			}
		} else {
			err = vector.AppendAny(segVec, nil, true, m)
			if err != nil {
				return
			}
		}
	}

	data.bats[MetaIDX].Vecs[Checkpoint_Meta_Insert_Block_LOC_IDX] = insVec
	data.bats[MetaIDX].Vecs[Checkpoint_Meta_CN_Delete_Block_LOC_IDX] = cnInsVec
	data.bats[MetaIDX].Vecs[Checkpoint_Meta_Delete_Block_LOC_IDX] = delVec
	data.bats[MetaIDX].Vecs[Checkpoint_Meta_Segment_LOC_IDX] = segVec
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
	// if err = data.InitMetaIdx(ctx, version, reader,location,m); err != nil {
	// 	return
	// }
	if version <= CheckpointVersion4 {
		if tableID == pkgcatalog.MO_DATABASE_ID || tableID == pkgcatalog.MO_TABLES_ID || tableID == pkgcatalog.MO_COLUMNS_ID {
			dataBats = make([]*batch.Batch, MetaMaxIdx)
			switch tableID {
			case pkgcatalog.MO_DATABASE_ID:
				item := checkpointDataReferVersions[version][DBInsertIDX]
				dataBats[BlockInsert], err = LoadCNSubBlkColumnsByMetaWithId(ctx, item.types, item.attrs, 0, uint16(DBInsertIDX), version, reader, m)
				if err != nil {
					return
				}
				item = checkpointDataReferVersions[version][DBDeleteIDX]
				dataBats[BlockDelete], err = LoadCNSubBlkColumnsByMetaWithId(ctx, item.types, item.attrs, 0, uint16(DBDeleteIDX), version, reader, m)
				if err != nil {
					return
				}
			case pkgcatalog.MO_TABLES_ID:
				item := checkpointDataReferVersions[version][TBLInsertIDX]
				dataBats[BlockInsert], err = LoadCNSubBlkColumnsByMetaWithId(ctx, item.types, item.attrs, 0, uint16(TBLInsertIDX), version, reader, m)
				if err != nil {
					return
				}
				item = checkpointDataReferVersions[version][TBLDeleteIDX]
				dataBats[BlockDelete], err = LoadCNSubBlkColumnsByMetaWithId(ctx, item.types, item.attrs, 0, uint16(TBLDeleteIDX), version, reader, m)
				if err != nil {
					return
				}
			case pkgcatalog.MO_COLUMNS_ID:
				item := checkpointDataReferVersions[version][TBLColInsertIDX]
				dataBats[BlockInsert], err = LoadCNSubBlkColumnsByMetaWithId(ctx, item.types, item.attrs, 0, uint16(TBLColInsertIDX), version, reader, m)
				if err != nil {
					return
				}
				item = checkpointDataReferVersions[version][TBLColDeleteIDX]
				dataBats[BlockDelete], err = LoadCNSubBlkColumnsByMetaWithId(ctx, item.types, item.attrs, 0, uint16(TBLColDeleteIDX), version, reader, m)
				if err != nil {
					return
				}
			}
			if version == CheckpointVersion1 {
				if tableID == pkgcatalog.MO_TABLES_ID {
					bat := dataBats[BlockInsert]
					if bat == nil {
						return
					}
					versionVec := vector.MustFixedCol[uint32](bat.Vecs[pkgcatalog.MO_TABLES_VERSION_IDX+2]) // 2 for rowid and committs
					length := len(versionVec)
					vec := vector.NewVec(types.T_uint32.ToType())
					for i := 0; i < length; i++ {
						err = vector.AppendFixed[uint32](vec, pkgcatalog.CatalogVersion_V1, false, m)
						if err != nil {
							return
						}
					}
					bat.Attrs = append(bat.Attrs, pkgcatalog.SystemRelAttr_CatalogVersion)
					bat.Vecs = append(bat.Vecs, vec)
				}
			}
			if version <= CheckpointVersion2 {
				if tableID == pkgcatalog.MO_DATABASE_ID {
					bat := dataBats[BlockDelete]
					if bat == nil {
						return
					}
					rowIDVec := vector.MustFixedCol[types.Rowid](bat.Vecs[0])
					length := len(rowIDVec)
					pkVec := vector.NewVec(types.T_uint64.ToType())
					for i := 0; i < length; i++ {
						err = vector.AppendFixed[uint64](pkVec, objectio.HackRowidToU64(rowIDVec[i]), false, m)
						if err != nil {
							return
						}
					}
					bat.Attrs = append(bat.Attrs, pkgcatalog.SystemDBAttr_ID)
					bat.Vecs = append(bat.Vecs, pkVec)
				} else if tableID == pkgcatalog.MO_TABLES_ID {
					bat := dataBats[BlockDelete]
					if bat == nil {
						return
					}
					rowIDVec := vector.MustFixedCol[types.Rowid](bat.Vecs[0])
					length := len(rowIDVec)
					pkVec2 := vector.NewVec(types.T_uint64.ToType())
					for i := 0; i < length; i++ {
						err = vector.AppendFixed[uint64](pkVec2, objectio.HackRowidToU64(rowIDVec[i]), false, m)
						if err != nil {
							return
						}
					}
					bat.Attrs = append(bat.Attrs, pkgcatalog.SystemRelAttr_ID)
					bat.Vecs = append(bat.Vecs, pkVec2)
				}

			}
			if version <= CheckpointVersion3 {
				if tableID == pkgcatalog.MO_COLUMNS_ID {
					bat := dataBats[BlockInsert]
					if bat == nil {
						return
					}
					rowIDVec := vector.MustFixedCol[types.Rowid](bat.Vecs[0])
					length := len(rowIDVec)
					enumVec := vector.NewVec(types.New(types.T_varchar, types.MaxVarcharLen, 0))
					for i := 0; i < length; i++ {
						err = vector.AppendAny(enumVec, []byte(""), false, m)
						if err != nil {
							return
						}
					}
					bat.Attrs = append(bat.Attrs, pkgcatalog.SystemColAttr_EnumValues)
					bat.Vecs = append(bat.Vecs, enumVec)
				}
			}
			return
		}
	}
	meta := data.GetTableMeta(tableID, version, location)
	if meta == nil {
		return
	}
	dataBats = make([]*batch.Batch, MetaMaxIdx)
	for i, table := range meta.tables {
		if table == nil {
			continue
		}
		if i > BlockDelete {
			if tableID == pkgcatalog.MO_DATABASE_ID ||
				tableID == pkgcatalog.MO_TABLES_ID ||
				tableID == pkgcatalog.MO_COLUMNS_ID {
				break
			}
		}
		idx := swithCheckpointIdx(uint16(i), tableID)
		it := table.locations.MakeIterator()
		for it.HasNext() {
			block := it.Next()
			var bat *batch.Batch
			schema := checkpointDataReferVersions[version][uint32(idx)]
			reader, err = blockio.NewObjectReader(reader.GetObjectReader().GetObject().GetFs(), block.GetLocation())
			if err != nil {
				return
			}
			bat, err = LoadCNSubBlkColumnsByMetaWithId(ctx, schema.types, schema.attrs, uint16(idx), block.GetID(), version, reader, m)
			if err != nil {
				return
			}
			//logutil.Infof("load block %v: %d-%d to %d", block.GetLocation().String(), block.GetStartOffset(), block.GetEndOffset(), bat.Vecs[0].Length())
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

func (data *CNCheckpointData) GetTableDataFromBats(tid uint64, bats []*batch.Batch) (ins, del, cnIns, segDel *api.Batch, err error) {
	var insTaeBat, delTaeBat, cnInsTaeBat, segDelTaeBat *batch.Batch
	if len(bats) == 0 {
		return
	}
	if tid == pkgcatalog.MO_DATABASE_ID || tid == pkgcatalog.MO_TABLES_ID || tid == pkgcatalog.MO_COLUMNS_ID {
		insTaeBat = bats[BlockInsert]
		delTaeBat = bats[BlockDelete]
		if insTaeBat != nil {
			ins, err = batch.BatchToProtoBatch(insTaeBat)
			if err != nil {
				return
			}
		}
		if delTaeBat != nil {
			del, err = batch.BatchToProtoBatch(delTaeBat)
			if err != nil {
				return
			}
		}
		return
	}

	insTaeBat = bats[BlockInsert]
	if insTaeBat != nil {
		ins, err = batch.BatchToProtoBatch(insTaeBat)
		if err != nil {
			return
		}
	}
	delTaeBat = bats[BlockDelete]
	cnInsTaeBat = bats[CNBlockInsert]
	if delTaeBat != nil {
		del, err = batch.BatchToProtoBatch(delTaeBat)
		if err != nil {
			return
		}
	}
	if cnInsTaeBat != nil {
		cnIns, err = batch.BatchToProtoBatch(cnInsTaeBat)
		if err != nil {
			return
		}
	}
	segDelTaeBat = bats[SegmentDelete]
	if segDelTaeBat != nil {
		segDel, err = batch.BatchToProtoBatch(segDelTaeBat)
		if err != nil {
			return
		}
	}
	return
}

func (data *CNCheckpointData) GetCloseCB(version uint32, m *mpool.MPool) func() {
	return func() {
		if version == CheckpointVersion1 {
			data.closeVector(TBLInsertIDX, pkgcatalog.MO_TABLES_CATALOG_VERSION_IDX+2, m) // 2 for rowid and committs
		}
		if version <= CheckpointVersion2 {
			data.closeVector(DBDeleteIDX, 2, m)
			data.closeVector(TBLDeleteIDX, 2, m)
		}
		if version <= CheckpointVersion3 {
			data.closeVector(TBLColInsertIDX, 25, m)
		}
		if version <= CheckpointVersion4 {
			data.closeVector(MetaIDX, Checkpoint_Meta_Insert_Block_LOC_IDX, m)
			data.closeVector(MetaIDX, Checkpoint_Meta_CN_Delete_Block_LOC_IDX, m)
			data.closeVector(MetaIDX, Checkpoint_Meta_Delete_Block_LOC_IDX, m)
			data.closeVector(MetaIDX, Checkpoint_Meta_Segment_LOC_IDX, m)
		}
	}
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

func (data *CheckpointData) fillInMetaBatchWithLocation(location objectio.Location) {
	length := data.bats[MetaIDX].Vecs[2].Length()
	insVec := containers.MakeVector(types.T_varchar.ToType())
	cnInsVec := containers.MakeVector(types.T_varchar.ToType())
	delVec := containers.MakeVector(types.T_varchar.ToType())
	segVec := containers.MakeVector(types.T_varchar.ToType())

	tidVec := data.bats[MetaIDX].GetVectorByName(SnapshotAttr_TID)
	blkInsStart := data.bats[MetaIDX].GetVectorByName(SnapshotMetaAttr_BlockInsertBatchStart).GetDownstreamVector()
	blkInsEnd := data.bats[MetaIDX].GetVectorByName(SnapshotMetaAttr_BlockInsertBatchEnd).GetDownstreamVector()
	blkDelStart := data.bats[MetaIDX].GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchStart).GetDownstreamVector()
	blkDelEnd := data.bats[MetaIDX].GetVectorByName(SnapshotMetaAttr_BlockInsertBatchStart).GetDownstreamVector()
	segDelStart := data.bats[MetaIDX].GetVectorByName(SnapshotMetaAttr_BlockInsertBatchStart).GetDownstreamVector()
	segDelEnd := data.bats[MetaIDX].GetVectorByName(SnapshotMetaAttr_BlockInsertBatchStart).GetDownstreamVector()
	for i := 0; i < length; i++ {
		insStart := vector.GetFixedAt[int32](blkInsStart, i)
		insEnd := vector.GetFixedAt[int32](blkInsEnd, i)
		if insStart < insEnd {
			insLoc := BuildBlockLoactionWithLocation(location.Name(), location.Extent(), location.Rows(), BLKMetaInsertIDX, uint64(insStart), uint64(insEnd))
			insVec.Append([]byte(insLoc), false)
		} else {
			insVec.Append(nil, true)
		}

		delStart := vector.GetFixedAt[int32](blkDelStart, i)
		delEnd := vector.GetFixedAt[int32](blkDelEnd, i)
		if delStart < delEnd {
			delLoc := BuildBlockLoactionWithLocation(location.Name(), location.Extent(), location.Rows(), BLKMetaDeleteIDX, uint64(delStart), uint64(delEnd))
			delVec.Append([]byte(delLoc), false)
			cnInsLoc := BuildBlockLoactionWithLocation(location.Name(), location.Extent(), location.Rows(), BLKCNMetaInsertIDX, uint64(delStart), uint64(delEnd))
			cnInsVec.Append([]byte(cnInsLoc), false)
		} else {
			delVec.Append(nil, true)
			cnInsVec.Append(nil, true)
		}

		segStart := vector.GetFixedAt[int32](segDelStart, i)
		segEnd := vector.GetFixedAt[int32](segDelEnd, i)
		if segStart < segEnd {
			segLoc := BuildBlockLoactionWithLocation(location.Name(), location.Extent(), location.Rows(), SEGDeleteIDX, uint64(segStart), uint64(segEnd))
			segVec.Append([]byte(segLoc), false)
		} else {
			segVec.Append(nil, true)
		}
	}

	tidVec.Append(uint64(0), false)
	blkLoc := BuildBlockLoactionWithLocation(location.Name(), location.Extent(), location.Rows(), BLKMetaInsertIDX, uint64(0), uint64(0))
	insVec.Append([]byte(blkLoc), false)
	cnInsVec.Append(nil, true)
	delVec.Append(nil, true)
	segVec.Append(nil, true)

	data.bats[MetaIDX].AddVector(SnapshotMetaAttr_BlockInsertBatchLocation, insVec)
	data.bats[MetaIDX].AddVector(SnapshotMetaAttr_BlockCNInsertBatchLocation, cnInsVec)
	data.bats[MetaIDX].AddVector(SnapshotMetaAttr_BlockDeleteBatchLocation, delVec)
	data.bats[MetaIDX].AddVector(SnapshotMetaAttr_SegDeleteBatchLocation, segVec)
}

func (data *CheckpointData) prepareMeta() {
	bat := data.bats[MetaIDX]
	blkInsLoc := bat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchLocation).GetDownstreamVector()

	blkDelLoc := bat.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchLocation).GetDownstreamVector()
	blkCNInsLoc := bat.GetVectorByName(SnapshotMetaAttr_BlockCNInsertBatchLocation).GetDownstreamVector()

	segDelLoc := bat.GetVectorByName(SnapshotMetaAttr_SegDeleteBatchLocation).GetDownstreamVector()

	tidVec := bat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector()
	sortMeta := make([]int, 0)
	for tid := range data.meta {
		sortMeta = append(sortMeta, int(tid))
	}
	sort.Ints(sortMeta)
	for _, tid := range sortMeta {
		vector.AppendFixed[uint64](tidVec, uint64(tid), false, common.DefaultAllocator)
		if data.meta[uint64(tid)].tables[BlockInsert] == nil {
			vector.AppendBytes(blkInsLoc, nil, true, common.DefaultAllocator)
		} else {
			vector.AppendBytes(blkInsLoc, []byte(data.meta[uint64(tid)].tables[BlockInsert].locations), false, common.DefaultAllocator)
		}
		if data.meta[uint64(tid)].tables[BlockDelete] == nil {
			vector.AppendBytes(blkDelLoc, nil, true, common.DefaultAllocator)
		} else {
			vector.AppendBytes(blkDelLoc, []byte(data.meta[uint64(tid)].tables[BlockDelete].locations), false, common.DefaultAllocator)
		}
		if data.meta[uint64(tid)].tables[CNBlockInsert] == nil {
			vector.AppendBytes(blkCNInsLoc, nil, true, common.DefaultAllocator)
		} else {
			vector.AppendBytes(blkCNInsLoc, []byte(data.meta[uint64(tid)].tables[CNBlockInsert].locations), false, common.DefaultAllocator)
		}
		if data.meta[uint64(tid)].tables[SegmentDelete] == nil {
			vector.AppendBytes(segDelLoc, nil, true, common.DefaultAllocator)
		} else {
			vector.AppendBytes(segDelLoc, []byte(data.meta[uint64(tid)].tables[SegmentDelete].locations), false, common.DefaultAllocator)
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
				panic(fmt.Sprintf("logic error interval %v, start %d, end %d", meta.tables[BlockDelete].ClosedInterval, start, end))
			}
		}
	}
}
func (data *CheckpointData) updateMOCatalog(tid uint64, insStart, insEnd, delStart, delEnd int32) {
	if delEnd <= delStart && insEnd <= insStart {
		return
	}
	data.updateTableMeta(tid, BlockInsert, insStart, insEnd)
	data.updateTableMeta(tid, BlockDelete, delStart, delEnd)
}
func (data *CheckpointData) UpdateBlkMeta(tid uint64, insStart, insEnd, delStart, delEnd int32) {
	if delEnd <= delStart && insEnd <= insStart {
		return
	}
	data.updateTableMeta(tid, BlockInsert, insStart, insEnd)
	data.updateTableMeta(tid, BlockDelete, delStart, delEnd)
	data.updateTableMeta(tid, CNBlockInsert, delStart, delEnd)
}

func (data *CheckpointData) UpdateSegMeta(tid uint64, delStart, delEnd int32) {
	if delEnd <= delStart {
		return
	}
	data.updateTableMeta(tid, SegmentDelete, delStart, delEnd)
}

func (data *CheckpointData) PrintData() {
	logutil.Info(BatchToString("BLK-META-DEL-BAT", data.bats[BLKMetaDeleteIDX], true))
	logutil.Info(BatchToString("BLK-META-INS-BAT", data.bats[BLKMetaInsertIDX], true))
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
			bat.Vecs[i] = containers.NewConstNullVector(*bat.Vecs[i].GetType(), length)
		}
	}
}

func (data *CheckpointData) prepareDNMetaBatch(name objectio.ObjectName, blks []objectio.BlockObject, schemaType []uint16) {
	for i, blk := range blks {
		location := objectio.BuildLocation(name, blk.GetExtent(), 0, blk.GetID())
		data.bats[DNMetaIDX].GetVectorByName(CheckpointMetaAttr_BlockLocation).Append([]byte(location), false)
		data.bats[DNMetaIDX].GetVectorByName(CheckpointMetaAttr_SchemaType).Append(schemaType[i], false)
	}
}

func (data *CheckpointData) WriteTo(
	fs fileservice.FileService,
	blockRows int,
) (CNLocation, DNLocation objectio.Location, err error) {
	segmentid := objectio.NewSegmentid()
	name := objectio.BuildObjectName(segmentid, 0)
	writer, err := blockio.NewBlockWriterNew(fs, name, 0, nil)
	if err != nil {
		return
	}
	blockIndexs := make([][]*BlockLocation, MaxIDX)
	schemaTypes := make([]uint16, 0)
	for i := range checkpointDataSchemas_Curr {
		if i == int(MetaIDX) || i == int(DNMetaIDX) {
			continue
		}
		offset := 0
		formatBatch(data.bats[i])
		var block objectio.BlockObject
		var bat *containers.Batch
		if data.bats[i].Length() == 0 {
			if block, err = writer.WriteSubBatch(containers.ToCNBatch(data.bats[i]), objectio.ConvertToSchemaType(uint16(i))); err != nil {
				return
			}
			blockLoc := BuildBlockLoaction(block.GetID(), uint64(offset), uint64(0))
			blockIndexs[i] = append(blockIndexs[i], &blockLoc)
			schemaTypes = append(schemaTypes, uint16(i))
		} else {
			split := containers.NewBatchSplitter(data.bats[i], blockRows)
			for {
				bat, err = split.Next()
				if err != nil {
					break
				}
				if block, err = writer.WriteSubBatch(containers.ToCNBatch(bat), objectio.ConvertToSchemaType(uint16(i))); err != nil {
					return
				}
				Endoffset := offset + bat.Length()
				blockLoc := BuildBlockLoaction(block.GetID(), uint64(offset), uint64(Endoffset))
				blockIndexs[i] = append(blockIndexs[i], &blockLoc)
				schemaTypes = append(schemaTypes, uint16(i))
				offset += bat.Length()
			}
		}
	}
	blks, _, err := writer.Sync(context.Background())

	data.prepareDNMetaBatch(name, blks, schemaTypes)

	for tid, mata := range data.meta {
		for i, table := range mata.tables {
			if table == nil || table.ClosedInterval.Start == table.ClosedInterval.End {
				continue
			}

			if i > BlockDelete {
				if tid == pkgcatalog.MO_DATABASE_ID ||
					tid == pkgcatalog.MO_TABLES_ID ||
					tid == pkgcatalog.MO_COLUMNS_ID {
					break
				}
			}
			idx := swithCheckpointIdx(uint16(i), tid)
			for _, block := range blockIndexs[idx] {
				if table.End <= block.GetStartOffset() {
					break
				}
				if table.Start >= block.GetEndOffset() {
					continue
				}
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
	blockLoc := BuildBlockLoactionWithLocation(
		name, blks[0].GetExtent(), 0, blks[0].GetID(),
		0, 0)
	data.meta[0].tables[0].locations.Append(blockLoc)
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
	if _, err = writer2.WriteSubBatch(
		containers.ToCNBatch(data.bats[MetaIDX]),
		objectio.ConvertToSchemaType(uint16(MetaIDX))); err != nil {
		return
	}
	if err != nil {
		return
	}
	if _, err = writer2.WriteSubBatch(
		containers.ToCNBatch(data.bats[DNMetaIDX]),
		objectio.ConvertToSchemaType(uint16(DNMetaIDX))); err != nil {
		return
	}
	if err != nil {
		return
	}
	blks2, _, err := writer2.Sync(context.Background())
	CNLocation = objectio.BuildLocation(name2, blks2[0].GetExtent(), 0, blks2[0].GetID())
	DNLocation = objectio.BuildLocation(name2, blks2[1].GetExtent(), 0, blks2[1].GetID())
	return
}

func LoadBlkColumnsByMeta(
	version uint32,
	cxt context.Context,
	colTypes []types.Type,
	colNames []string,
	id uint16,
	reader *blockio.BlockReader,
) ([]*containers.Batch, error) {
	idxs := make([]uint16, len(colNames))
	for i := range colNames {
		idxs[i] = uint16(i)
	}
	var err error
	var ioResults []*batch.Batch
	if version <= CheckpointVersion4 {
		ioResults = make([]*batch.Batch, 1)
		ioResults[0], err = reader.LoadColumns(cxt, idxs, nil, id, nil)
	} else {
		ioResults, err = reader.LoadSubColumns(cxt, idxs, nil, id, nil)
	}
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
				vec = containers.MakeVector(colTypes[i])
			} else {
				vec = containers.ToDNVector(pkgVec)
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
	if version <= CheckpointVersion4 {
		ioResults = make([]*batch.Batch, 1)
		ioResults[0], err = reader.LoadColumns(cxt, idxs, nil, id, nil)
	} else {
		ioResults, err = reader.LoadSubColumns(cxt, idxs, nil, id, m)
	}
	if err != nil {
		return nil, err
	}
	for i := range ioResults {
		ioResults[i].Attrs = make([]string, len(colNames))
		copy(ioResults[i].Attrs, colNames)
	}
	return ioResults, nil
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
) (ioResult *batch.Batch, err error) {
	idxs := make([]uint16, len(colNames))
	for i := range colNames {
		idxs[i] = uint16(i)
	}
	if version <= CheckpointVersion3 {
		ioResult, err = reader.LoadColumns(cxt, idxs, nil, id, nil)
	} else {
		ioResult, err = reader.LoadOneSubColumns(cxt, idxs, nil, dataType, id, m)
	}
	if err != nil {
		return nil, err
	}
	ioResult.Attrs = make([]string, len(colNames))
	copy(ioResult.Attrs, colNames)
	return ioResult, nil
}
func (data *CheckpointData) ReadDNMetaBatch(
	ctx context.Context,
	version uint32,
	location objectio.Location,
	reader *blockio.BlockReader,
) (err error) {
	if data.bats[DNMetaIDX].Length() == 0 {
		if version < CheckpointVersion5 {
			for i := 2; i < MetaMaxIdx; i++ {
				location := objectio.BuildLocation(location.Name(), location.Extent(), 0, uint16(i))
				data.bats[DNMetaIDX].GetVectorByName(CheckpointMetaAttr_BlockLocation).Append([]byte(location), false)
				data.bats[DNMetaIDX].GetVectorByName(CheckpointMetaAttr_SchemaType).Append(uint16(i), false)
			}
		} else {
			var bats []*containers.Batch
			item := checkpointDataReferVersions[version][DNMetaIDX]
			bats, err = LoadBlkColumnsByMeta(version, ctx, item.types, item.attrs, DNMetaIDX, reader)
			if err != nil {
				return
			}
			// logutil.Infof("bats[0].Vecs[1].String() is %v", bats[0].Vecs[0].String())
			data.bats[DNMetaIDX] = bats[0]
		}
	}
	return
}

func (data *CheckpointData) PrefetchMeta(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
	key objectio.Location) (err error) {
	if version < CheckpointVersion4 {
		return
	}
	var pref blockio.PrefetchParams
	pref, err = blockio.BuildSubPrefetchParams(service, key)
	if err != nil {
		return
	}
	meteIdxSchema := checkpointDataReferVersions[version][MetaIDX]
	dnMeteIdxSchema := checkpointDataReferVersions[version][DNMetaIDX]
	idxes := make([]uint16, 0)
	dnIdxes := make([]uint16, 0)
	for attr := range meteIdxSchema.attrs {
		idxes = append(idxes, uint16(attr))
	}
	for attr := range dnMeteIdxSchema.attrs {
		dnIdxes = append(dnIdxes, uint16(attr))
	}
	pref.AddBlockWithType(idxes, []uint16{0}, MetaIDX)
	pref.AddBlockWithType(dnIdxes, []uint16{1}, DNMetaIDX)
	return blockio.PrefetchWithMerged(pref)
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
	if version < CheckpointVersion4 {
		return prefetchCheckpointData(ctx, version, service, key)
	}
	blocks := vector.MustBytesCol(data.bats[DNMetaIDX].GetVectorByName(CheckpointMetaAttr_BlockLocation).GetDownstreamVector())
	dataType := vector.MustFixedCol[uint16](data.bats[DNMetaIDX].GetVectorByName(CheckpointMetaAttr_SchemaType).GetDownstreamVector())
	var pref blockio.PrefetchParams
	locations := make(map[string][]blockIdx)
	for i := 0; i < len(blocks); i++ {
		location := objectio.Location(blocks[i])
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
		pref, err = blockio.BuildSubPrefetchParams(service, blockIdxes[0].location)
		if err != nil {
			return
		}
		for _, idx := range blockIdxes {
			schema := checkpointDataReferVersions[version][idx.dataType]
			idxes := make([]uint16, len(schema.attrs))
			for attr := range schema.attrs {
				idxes[attr] = uint16(attr)
			}
			pref.AddBlockWithType(idxes, []uint16{idx.location.ID()}, idx.dataType)
		}
		err = blockio.PrefetchWithMerged(pref)
		if err != nil {
			logutil.Warnf("PrefetchFrom PrefetchWithMerged error %v", err)
		}
	}
	return
}

func prefetchCheckpointData(
	ctx context.Context,
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
	return blockio.PrefetchWithMerged(pref)
}

// TODO:
// There need a global io pool
func (data *CheckpointData) ReadFrom(
	ctx context.Context,
	version uint32,
	location objectio.Location,
	reader *blockio.BlockReader,
	fs fileservice.FileService,
	m *mpool.MPool,
) (err error) {
	err = data.readMetaBatch(ctx, version, reader, m)
	if err != nil {
		return
	}
	if version <= CheckpointVersion4 {
		data.fillInMetaBatchWithLocation(location)
	}
	err = data.readAll(ctx, version, fs)
	if err != nil {
		return
	}

	return
}

func (data *CheckpointData) readMetaBatch(
	ctx context.Context,
	version uint32,
	reader *blockio.BlockReader,
	m *mpool.MPool,
) (err error) {
	if data.bats[MetaIDX].Length() == 0 {
		var bats []*containers.Batch
		item := checkpointDataReferVersions[version][MetaIDX]
		bats, err = LoadBlkColumnsByMeta(version, ctx, item.types, item.attrs, uint16(0), reader)
		if err != nil {
			return
		}
		// logutil.Infof("bats[0].Vecs[1].String() is %v", bats[0].Vecs[0].String())
		data.bats[MetaIDX] = bats[0]
	}
	return
}

func (data *CheckpointData) replayMetaBatch() {
	bat := data.bats[MetaIDX]
	data.locations = make(map[string]objectio.Location)
	tidVec := vector.MustFixedCol[uint64](bat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector())
	insVec := vector.MustBytesCol(bat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchLocation).GetDownstreamVector())
	delVec := vector.MustBytesCol(bat.GetVectorByName(SnapshotMetaAttr_BlockCNInsertBatchLocation).GetDownstreamVector())
	delCNVec := vector.MustBytesCol(bat.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchLocation).GetDownstreamVector())
	segVec := vector.MustBytesCol(bat.GetVectorByName(SnapshotMetaAttr_SegDeleteBatchLocation).GetDownstreamVector())

	for i := 0; i < data.bats[MetaIDX].GetVectorByName(SnapshotAttr_TID).Length(); i++ {
		tid := tidVec[i]
		if tid == 0 {
			bl := BlockLocation(insVec[i])
			loc := bl.GetLocation()
			data.locations[loc.Name().String()] = loc
			continue
		}
		insLocation := insVec[i]
		delLocation := delVec[i]
		delCNLocation := delCNVec[i]
		segLocation := segVec[i]

		tableMeta := NewCheckpointMeta()
		tableMeta.DecodeFromString([][]byte{insLocation, delLocation, delCNLocation, segLocation})
		data.meta[tid] = tableMeta
	}

	for _, meta := range data.meta {
		for _, table := range meta.tables {
			it := table.locations.MakeIterator()
			for it.HasNext() {
				block := it.Next()
				if !block.GetLocation().IsEmpty() {
					data.locations[block.GetLocation().Name().String()] = block.GetLocation()
					return
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
	data.replayMetaBatch()
	for _, val := range data.locations {
		var reader *blockio.BlockReader
		reader, err = blockio.NewObjectReader(service, val)
		if err != nil {
			return
		}
		var bats []*containers.Batch
		for idx := range checkpointDataReferVersions[version] {
			if uint16(idx) == MetaIDX || uint16(idx) == DNMetaIDX {
				continue
			}
			item := checkpointDataReferVersions[version][idx]
			bats, err = LoadBlkColumnsByMeta(version, ctx, item.types, item.attrs, uint16(idx), reader)
			if err != nil {
				return
			}
			if version == CheckpointVersion1 {
				if uint16(idx) == TBLInsertIDX {
					for _, bat := range bats {
						length := bat.GetVectorByName(pkgcatalog.SystemRelAttr_Version).Length()
						vec := containers.MakeVector(types.T_uint32.ToType())
						for i := 0; i < length; i++ {
							vec.Append(pkgcatalog.CatalogVersion_V1, false)
						}
						//Fixme: add vector to batch
						//bat.AddVector(pkgcatalog.SystemRelAttr_CatalogVersion, vec)
					}
				}
			}
			if version <= CheckpointVersion2 {
				if uint16(idx) == DBDeleteIDX {
					for _, bat := range bats {
						rowIDVec := bat.GetVectorByName(catalog.AttrRowID)
						length := rowIDVec.Length()
						pkVec := containers.MakeVector(types.T_uint64.ToType())
						for i := 0; i < length; i++ {
							pkVec.Append(objectio.HackRowidToU64(rowIDVec.Get(i).(types.Rowid)), false)
						}
						bat.Attrs = append(bat.Attrs, pkgcatalog.SystemDBAttr_ID)
					}
				}

				if uint16(idx) == TBLDeleteIDX {
					for _, bat := range bats {
						rowIDVec := bat.GetVectorByName(catalog.AttrRowID)
						length := rowIDVec.Length()
						pkVec2 := containers.MakeVector(types.T_uint64.ToType())
						for i := 0; i < length; i++ {
							pkVec2.Append(objectio.HackRowidToU64(rowIDVec.Get(i).(types.Rowid)), false)
							if err != nil {
								return err
							}
						}
						bat.Attrs = append(bat.Attrs, pkgcatalog.SystemRelAttr_ID)
						bat.Vecs = append(bat.Vecs, pkVec2)
					}
				}

				if uint16(idx) == TBLColDeleteIDX {
					for _, bat := range bats {
						rowIDVec := bat.GetVectorByName(catalog.AttrRowID)
						length := rowIDVec.Length()
						pkVec2 := containers.MakeVector(types.T_uint64.ToType())
						for i := 0; i < length; i++ {
							pkVec2.Append(nil, true)
							if err != nil {
								return err
							}
						}
						bat.Attrs = append(bat.Attrs, pkgcatalog.SystemColAttr_UniqName)
						bat.Vecs = append(bat.Vecs, pkVec2)
					}
				}
			}
			if version <= CheckpointVersion3 {
				if uint16(idx) == TBLColInsertIDX {
					for _, bat := range bats {
						length := bat.GetVectorByName(catalog.AttrRowID).Length()
						vec := containers.MakeVector(types.New(types.T_varchar, types.MaxVarcharLen, 0))
						for i := 0; i < length; i++ {
							vec.Append([]byte(""), false)
						}
						bat.AddVector(pkgcatalog.SystemColAttr_EnumValues, vec)
					}
				}
			}
			for i := range bats {
				data.bats[idx].Append(bats[i])
			}
		}
	}
	return
}

func (data *CheckpointData) Close() {
	for idx := range data.bats {
		if data.bats[idx] != nil {
			data.bats[idx].Close()
			data.bats[idx] = nil
		}
	}
}

func (data *CheckpointData) CloseWhenLoadFromCache(version uint32) {
	if version == CheckpointVersion1 {
		bat := data.bats[TBLInsertIDX]
		if bat == nil {
			return
		}
		vec := data.bats[TBLInsertIDX].GetVectorByName(pkgcatalog.SystemRelAttr_CatalogVersion)
		vec.Close()
	}
	if version <= CheckpointVersion2 {
		bat := data.bats[DBDeleteIDX]
		if bat == nil {
			return
		}
		vec := data.bats[DBDeleteIDX].GetVectorByName(pkgcatalog.SystemDBAttr_ID)
		vec.Close()
		bat = data.bats[TBLDeleteIDX]
		if bat == nil {
			return
		}
		vec = data.bats[TBLDeleteIDX].GetVectorByName(pkgcatalog.SystemRelAttr_ID)
		vec.Close()
	}
	if version <= CheckpointVersion3 {
		bat := data.bats[TBLColInsertIDX]
		if bat == nil {
			return
		}
		vec := data.bats[TBLColInsertIDX].GetVectorByName(pkgcatalog.SystemColAttr_EnumValues)
		vec.Close()
	}
	if version < CheckpointVersion5 {
		bat := data.bats[MetaIDX]
		if bat == nil {
			return
		}
		vec := data.bats[MetaIDX].GetVectorByName(SnapshotMetaAttr_BlockInsertBatchLocation)
		vec.Close()
		vec = data.bats[MetaIDX].GetVectorByName(SnapshotMetaAttr_BlockCNInsertBatchLocation)
		vec.Close()
		vec = data.bats[MetaIDX].GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchLocation)
		vec.Close()
		vec = data.bats[MetaIDX].GetVectorByName(SnapshotMetaAttr_SegDeleteBatchLocation)
		vec.Close()

		bat = data.bats[DNMetaIDX]
		if bat == nil {
			return
		}
		vec = data.bats[MetaIDX].GetVectorByName(CheckpointMetaAttr_BlockLocation)
		vec.Close()
		vec = data.bats[MetaIDX].GetVectorByName(CheckpointMetaAttr_SchemaType)
		vec.Close()
	}
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
func (data *CheckpointData) GetSegBatchs() (
	*containers.Batch,
	*containers.Batch,
	*containers.Batch,
	*containers.Batch) {
	return data.bats[SEGInsertIDX],
		data.bats[SEGInsertTxnIDX],
		data.bats[SEGDeleteIDX],
		data.bats[SEGDeleteTxnIDX]
}
func (data *CheckpointData) GetBlkBatchs() (
	*containers.Batch,
	*containers.Batch,
	*containers.Batch,
	*containers.Batch) {
	return data.bats[BLKMetaInsertIDX],
		data.bats[BLKMetaInsertTxnIDX],
		data.bats[BLKMetaDeleteIDX],
		data.bats[BLKMetaDeleteTxnIDX]
}
func (data *CheckpointData) GetDNBlkBatchs() (
	*containers.Batch,
	*containers.Batch,
	*containers.Batch,
	*containers.Batch) {
	return data.bats[BLKDNMetaInsertIDX],
		data.bats[BLKDNMetaInsertTxnIDX],
		data.bats[BLKDNMetaDeleteIDX],
		data.bats[BLKDNMetaDeleteTxnIDX]
}

func (collector *BaseCollector) VisitDB(entry *catalog.DBEntry) error {
	if shouldIgnoreDBInLogtail(entry.ID) {
		return nil
	}
	entry.RLock()
	mvccNodes := entry.ClonePreparedInRange(collector.start, collector.end)
	entry.RUnlock()
	delStart := collector.data.bats[DBDeleteIDX].GetVectorByName(catalog.AttrRowID).Length()
	insStart := collector.data.bats[DBInsertIDX].GetVectorByName(catalog.AttrRowID).Length()
	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		dbNode := node
		var created, dropped bool
		if dbNode.HasDropCommitted() {
			dropped = true
			if dbNode.CreatedAt.Equal(dbNode.DeletedAt) {
				created = true
			}
		} else {
			created = true
		}
		if dropped {
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
	delEnd := collector.data.bats[DBDeleteIDX].GetVectorByName(catalog.AttrRowID).Length()
	insEnd := collector.data.bats[DBInsertIDX].GetVectorByName(catalog.AttrRowID).Length()
	collector.data.updateMOCatalog(pkgcatalog.MO_DATABASE_ID, int32(insStart), int32(insEnd), int32(delStart), int32(delEnd))
	return nil
}
func (collector *GlobalCollector) isEntryDeletedBeforeThreshold(entry catalog.BaseEntry) bool {
	entry.RLock()
	defer entry.RUnlock()
	return entry.DeleteBefore(collector.versionThershold)
}
func (collector *GlobalCollector) VisitDB(entry *catalog.DBEntry) error {
	if collector.isEntryDeletedBeforeThreshold(entry.BaseEntryImpl) {
		return nil
	}
	return collector.BaseCollector.VisitDB(entry)
}

func (collector *BaseCollector) VisitTable(entry *catalog.TableEntry) (err error) {
	if shouldIgnoreTblInLogtail(entry.ID) {
		return nil
	}
	entry.RLock()
	mvccNodes := entry.ClonePreparedInRange(collector.start, collector.end)
	entry.RUnlock()
	tableColDelBat := collector.data.bats[TBLColDeleteIDX]
	tableDelTxnBat := collector.data.bats[TBLDeleteTxnIDX]
	tableDelBat := collector.data.bats[TBLDeleteIDX]
	tableColInsBat := collector.data.bats[TBLColInsertIDX]
	tableInsBat := collector.data.bats[TBLInsertIDX]
	tableColInsTxnBat := collector.data.bats[TBLInsertTxnIDX]
	tblDelStart := collector.data.bats[TBLDeleteIDX].GetVectorByName(catalog.AttrRowID).Length()
	tblInsStart := collector.data.bats[TBLInsertIDX].GetVectorByName(catalog.AttrRowID).Length()
	colDelStart := collector.data.bats[TBLColDeleteIDX].GetVectorByName(catalog.AttrRowID).Length()
	colInsStart := collector.data.bats[TBLColInsertIDX].GetVectorByName(catalog.AttrRowID).Length()
	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		tblNode := node
		var created, dropped bool
		if tblNode.HasDropCommitted() {
			dropped = true
			if tblNode.CreatedAt.Equal(tblNode.DeletedAt) {
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
				tableColDelBat.GetVectorByName(catalog.AttrRowID).Append(objectio.HackBytes2Rowid([]byte(fmt.Sprintf("%d-%s", entry.GetID(), name))), false)
				tableColDelBat.GetVectorByName(catalog.AttrCommitTs).Append(tblNode.GetEnd(), false)
			}
			rowidVec := tableColInsBat.GetVectorByName(catalog.AttrRowID)
			commitVec := tableColInsBat.GetVectorByName(catalog.AttrCommitTs)
			for _, usercol := range tblNode.BaseNode.Schema.ColDefs {
				rowidVec.Append(objectio.HackBytes2Rowid([]byte(fmt.Sprintf("%d-%s", entry.GetID(), usercol.Name))), false)
				commitVec.Append(tblNode.GetEnd(), false)
			}

			tableColInsTxnBat.GetVectorByName(
				SnapshotAttr_BlockMaxRow).Append(entry.GetLastestSchema().BlockMaxRows, false)
			tableColInsTxnBat.GetVectorByName(
				SnapshotAttr_SegmentMaxBlock).Append(entry.GetLastestSchema().SegmentMaxBlocks, false)
			tableColInsTxnBat.GetVectorByName(
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

			tblNode.TxnMVCCNode.AppendTuple(tableColInsTxnBat)
		}
		if dropped {
			tableDelTxnBat.GetVectorByName(
				SnapshotAttr_DBID).Append(entry.GetDB().GetID(), false)
			tableDelTxnBat.GetVectorByName(
				SnapshotAttr_TID).Append(entry.GetID(), false)

			rowidVec := tableColDelBat.GetVectorByName(catalog.AttrRowID)
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
	tblDelEnd := collector.data.bats[TBLDeleteIDX].GetVectorByName(catalog.AttrRowID).Length()
	tblInsEnd := collector.data.bats[TBLInsertIDX].GetVectorByName(catalog.AttrRowID).Length()
	colDelEnd := collector.data.bats[TBLColDeleteIDX].GetVectorByName(catalog.AttrRowID).Length()
	colInsEnd := collector.data.bats[TBLColInsertIDX].GetVectorByName(catalog.AttrRowID).Length()
	collector.data.updateMOCatalog(pkgcatalog.MO_TABLES_ID, int32(tblInsStart), int32(tblInsEnd), int32(tblDelStart), int32(tblDelEnd))
	collector.data.updateMOCatalog(pkgcatalog.MO_COLUMNS_ID, int32(colInsStart), int32(colInsEnd), int32(colDelStart), int32(colDelEnd))
	return nil
}

func (collector *GlobalCollector) VisitTable(entry *catalog.TableEntry) error {
	if collector.isEntryDeletedBeforeThreshold(entry.BaseEntryImpl) {
		return nil
	}
	if collector.isEntryDeletedBeforeThreshold(entry.GetDB().BaseEntryImpl) {
		return nil
	}
	return collector.BaseCollector.VisitTable(entry)
}

func (collector *BaseCollector) VisitSeg(entry *catalog.SegmentEntry) (err error) {
	entry.RLock()
	mvccNodes := entry.ClonePreparedInRange(collector.start, collector.end)
	entry.RUnlock()
	if len(mvccNodes) == 0 {
		return nil
	}
	delStart := collector.data.bats[SEGDeleteIDX].GetVectorByName(catalog.AttrRowID).Length()
	segDelBat := collector.data.bats[SEGDeleteIDX]
	segDelTxn := collector.data.bats[SEGDeleteTxnIDX]
	segInsBat := collector.data.bats[SEGInsertIDX]
	segInsTxn := collector.data.bats[SEGInsertTxnIDX]

	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		segNode := node
		if segNode.HasDropCommitted() {
			vector.AppendFixed(
				segDelBat.GetVectorByName(catalog.AttrRowID).GetDownstreamVector(),
				objectio.HackSegid2Rowid(&entry.ID),
				false,
				common.DefaultAllocator,
			)
			vector.AppendFixed(
				segDelBat.GetVectorByName(catalog.AttrCommitTs).GetDownstreamVector(),
				segNode.GetEnd(),
				false,
				common.DefaultAllocator,
			)
			vector.AppendFixed(
				segDelTxn.GetVectorByName(SnapshotAttr_DBID).GetDownstreamVector(),
				entry.GetTable().GetDB().GetID(),
				false,
				common.DefaultAllocator,
			)
			vector.AppendFixed(
				segDelTxn.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector(),
				entry.GetTable().GetID(),
				false,
				common.DefaultAllocator,
			)
			segNode.TxnMVCCNode.AppendTuple(segDelTxn)
		} else {
			vector.AppendFixed(
				segInsBat.GetVectorByName(SegmentAttr_ID).GetDownstreamVector(),
				entry.ID,
				false,
				common.DefaultAllocator,
			)
			vector.AppendFixed(
				segInsBat.GetVectorByName(SegmentAttr_CreateAt).GetDownstreamVector(),
				segNode.GetEnd(),
				false,
				common.DefaultAllocator,
			)
			buf := &bytes.Buffer{}
			if _, err := entry.SegmentNode.WriteTo(buf); err != nil {
				return err
			}
			vector.AppendBytes(
				segInsBat.GetVectorByName(SegmentAttr_SegNode).GetDownstreamVector(),
				buf.Bytes(),
				false,
				common.DefaultAllocator,
			)
			vector.AppendFixed(
				segInsTxn.GetVectorByName(SnapshotAttr_DBID).GetDownstreamVector(),
				entry.GetTable().GetDB().GetID(),
				false,
				common.DefaultAllocator,
			)
			vector.AppendFixed(
				segInsTxn.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector(),
				entry.GetTable().GetID(),
				false,
				common.DefaultAllocator,
			)
			segNode.TxnMVCCNode.AppendTuple(segInsTxn)
		}
	}
	delEnd := segDelBat.GetVectorByName(catalog.AttrRowID).Length()
	collector.data.UpdateSegMeta(entry.GetTable().ID, int32(delStart), int32(delEnd))
	return nil
}

func (collector *GlobalCollector) VisitSeg(entry *catalog.SegmentEntry) error {
	if collector.isEntryDeletedBeforeThreshold(entry.BaseEntryImpl) {
		return nil
	}
	if collector.isEntryDeletedBeforeThreshold(entry.GetTable().BaseEntryImpl) {
		return nil
	}
	if collector.isEntryDeletedBeforeThreshold(entry.GetTable().GetDB().BaseEntryImpl) {
		return nil
	}
	return collector.BaseCollector.VisitSeg(entry)
}

func (collector *BaseCollector) VisitBlk(entry *catalog.BlockEntry) (err error) {
	entry.RLock()
	mvccNodes := entry.ClonePreparedInRange(collector.start, collector.end)
	entry.RUnlock()
	if len(mvccNodes) == 0 {
		return nil
	}
	insStart := collector.data.bats[BLKMetaInsertIDX].GetVectorByName(catalog.AttrRowID).Length()
	delStart := collector.data.bats[BLKMetaDeleteIDX].GetVectorByName(catalog.AttrRowID).Length()
	blkDNMetaDelBat := collector.data.bats[BLKDNMetaDeleteIDX]
	blkDNMetaDelTxnBat := collector.data.bats[BLKDNMetaDeleteTxnIDX]
	blkDNMetaInsBat := collector.data.bats[BLKDNMetaInsertIDX]
	blkDNMetaInsTxnBat := collector.data.bats[BLKDNMetaInsertTxnIDX]
	blkMetaDelBat := collector.data.bats[BLKMetaDeleteIDX]
	blkMetaDelTxnBat := collector.data.bats[BLKMetaDeleteTxnIDX]
	blkCNMetaInsBat := collector.data.bats[BLKCNMetaInsertIDX]
	blkMetaInsBat := collector.data.bats[BLKMetaInsertIDX]
	blkMetaInsTxnBat := collector.data.bats[BLKMetaInsertTxnIDX]

	blkDNMetaDelRowIDVec := blkDNMetaDelBat.GetVectorByName(catalog.AttrRowID).GetDownstreamVector()
	blkDNMetaDelCommitTsVec := blkDNMetaDelBat.GetVectorByName(catalog.AttrCommitTs).GetDownstreamVector()
	blkDNMetaDelTxnDBIDVec := blkDNMetaDelTxnBat.GetVectorByName(SnapshotAttr_DBID).GetDownstreamVector()
	blkDNMetaDelTxnTIDVec := blkDNMetaDelTxnBat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector()
	blkDNMetaDelTxnMetaLocVec := blkDNMetaDelTxnBat.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).GetDownstreamVector()
	blkDNMetaDelTxnDeltaLocVec := blkDNMetaDelTxnBat.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).GetDownstreamVector()

	blkDNMetaInsRowIDVec := blkDNMetaInsBat.GetVectorByName(catalog.AttrRowID).GetDownstreamVector()
	blkDNMetaInsCommitTimeVec := blkDNMetaInsBat.GetVectorByName(catalog.AttrCommitTs).GetDownstreamVector()
	blkDNMetaInsIDVec := blkDNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_ID).GetDownstreamVector()
	blkDNMetaInsStateVec := blkDNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_EntryState).GetDownstreamVector()
	blkDNMetaInsMetaLocVec := blkDNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).GetDownstreamVector()
	blkDNMetaInsDelLocVec := blkDNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).GetDownstreamVector()
	blkDNMetaInsSortedVec := blkDNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_Sorted).GetDownstreamVector()
	blkDNMetaInsSegIDVec := blkDNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_SegmentID).GetDownstreamVector()
	blkDNMetaInsCommitTsVec := blkDNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_CommitTs).GetDownstreamVector()

	blkDNMetaInsTxnDBIDVec := blkDNMetaInsTxnBat.GetVectorByName(SnapshotAttr_DBID).GetDownstreamVector()
	blkDNMetaInsTxnTIDVec := blkDNMetaInsTxnBat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector()
	blkDNMetaInsTxnMetaLocVec := blkDNMetaInsTxnBat.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).GetDownstreamVector()
	blkDNMetaInsTxnDeltaLocVec := blkDNMetaInsTxnBat.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).GetDownstreamVector()

	blkMetaDelRowIDVec := blkMetaDelBat.GetVectorByName(catalog.AttrRowID).GetDownstreamVector()
	blkMetaDelCommitTsVec := blkMetaDelBat.GetVectorByName(catalog.AttrCommitTs).GetDownstreamVector()

	blkMetaDelTxnDBIDVec := blkMetaDelTxnBat.GetVectorByName(SnapshotAttr_DBID).GetDownstreamVector()
	blkMetaDelTxnTIDVec := blkMetaDelTxnBat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector()
	blkMetaDelTxnMetaLocVec := blkMetaDelTxnBat.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).GetDownstreamVector()
	blkMetaDelTxnDeltaLocVec := blkMetaDelTxnBat.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).GetDownstreamVector()

	blkCNMetaInsRowIDVec := blkCNMetaInsBat.GetVectorByName(catalog.AttrRowID).GetDownstreamVector()
	blkCNMetaInsCommitTimeVec := blkCNMetaInsBat.GetVectorByName(catalog.AttrCommitTs).GetDownstreamVector()
	blkCNMetaInsIDVec := blkCNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_ID).GetDownstreamVector()
	blkCNMetaInsStateVec := blkCNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_EntryState).GetDownstreamVector()
	blkCNMetaInsMetaLocVec := blkCNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).GetDownstreamVector()
	blkCNMetaInsDelLocVec := blkCNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).GetDownstreamVector()
	blkCNMetaInsSortedVec := blkCNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_Sorted).GetDownstreamVector()
	blkCNMetaInsSegIDVec := blkCNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_SegmentID).GetDownstreamVector()
	blkCNMetaInsCommitTsVec := blkCNMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_CommitTs).GetDownstreamVector()

	blkMetaInsRowIDVec := blkMetaInsBat.GetVectorByName(catalog.AttrRowID).GetDownstreamVector()
	blkMetaInsCommitTimeVec := blkMetaInsBat.GetVectorByName(catalog.AttrCommitTs).GetDownstreamVector()
	blkMetaInsIDVec := blkMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_ID).GetDownstreamVector()
	blkMetaInsStateVec := blkMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_EntryState).GetDownstreamVector()
	blkMetaInsMetaLocVec := blkMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).GetDownstreamVector()
	blkMetaInsDelLocVec := blkMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).GetDownstreamVector()
	blkMetaInsSortedVec := blkMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_Sorted).GetDownstreamVector()
	blkMetaInsSegIDVec := blkMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_SegmentID).GetDownstreamVector()
	blkMetaInsCommitTsVec := blkMetaInsBat.GetVectorByName(pkgcatalog.BlockMeta_CommitTs).GetDownstreamVector()

	blkMetaInsTxnDBIDVec := blkMetaInsTxnBat.GetVectorByName(SnapshotAttr_DBID).GetDownstreamVector()
	blkMetaInsTxnTIDVec := blkMetaInsTxnBat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector()
	blkMetaInsTxnMetaLocVec := blkMetaInsTxnBat.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).GetDownstreamVector()
	blkMetaInsTxnDeltaLocVec := blkMetaInsTxnBat.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).GetDownstreamVector()

	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		metaNode := node
		if metaNode.BaseNode.MetaLoc.IsEmpty() || metaNode.Aborted {
			if metaNode.HasDropCommitted() {
				vector.AppendFixed(
					blkDNMetaDelRowIDVec,
					objectio.HackBlockid2Rowid(&entry.ID),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkDNMetaDelCommitTsVec,
					metaNode.GetEnd(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkDNMetaDelTxnDBIDVec,
					entry.GetSegment().GetTable().GetDB().GetID(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkDNMetaDelTxnTIDVec,
					entry.GetSegment().GetTable().GetID(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkDNMetaDelTxnMetaLocVec,
					[]byte(metaNode.BaseNode.MetaLoc),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkDNMetaDelTxnDeltaLocVec,
					[]byte(metaNode.BaseNode.DeltaLoc),
					false,
					common.DefaultAllocator,
				)
				metaNode.TxnMVCCNode.AppendTuple(blkDNMetaDelTxnBat)
			} else {
				vector.AppendFixed(
					blkDNMetaInsIDVec,
					entry.ID,
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkDNMetaInsStateVec,
					entry.IsAppendable(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkDNMetaInsCommitTsVec,
					metaNode.GetEnd(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkDNMetaInsMetaLocVec,
					[]byte(metaNode.BaseNode.MetaLoc),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkDNMetaInsDelLocVec,
					[]byte(metaNode.BaseNode.DeltaLoc),
					false,
					common.DefaultAllocator,
				)
				is_sorted := false
				if !entry.IsAppendable() && entry.GetSchema().HasSortKey() {
					is_sorted = true
				}
				vector.AppendFixed(
					blkDNMetaInsSortedVec,
					is_sorted,
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkDNMetaInsSegIDVec,
					entry.GetSegment().ID,
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkDNMetaInsCommitTimeVec,
					metaNode.CreatedAt,
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkDNMetaInsRowIDVec,
					objectio.HackBlockid2Rowid(&entry.ID),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkDNMetaInsTxnDBIDVec,
					entry.GetSegment().GetTable().GetDB().GetID(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkDNMetaInsTxnTIDVec,
					entry.GetSegment().GetTable().GetID(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkDNMetaInsTxnMetaLocVec,
					[]byte(metaNode.BaseNode.MetaLoc),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkDNMetaInsTxnDeltaLocVec,
					[]byte(metaNode.BaseNode.DeltaLoc),
					false,
					common.DefaultAllocator,
				)
				metaNode.TxnMVCCNode.AppendTuple(blkDNMetaInsTxnBat)
			}
		} else {
			if metaNode.HasDropCommitted() {
				vector.AppendFixed(
					blkMetaDelRowIDVec,
					objectio.HackBlockid2Rowid(&entry.ID),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkMetaDelCommitTsVec,
					metaNode.GetEnd(),
					false,
					common.DefaultAllocator,
				)

				vector.AppendFixed(
					blkMetaDelTxnDBIDVec,
					entry.GetSegment().GetTable().GetDB().GetID(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkMetaDelTxnTIDVec,
					entry.GetSegment().GetTable().GetID(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkMetaDelTxnMetaLocVec,
					[]byte(metaNode.BaseNode.MetaLoc),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkMetaDelTxnDeltaLocVec,
					[]byte(metaNode.BaseNode.DeltaLoc),
					false,
					common.DefaultAllocator,
				)
				metaNode.TxnMVCCNode.AppendTuple(blkMetaDelTxnBat)
				is_sorted := false
				if !entry.IsAppendable() && entry.GetSchema().HasSortKey() {
					is_sorted = true
				}
				vector.AppendFixed(
					blkCNMetaInsIDVec,
					entry.ID,
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkCNMetaInsStateVec,
					entry.IsAppendable(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkCNMetaInsMetaLocVec,
					[]byte(metaNode.BaseNode.MetaLoc),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkCNMetaInsDelLocVec,
					[]byte(metaNode.BaseNode.DeltaLoc),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkCNMetaInsSortedVec,
					is_sorted,
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkCNMetaInsSegIDVec,
					entry.GetSegment().ID,
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkCNMetaInsCommitTsVec,
					metaNode.GetEnd(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkCNMetaInsRowIDVec,
					objectio.HackBlockid2Rowid(&entry.ID),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkCNMetaInsCommitTimeVec,
					metaNode.CreatedAt,
					false,
					common.DefaultAllocator,
				)
			} else {
				is_sorted := false
				if !entry.IsAppendable() && entry.GetSchema().HasSortKey() {
					is_sorted = true
				}
				vector.AppendFixed(
					blkMetaInsIDVec,
					entry.ID,
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkMetaInsStateVec,
					entry.IsAppendable(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkMetaInsMetaLocVec,
					[]byte(metaNode.BaseNode.MetaLoc),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkMetaInsDelLocVec,
					[]byte(metaNode.BaseNode.DeltaLoc),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkMetaInsCommitTsVec,
					metaNode.GetEnd(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkMetaInsSortedVec,
					is_sorted,
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkMetaInsSegIDVec,
					entry.GetSegment().ID,
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkMetaInsCommitTimeVec,
					metaNode.CreatedAt,
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkMetaInsRowIDVec,
					objectio.HackBlockid2Rowid(&entry.ID),
					false,
					common.DefaultAllocator,
				)

				vector.AppendFixed(
					blkMetaInsTxnDBIDVec,
					entry.GetSegment().GetTable().GetDB().GetID(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendFixed(
					blkMetaInsTxnTIDVec,
					entry.GetSegment().GetTable().GetID(),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkMetaInsTxnMetaLocVec,
					[]byte(metaNode.BaseNode.MetaLoc),
					false,
					common.DefaultAllocator,
				)
				vector.AppendBytes(
					blkMetaInsTxnDeltaLocVec,
					[]byte(metaNode.BaseNode.DeltaLoc),
					false,
					common.DefaultAllocator,
				)

				metaNode.TxnMVCCNode.AppendTuple(blkMetaInsTxnBat)
			}
		}
	}
	insEnd := collector.data.bats[BLKMetaInsertIDX].GetVectorByName(catalog.AttrRowID).Length()
	delEnd := collector.data.bats[BLKMetaDeleteIDX].GetVectorByName(catalog.AttrRowID).Length()
	collector.data.UpdateBlkMeta(entry.GetSegment().GetTable().ID, int32(insStart), int32(insEnd), int32(delStart), int32(delEnd))
	return nil
}

func (collector *GlobalCollector) VisitBlk(entry *catalog.BlockEntry) error {
	if collector.isEntryDeletedBeforeThreshold(entry.BaseEntryImpl) {
		return nil
	}
	if collector.isEntryDeletedBeforeThreshold(entry.GetSegment().BaseEntryImpl) {
		return nil
	}
	if collector.isEntryDeletedBeforeThreshold(entry.GetSegment().GetTable().BaseEntryImpl) {
		return nil
	}
	if collector.isEntryDeletedBeforeThreshold(entry.GetSegment().GetTable().GetDB().BaseEntryImpl) {
		return nil
	}
	return collector.BaseCollector.VisitBlk(entry)
}

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
