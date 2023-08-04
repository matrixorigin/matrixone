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

const CheckpointBlockRows = 10

const (
	CheckpointVersion1 uint32 = 1
	CheckpointVersion2 uint32 = 2
	CheckpointVersion3 uint32 = 3

	CheckpointCurrentVersion = CheckpointVersion3
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
)

const MaxIDX = BLKCNMetaInsertIDX + 1

const (
	Checkpoint_Meta_TID_IDX                 = 2
	Checkpoint_Meta_Insert_Block_LOC_IDX    = 3
	Checkpoint_Meta_CN_Delete_Block_LOC_IDX = 4
	Checkpoint_Meta_Delete_Block_LOC_IDX    = 5
	Checkpoint_Meta_Segment_LOC_IDX         = 6
)

type checkpointDataItem struct {
	schema *catalog.Schema
	types  []types.Type
	attrs  []string
}

var checkpointDataSchemas_V1 [MaxIDX]*catalog.Schema
var checkpointDataSchemas_V2 [MaxIDX]*catalog.Schema
var checkpointDataSchemas_V3 [MaxIDX]*catalog.Schema
var checkpointDataSchemas_Curr [MaxIDX]*catalog.Schema

var checkpointDataRefer_V1 [MaxIDX]*checkpointDataItem
var checkpointDataRefer_V2 [MaxIDX]*checkpointDataItem
var checkpointDataRefer_V3 [MaxIDX]*checkpointDataItem
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
		catalog.SystemColumnSchema,
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
		catalog.SystemColumnSchema,
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
	}
	checkpointDataSchemas_V3 = [MaxIDX]*catalog.Schema{
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
	}

	checkpointDataSchemas_Curr = checkpointDataSchemas_V3
	checkpointDataReferVersions = make(map[uint32][24]*checkpointDataItem)

	for idx, schema := range checkpointDataSchemas_V1 {
		checkpointDataRefer_V1[idx] = &checkpointDataItem{
			schema,
			append(BaseTypes, schema.Types()...),
			append(BaseAttr, schema.AllNames()...),
		}
	}
	checkpointDataReferVersions[CheckpointVersion1] = checkpointDataRefer_V1
	for idx, schema := range checkpointDataSchemas_V2 {
		checkpointDataRefer_V2[idx] = &checkpointDataItem{
			schema,
			append(BaseTypes, schema.Types()...),
			append(BaseAttr, schema.AllNames()...),
		}
	}
	checkpointDataReferVersions[CheckpointVersion2] = checkpointDataRefer_V2
	for idx, schema := range checkpointDataSchemas_V3 {
		checkpointDataRefer_V3[idx] = &checkpointDataItem{
			schema,
			append(BaseTypes, schema.Types()...),
			append(BaseAttr, schema.AllNames()...),
		}
	}
	checkpointDataReferVersions[CheckpointVersion3] = checkpointDataRefer_V3
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
	logutil.Infof("end is %d", end)
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
	if m==nil{
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
	if m==nil{
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
	metaLocs  [MaxIDX][]objectio.Location
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

func (data *CNCheckpointData) PrefetchFrom(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
	key objectio.Location) (err error) {
	return prefetchCheckpointData(ctx, version, service, key)
}

func (data *CNCheckpointData) GetTableMeta(tableID uint64) (meta *CheckpointMeta) {
	if len(data.meta) != 0 {
		meta = data.meta[tableID]
		return
	}
	tidVec := vector.MustFixedCol[uint64](data.bats[MetaIDX].Vecs[Checkpoint_Meta_TID_IDX])
	blkIns := data.bats[MetaIDX].Vecs[Checkpoint_Meta_Insert_Block_LOC_IDX]
	blkCNIns := data.bats[MetaIDX].Vecs[Checkpoint_Meta_CN_Delete_Block_LOC_IDX]
	blkDel := data.bats[MetaIDX].Vecs[Checkpoint_Meta_Delete_Block_LOC_IDX]
	segDel := data.bats[MetaIDX].Vecs[Checkpoint_Meta_Segment_LOC_IDX]

	i := vector.OrderedFindFirstIndexInSortedSlice[uint64](tableID, tidVec)
	if i < 0 {
		return
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
	if len(blkDelStr) > 0 {
		blkDeleteTableMeta := NewTableMeta()
		blkDeleteTableMeta.locations = blkDelStr
		tableMeta.tables[BlockDelete] = blkDeleteTableMeta
	}
	if len(blkCNInsStr) > 0 {
		blkDeleteTableMeta := NewTableMeta()
		blkDeleteTableMeta.locations = blkDelStr
		tableMeta.tables[BlockDelete] = blkDeleteTableMeta
		cnBlkInsTableMeta := NewTableMeta()
		cnBlkInsTableMeta.locations = blkCNInsStr
		tableMeta.tables[CNBlockInsert] = cnBlkInsTableMeta
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

func (data *CNCheckpointData) ReadFromData(
	ctx context.Context,
	tableID uint64,
	reader *blockio.BlockReader,
	version uint32,
	m *mpool.MPool,
) (dataBats []*batch.Batch, err error) {

	var metaBats []*batch.Batch
	metaIdx := checkpointDataReferVersions[version][MetaIDX]
	metaBats, err = LoadCNSubBlkColumnsByMeta(ctx, metaIdx.types, metaIdx.attrs, MetaIDX, reader, m)
	if err != nil {
		return
	}
	metaBat := metaBats[0]
	data.bats[MetaIDX] = metaBat
	meta := data.GetTableMeta(tableID)
	if meta == nil {
		return
	}
	dataBats = make([]*batch.Batch, MetaMaxIdx)
	for i, table := range meta.tables {
		if table == nil {
			continue
		}
		idx := uint16(i)
		if i > BlockDelete {
			if tableID == pkgcatalog.MO_DATABASE_ID ||
				tableID == pkgcatalog.MO_TABLES_ID ||
				tableID == pkgcatalog.MO_COLUMNS_ID {
				break
			}
		}

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
		it := table.locations.MakeIterator()
		for it.HasNext() {
			block := it.Next()
			var bat *batch.Batch
			schema := checkpointDataReferVersions[version][uint32(idx)]
			reader, err = blockio.NewObjectReader(reader.GetObjectReader().GetObject().GetFs(), block.GetLocation())
			bat, err = LoadCNSubBlkColumnsByMetaWithId(ctx, schema.types, schema.attrs, uint16(idx), block.GetID(), reader, m)
			if err != nil {
				return
			}
			logutil.Infof("load block %v: %d-%d to %d", block.GetLocation().String(), block.GetStartOffset(), block.GetEndOffset(), bat.Vecs[0].Length())
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

	if tableID == pkgcatalog.MO_DATABASE_ID ||
		tableID == pkgcatalog.MO_TABLES_ID ||
		tableID == pkgcatalog.MO_COLUMNS_ID {
		if version == CheckpointVersion1 {
			bat := data.bats[BlockInsert]
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
		if version <= CheckpointVersion2 {
			if tableID == pkgcatalog.MO_DATABASE_ID {
				bat := data.bats[BlockDelete]
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
				bat := data.bats[BlockDelete]
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
	vec := data.bats[TBLInsertIDX].Vecs[colIdx]
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

func (data *CheckpointData) prepareMeta() {
	bat := data.bats[MetaIDX]
	blkInsLoc := bat.GetVectorByName(SnapshotMetaAttr_BlockInsertBatchLocation).GetDownstreamVector()

	blkDelLoc := bat.GetVectorByName(SnapshotMetaAttr_BlockDeleteBatchLocation).GetDownstreamVector()
	blkCNInsLoc := bat.GetVectorByName(SnapshotMetaAttr_BlockCNInsertBatchLocation).GetDownstreamVector()

	segDelLoc := bat.GetVectorByName(SnapshotMetaAttr_SegDeleteBatchLocation).GetDownstreamVector()

	tidVec := bat.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector()
	var sortMeta []int
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

type blockIndex struct {
	offset *common.ClosedInterval
	id     []uint32
}

func (data *CheckpointData) WriteTo(
	fs fileservice.FileService,
) (location objectio.Location, err error) {
	segmentid := objectio.NewSegmentid()
	name := objectio.BuildObjectName(segmentid, 0)
	writer, err := blockio.NewBlockWriterNew(fs, name, 0, nil)
	if err != nil {
		return
	}
	blockIndexs := make([][]*BlockLocation, MaxIDX)
	for i := range checkpointDataSchemas_Curr {
		if i == int(MetaIDX) {
			continue
		}
		offset := 0
		bats := containers.SplitDNBatch(data.bats[i], CheckpointBlockRows)
		for _, bat := range bats {
			var block objectio.BlockObject
			if block, err = writer.WriteSubBatch(containers.ToCNBatch(bat), objectio.ConvertToSchemaType(uint16(i))); err != nil {
				return
			}
			Endoffset := offset + bat.Length()
			blockLoc := BuildBlockLoaction(block.GetID(), uint64(offset), uint64(Endoffset))
			logutil.Infof("blockLoc is %d-%d", blockLoc.GetStartOffset(), blockLoc.GetEndOffset())
			blockIndexs[i] = append(blockIndexs[i], &blockLoc)
		}
	}
	blks, _, err := writer.Sync(context.Background())

	for tid, mata := range data.meta {
		for i, table := range mata.tables {
			if table == nil || table.ClosedInterval.Start == table.ClosedInterval.End {
				continue
			}

			idx := uint16(i)
			if i > BlockDelete {
				if tid == pkgcatalog.MO_DATABASE_ID ||
					tid == pkgcatalog.MO_TABLES_ID ||
					tid == pkgcatalog.MO_COLUMNS_ID {
					break
				}
			}

			if i == BlockInsert {
				idx = BLKMetaInsertIDX
			} else if i == BlockDelete {
				idx = BLKMetaDeleteIDX
			} else if i == CNBlockInsert {
				idx = BLKCNMetaInsertIDX
			} else if i == SegmentDelete {
				idx = SEGDeleteIDX
			}
			switch tid {
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
			for _, block := range blockIndexs[idx] {
				if table.End < block.GetStartOffset() {
					break
				}
				blockLoc1 := objectio.BuildLocation(name, blks[block.GetID()].GetExtent(), 0, block.GetID())
				logutil.Infof("write tid %d-%d block %v to %d-%d, table is %d-%d", tid,i,blockLoc1.String(), block.GetStartOffset(), block.GetEndOffset(), table.Start, table.End)
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
						0, block.GetEndOffset()-table.End)
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
	blks2, _, err := writer2.Sync(context.Background())
	location = objectio.BuildLocation(name2, blks2[0].GetExtent(), 0, blks2[0].GetID())
	return
}

func LoadBlkColumnsByMeta(
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
	ioResults, err := reader.LoadSubColumns(cxt, idxs, nil, id, nil)
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
	ioResults, err := reader.LoadSubColumns(cxt, idxs, nil, id, m)
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
	reader *blockio.BlockReader,
	m *mpool.MPool,
) (*batch.Batch, error) {
	idxs := make([]uint16, len(colNames))
	for i := range colNames {
		idxs[i] = uint16(i)
	}
	ioResult, err := reader.LoadOneSubColumns(cxt, idxs, nil, dataType, id, m)
	if err != nil {
		return nil, err
	}
	ioResult.Attrs = make([]string, len(colNames))
	copy(ioResult.Attrs, colNames)
	return ioResult, nil
}

func (data *CheckpointData) PrefetchFrom(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
	key objectio.Location) (err error) {
	return prefetchCheckpointData(ctx, version, service, key)
}

func prefetchCheckpointData(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
	key objectio.Location,
) (err error) {
	// FIXME
	return nil
	pref, err := blockio.BuildSubPrefetchParams(service, key)
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

func prefetchMetaBatch(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
	key objectio.Location) (err error) {

	pref, err := blockio.BuildSubPrefetchParams(service, key)
	if err != nil {
		return
	}
	item := checkpointDataReferVersions[version][MetaIDX]
	idxes := make([]uint16, len(item.attrs))
	for i := range item.attrs {
		idxes[i] = uint16(i)
	}
	pref.AddBlock(idxes, []uint16{uint16(MetaIDX)})
	return blockio.PrefetchWithMerged(pref)
}

func prefetchBatches(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
	schemaIdxes []uint16,
	key objectio.Location,
) (err error) {
	pref, err := blockio.BuildSubPrefetchParams(service, key)
	if err != nil {
		return
	}
	for _, idx := range schemaIdxes {
		item := checkpointDataReferVersions[version][idx]
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
	reader *blockio.BlockReader,
	fs fileservice.FileService,
	m *mpool.MPool,
) (err error) {
	err = data.readMetaBatch(ctx, version, reader, m)
	if err != nil {
		return
	}
	err = data.readAll(ctx, version, fs)
	if err != nil {
		return
	}

	if version == CheckpointVersion1 {
		bat := data.bats[TBLInsertIDX]
		if bat == nil {
			return
		}
		length := bat.GetVectorByName(pkgcatalog.SystemRelAttr_Version).Length()
		vec := containers.MakeVector(types.T_uint32.ToType())
		for i := 0; i < length; i++ {
			vec.Append(pkgcatalog.CatalogVersion_V1, false)
		}
		//Fixme: add vector to batch
		//bat.AddVector(pkgcatalog.SystemRelAttr_CatalogVersion, vec)
	}

	return
}

func (data *CheckpointData) readMetaBatch(
	ctx context.Context,
	version uint32,
	reader *blockio.BlockReader,
	m *mpool.MPool,
) (err error) {
	var bats []*containers.Batch
	item := checkpointDataReferVersions[version][MetaIDX]
	bats, err = LoadBlkColumnsByMeta(ctx, item.types, item.attrs, uint16(0), reader)
	if err != nil {
		return
	}
	logutil.Infof("bats[0].Vecs[1].String() is %v", bats[0].Vecs[0].String())
	data.setMetaBatch(bats[0])
	return
}
func (data *CheckpointData) setMetaBatch(bat *containers.Batch) {
	data.bats[MetaIDX].Append(bat)
}
func (data *CheckpointData) getMetaBatch() (bat *containers.Batch) {
	return data.bats[MetaIDX]
}

func (data *CheckpointData) replayMetaBatch() {
	bat := data.getMetaBatch()
	data.locations = make(map[string]objectio.Location)
	tidVec := vector.MustFixedCol[uint64](bat.Vecs[Checkpoint_Meta_TID_IDX].GetDownstreamVector())
	insVec := vector.MustBytesCol(bat.Vecs[Checkpoint_Meta_Insert_Block_LOC_IDX].GetDownstreamVector())
	delVec := vector.MustBytesCol(bat.Vecs[Checkpoint_Meta_Delete_Block_LOC_IDX].GetDownstreamVector())
	delCNVec := vector.MustBytesCol(bat.Vecs[Checkpoint_Meta_CN_Delete_Block_LOC_IDX].GetDownstreamVector())
	segVec := vector.MustBytesCol(bat.Vecs[Checkpoint_Meta_Segment_LOC_IDX].GetDownstreamVector())

	for i := 0; i < data.bats[MetaIDX].Vecs[Checkpoint_Meta_TID_IDX].Length(); i++ {
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

func (data *CheckpointData) prefetchMetaBatch(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
	key objectio.Location,
) (err error) {
	err = prefetchMetaBatch(ctx, version, service, key)
	return
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
			if uint16(idx) == MetaIDX {
				continue
			}
			item := checkpointDataReferVersions[version][idx]
			bats, err = LoadBlkColumnsByMeta(ctx, item.types, item.attrs, uint16(idx), reader)
			if err != nil {
				return
			}
			for i := range bats {
				data.bats[idx].Append(bats[i])
			}
		}
	}
	return
}

func (data *CheckpointData) readBatch(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
) (err error) {
	return
}

func (data *CheckpointData) prefetchAll(
	ctx context.Context,
	version uint32,
	service fileservice.FileService,
) (err error) {
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
	switch version {
	case CheckpointVersion1:
		bat := data.bats[TBLInsertIDX]
		if bat == nil {
			return
		}
		vec := data.bats[TBLInsertIDX].GetVectorByName(pkgcatalog.SystemRelAttr_CatalogVersion)
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
	logutil.Infof("UpdateSegMeta delStart %d delEnd %d ", delStart, delEnd)
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
				logutil.Infof("metaNode.HasDropCommitted() is true, metaNode: %v", metaNode)
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
				logutil.Infof("!metaNode.HasDropCommitted() is true, metaNode: %v", metaNode)
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
			logutil.Infof("metaNode.HasDropCommitted()111 is true, metaNode: %v", metaNode)
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
				logutil.Infof("is_sorted.HasDropCommitted()111 is true, metaNode: %v", metaNode)
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
	logutil.Infof("UpdateBlkMeta entry.GetSegment().GetTable().ID is %dinsStart %d insEnd %d delStart %d delEnd %d ", entry.GetSegment().GetTable().ID, insStart, insEnd, delStart, delEnd)
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
