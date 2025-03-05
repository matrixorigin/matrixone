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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

const DefaultCheckpointBlockRows = 10000
const DefaultCheckpointSize = 512 * 1024 * 1024

const (
	CheckpointVersion12 uint32 = 12
	CheckpointVersion13 uint32 = 13

	CheckpointCurrentVersion = CheckpointVersion13
)

const (
	MetaIDX uint16 = iota

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

var checkpointDataReferVersions map[uint32][MaxIDX]*checkpointDataItem

func init() {

	checkpointDataSchemas_V12 = [MaxIDX]*catalog.Schema{
		MetaSchema,
		TNMetaSchema,
		StorageUsageSchema,
		ObjectInfoSchema,
		StorageUsageSchema,
		ObjectInfoSchema, //15
	}

	checkpointDataReferVersions = make(map[uint32][MaxIDX]*checkpointDataItem)
	registerCheckpointDataReferVersion(CheckpointVersion12, checkpointDataSchemas_V12[:])
}

func IDXString(idx uint16) string {
	switch idx {
	case MetaIDX:
		return "MetaIDX"

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
	start, end types.TS,
	size int,
	fs fileservice.FileService,
) func(c *catalog.Catalog) (*CheckpointData_V2, error) {
	return func(c *catalog.Catalog) (data *CheckpointData_V2, err error) {
		collector := NewBaseCollector_V2(start, end, size, fs)
		if err = collector.Collect(c); err != nil {
			return
		}
		data = collector.OrphanData()
		return
	}
}

func BackupCheckpointDataFactory(
	start, end types.TS,
	fs fileservice.FileService,
) func(c *catalog.Catalog) (*CheckpointData_V2, error) {
	return func(c *catalog.Catalog) (data *CheckpointData_V2, err error) {
		collector := NewBackupCollector_V2(start, end, fs)
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
	end types.TS,
	versionInterval time.Duration,
	fs fileservice.FileService,
) func(c *catalog.Catalog) (*CheckpointData_V2, error) {
	return func(c *catalog.Catalog) (data *CheckpointData_V2, err error) {
		collector := NewGlobalCollector_V2(fs, end, versionInterval)
		defer collector.Close()
		err = c.RecurLoop(collector)
		if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
			err = nil
		}

		if err != nil {
			return
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
	bats [MaxIDX]*containers.Batch
}

func LoadBlkColumnsByMeta(
	version uint32,
	cxt context.Context,
	colTypes []types.Type,
	colNames []string,
	id uint16,
	reader *ioutil.BlockReader,
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

type blockIndexes struct {
	fileNum uint16
	indexes *BlockLocation
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
