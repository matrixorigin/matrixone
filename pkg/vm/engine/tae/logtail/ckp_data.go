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

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
)

type CheckpointData_V2 struct {
	batch     *batch.Batch
	sinker    *ioutil.Sinker
	allocator *mpool.MPool
}

func (data *CheckpointData_V2) WriteTo(ctx context.Context, fs fileservice.FileService) (CNLocation, TNLocation objectio.Location, err error) {
	files, inMems := data.sinker.GetResult()
	if len(inMems) != 0 {
		panic("logic error")
	}
	ranges := ckputil.MakeTableRangeBatch()
	defer ranges.Clean(data.allocator)
	err = ckputil.CollectTableRanges(ctx, files, ranges, data.allocator, fs)
	if err != nil {
		return
	}
	segmentid := objectio.NewSegmentid()
	fileNum := uint16(0)
	name := objectio.BuildObjectName(segmentid, fileNum)
	writer, err := ioutil.NewBlockWriterNew(fs, name, 0, nil, false)
	if err != nil {
		return
	}
	_, err = writer.WriteBatch(ranges)
	if err != nil {
		return
	}
	blks, _, err := writer.Sync(ctx)
	if err != nil {
		return
	}
	CNLocation = objectio.BuildLocation(name, blks[0].GetExtent(), 0, blks[0].GetID())
	TNLocation = CNLocation
	return
}

func (data *CheckpointData_V2) ReadAll(
	ctx context.Context,
	version uint32,
	metaLocation objectio.Location,
	reader *ioutil.BlockReader,
	fs fileservice.FileService,
) (releaseCBs []func(), err error) {
	ranges, err := LoadBlkColumnsByMeta(version, ctx, ckputil.MetaSchema_TableRange_Types, ckputil.MetaSchema_TableRange_Attrs, 0, reader, data.allocator)
	if err != nil {
		return
	}
	locationVec := ranges[0].Vecs[ckputil.MetaAttr_Location_Idx]
	locations := make(map[string]objectio.Location)
	for i := 0; i < locationVec.Length(); i++ {
		location := objectio.Location(locationVec.GetDownstreamVector().GetBytesAt(i))
		locations[location.String()] = location
	}
	vecs := containers.NewVectors(len(ckputil.DataScan_ObjectEntryAttrs))
	releaseCBs = make([]func(), 0, len(locations))
	data.batch = batch.New(ckputil.DataScan_ObjectEntryAttrs)
	for _, loc := range locations {
		_, release, err := ioutil.LoadColumnsData(
			ctx,
			ckputil.DataScan_ObjectEntrySeqnums,
			ckputil.DataScan_ObjectEntryTypes,
			fs,
			loc,
			vecs,
			data.allocator,
			0,
		)
		if err != nil {
			for _, fn := range releaseCBs {
				fn()
			}
			return nil, err
		}

		releaseCBs = append(releaseCBs, release)
	}
	panic("todo")
}

func ReplayThreeTablesObjectlist(
	ctx context.Context,
	location objectio.Location,
	catalog *catalog.Catalog,
	fs fileservice.FileService,
	dataFactory *tables.DataFactory,
	mp *mpool.MPool) {
	vecs := containers.NewVectors(len(ckputil.DataScan_ObjectEntryAttrs))
	_, release, err := ioutil.LoadColumnsData(
		ctx,
		ckputil.DataScan_ObjectEntrySeqnums,
		ckputil.DataScan_ObjectEntryTypes,
		fs,
		location,
		vecs,
		mp,
		0,
	)
	if err != nil {
		return
	}
	defer release()
	tids := vector.MustFixedColNoTypeCheck[uint64](&vecs[ckputil.MetaAttr_Table_Idx])
	locationVec := vecs[ckputil.MetaAttr_Location_Idx]
	for i := 0; i < len(tids); i++ {
		if !pkgcatalog.IsSystemTable(tids[i]) {
			break
		}
		
	}
}

func PrefetchAll(location objectio.Location) { panic("todo") }

func ReplayCatalog() {}

/*
replay
read manifaster
for locations {prefetch}
for locations {replay catalog}
for locations {replay objects}
*/

type BaseCollector_V2 struct {
	*catalog.LoopProcessor
	data       *CheckpointData_V2
	start, end types.TS
	packer     *types.Packer
}

func (collector *BaseCollector_V2) visitObject(entry *catalog.ObjectEntry) error {
	mvccNodes := entry.GetMVCCNodeInRange(collector.start, collector.end)

	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		create := node.End.Equal(&entry.CreatedAt)
		visitObject_V2(collector.data.batch, entry, create, false, types.TS{}, collector.packer, collector.data.allocator)
		if collector.data.batch.Vecs[0].Length() >= DefaultCheckpointBlockRows {
			collector.data.sinker.Write(context.Background(), collector.data.batch)
			collector.data.batch.CleanOnlyData()
		}
	}
	return nil
}

func visitObject_V2(
	bat *batch.Batch, entry *catalog.ObjectEntry, create, push bool, commitTS types.TS, packer *types.Packer, mp *mpool.MPool) {
	vector.AppendFixed(bat.Vecs[ckputil.TableObjectsAttr_Accout_Idx], entry.GetTable().GetDB().GetTenantID(), false, mp)
	vector.AppendFixed(bat.Vecs[ckputil.TableObjectsAttr_DB_Idx], entry.GetTable().GetDB().ID, false, mp)
	vector.AppendFixed(bat.Vecs[ckputil.TableObjectsAttr_Table_Idx], entry.GetTable().ID, false, mp)
	vector.AppendBytes(bat.Vecs[ckputil.TableObjectsAttr_ID_Idx], entry.ObjectStats[:], false, mp)
	vector.AppendFixed(bat.Vecs[ckputil.TableObjectsAttr_ObjectType_Idx], entry.IsTombstone, false, mp)
	packer.Reset()
	objType := ckputil.ObjectType_Data
	if entry.IsTombstone {
		objType = ckputil.ObjectType_Tombstone
	}
	ckputil.EncodeCluser(packer, entry.GetTable().ID, objType, entry.ID())
	vector.AppendBytes(bat.Vecs[ckputil.TableObjectsAttr_Cluster_Idx], packer.Bytes(), false, mp)
	if create {
		if push {
			vector.AppendFixed(bat.Vecs[ckputil.TableObjectsAttr_CreateTS_Idx], commitTS, false, mp)
			vector.AppendFixed(bat.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx], types.TS{}, false, mp)
		} else {
			vector.AppendFixed(bat.Vecs[ckputil.TableObjectsAttr_CreateTS_Idx], entry.CreatedAt, false, mp)
			vector.AppendFixed(bat.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx], types.TS{}, false, mp)
		}
	} else {
		if push {
			vector.AppendFixed(bat.Vecs[ckputil.TableObjectsAttr_CreateTS_Idx], entry.DeletedAt, false, mp)
			vector.AppendFixed(bat.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx], commitTS, false, mp)
		} else {
			vector.AppendFixed(bat.Vecs[ckputil.TableObjectsAttr_CreateTS_Idx], entry.DeletedAt, false, mp)
			vector.AppendFixed(bat.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx], entry.CreatedAt, false, mp)
		}
	}
}
