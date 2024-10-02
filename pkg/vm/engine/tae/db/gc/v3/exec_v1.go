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

package gc

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

const statsIdx = 2

func MakeBloomfilterCoarseFilter(
	ctx context.Context,
	rowCount int,
	probability float64,
	buffer containers.IBatchBuffer,
	location *objectio.Location,
	ts *types.TS,
	transObjects *map[string]*ObjectEntry,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (
	FilterFn,
	error,
) {
	reader, err := logtail.MakeGlobalCheckpointDataReader(ctx, "", fs, location, 0)
	if err != nil {
		return nil, err
	}
	bf, err := BuildBloomfilter(
		ctx,
		rowCount,
		probability,
		statsIdx,
		reader.LoadBatchData,
		buffer,
		mp,
	)
	if err != nil {
		reader.Close()
		return nil, err
	}
	reader.Close()
	return func(
		ctx context.Context,
		bm *bitmap.Bitmap,
		bat *batch.Batch,
		mp *mpool.MPool,
	) (err error) {
		createTSs := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[1])
		dropTSs := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[2])
		dbs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[3])
		tableIDs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[4])
		bf.Test(
			bat.Vecs[0],
			func(exists bool, i int) {
				if exists {
					return
				}

				bm.Add(uint64(i))
				createTS := createTSs[i]
				dropTS := dropTSs[i]
				if !createTS.LT(ts) || !dropTS.LT(ts) {
					return
				}

				buf := bat.Vecs[0].GetRawBytesAt(i)
				stats := (objectio.ObjectStats)(buf)
				name := stats.ObjectName().UnsafeString()

				if dropTS.IsEmpty() && (*transObjects)[name] == nil {
					object := &ObjectEntry{
						stats:    &stats,
						createTS: createTS,
						dropTS:   dropTS,
						db:       dbs[i],
						table:    tableIDs[i],
					}
					(*transObjects)[name] = object
					return
				}
				if (*transObjects)[name] != nil {
					(*transObjects)[name].dropTS = dropTS
					return
				}
			},
		)
		return nil

	}, nil
}

func MakeSnapshotAndPitrFineFilter(
	ts *types.TS,
	accountSnapshots map[uint32][]types.TS,
	pitrs *logtail.PitrInfo,
	snapshotMeta *logtail.SnapshotMeta,
	transObjects map[string]*ObjectEntry,
) (
	filter FilterFn,
	err error,
) {
	tableSnapshots, tablePitrs := snapshotMeta.AccountToTableSnapshots(
		accountSnapshots,
		pitrs,
	)
	return func(
		ctx context.Context,
		bm *bitmap.Bitmap,
		bat *batch.Batch,
		mp *mpool.MPool,
	) error {
		createTSs := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[1])
		deleteTSs := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[2])
		tableIDs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[4])
		for i := 0; i < bat.Vecs[0].Length(); i++ {
			buf := bat.Vecs[0].GetRawBytesAt(i)
			stats := (objectio.ObjectStats)(buf)
			name := stats.ObjectName().UnsafeString()
			tableID := tableIDs[i]
			createTS := createTSs[i]
			dropTS := deleteTSs[i]

			snapshots := tableSnapshots[tableID]
			pitr := tablePitrs[tableID]

			if entry := transObjects[name]; entry != nil {
				if !isSnapshotRefers(
					entry.stats, pitr, &entry.createTS, &entry.dropTS, snapshots,
				) {
					bm.Add(uint64(i))
				}
				continue
			}
			if !createTS.LT(ts) || !dropTS.LT(ts) {
				continue
			}
			if dropTS.IsEmpty() {
				panic(fmt.Sprintf("dropTS is empty, name: %s, createTS: %s", name, createTS.ToString()))
			}
			if !isSnapshotRefers(
				&stats, pitr, &createTS, &dropTS, snapshots,
			) {
				bm.Add(uint64(i))
			}
		}
		return nil
	}, nil
}

func MakeFinalCanGCSinker(
	filesToGC *[]string,
) (
	SinkerFn,
	error,
) {
	buffer := make(map[string]struct{}, 100)
	return func(
		ctx context.Context, bat *batch.Batch,
	) error {
		clear(buffer)
		for i := 0; i < bat.Vecs[0].Length(); i++ {
			buf := bat.Vecs[0].GetRawBytesAt(i)
			stats := (*objectio.ObjectStats)(unsafe.Pointer(&buf[0]))
			name := stats.ObjectName().String()
			buffer[name] = struct{}{}
		}
		for name := range buffer {
			*filesToGC = append(*filesToGC, name)
		}
		return nil
	}, nil
}
