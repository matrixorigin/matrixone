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
	objects map[string]*ObjectEntry,
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
		buildMap bool,
		mp *mpool.MPool,
	) (err error) {
		creates := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[1])
		deletes := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[2])
		dbs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[3])
		tids := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[4])
		bf.Test(
			bat.Vecs[0],
			func(exists bool, i int) {
				if !exists {
					bm.Add(uint64(i))
					if !buildMap {
						return
					}
					buf := bat.Vecs[0].GetRawBytesAt(i)
					stats := (objectio.ObjectStats)(buf)
					name := stats.ObjectName().String()
					tid := tids[i]
					createTs := creates[i]
					dropTs := deletes[i]
					if !createTs.LT(ts) || !dropTs.LT(ts) {
						return
					}
					if dropTs.IsEmpty() && objects[name] == nil {
						object := &ObjectEntry{
							stats:    &stats,
							createTS: createTs,
							dropTS:   dropTs,
							db:       dbs[i],
							table:    tid,
						}
						objects[name] = object
						return
					}
					if objects[name] != nil {
						objects[name].dropTS = dropTs
						return
					}
				}
			},
		)
		return nil

	}, nil
}
