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

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

func MakeBloomfilterCoarseFilter(
	ctx context.Context,
	rowCount int,
	probability float64,
	buffer containers.IBatchBuffer,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (
	FilterFn,
	error,
) {
	var reader engine.BaseReader
	// TODO: make gloabl check point reader or something
	// reader = MakeReaderXXX(...)
	bf, err := BuildBloomfilter(
		ctx,
		rowCount,
		probability,
		0,
		func(ctx context.Context, bat *batch.Batch, mp *mpool.MPool) (bool, error) {
			return reader.Read(ctx, bat.Attrs, nil, mp, bat)
		},
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
		bf.Test(
			bat.Vecs[0],
			func(exists bool, i int) {
				if !exists {
					bm.Add(uint64(i))
				}
			},
		)
		return nil

	}, nil
}
