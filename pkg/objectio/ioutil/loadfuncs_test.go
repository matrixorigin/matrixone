// Copyright 2026 Matrix Origin
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

package ioutil

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

type releaseTrackingFS struct {
	fileservice.FileService
	tracked     atomic.Int32
	outstanding atomic.Int32
}

func (f *releaseTrackingFS) Read(
	ctx context.Context,
	ioVector *fileservice.IOVector,
) error {
	if err := f.FileService.Read(ctx, ioVector); err != nil {
		return err
	}
	for i := range ioVector.Entries {
		data := ioVector.Entries[i].CachedData
		if data == nil {
			continue
		}
		f.tracked.Add(1)
		f.outstanding.Add(1)
		ioVector.Entries[i].CachedData = &releaseTrackingData{
			Data:        data,
			outstanding: &f.outstanding,
		}
	}
	return nil
}

type releaseTrackingData struct {
	fscache.Data
	outstanding *atomic.Int32
}

func (d *releaseTrackingData) Slice(length int) fscache.Data {
	d.Data = d.Data.Slice(length)
	return d
}

func (d *releaseTrackingData) Retain() {
	d.Data.Retain()
	d.outstanding.Add(1)
}

func (d *releaseTrackingData) Release() {
	d.Data.Release()
	d.outstanding.Add(-1)
}

func TestLoadColumns2NeedCopyReleasesSourceCachedData(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_TS.ToType())
	require.NoError(t, vector.AppendFixed(
		bat.Vecs[0],
		types.BuildTS(42, 0),
		false,
		mp,
	))
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	writer := ConstructWriter(0, []uint16{0}, -1, false, false, fs)
	_, err := writer.WriteBatch(bat)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)

	trackingFS := &releaseTrackingFS{FileService: fs}
	pool := containers.NewVectorPool(
		t.Name(),
		1,
		containers.WithMPool(mp),
	)
	defer pool.Destory()
	stats := writer.GetObjectStats()

	vectors, release, err := LoadColumns2(
		ctx,
		[]uint16{0},
		[]types.Type{types.T_TS.ToType()},
		trackingFS,
		stats.ObjectLocation(),
		fileservice.Policy(0),
		true,
		pool,
	)
	require.NoError(t, err)
	require.Nil(t, release)
	require.Positive(t, trackingFS.tracked.Load())
	require.Zero(t, trackingFS.outstanding.Load())
	require.Len(t, vectors, 1)
	require.Equal(t, types.BuildTS(42, 0), vectors[0].Get(0))
	vectors[0].Close()
}
