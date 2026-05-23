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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

type corruptCachedData struct {
	bytes []byte
}

func (d *corruptCachedData) Bytes() []byte          { return d.bytes }
func (d *corruptCachedData) Slice(int) fscache.Data { return d }
func (d *corruptCachedData) Retain()                {}
func (d *corruptCachedData) Release()               {}

type corruptColumnReadFS struct {
	fileservice.FileService
	readCalls int
}

func (fs *corruptColumnReadFS) Read(ctx context.Context, vector *fileservice.IOVector) error {
	if err := fs.FileService.Read(ctx, vector); err != nil {
		return err
	}
	fs.readCalls++
	if fs.readCalls == 2 {
		for i := range vector.Entries {
			vector.Entries[i].CachedData = &corruptCachedData{
				bytes: make([]byte, objectio.IOEntryHeaderSize),
			}
		}
	}
	return nil
}

type policyCaptureFS struct {
	fileservice.FileService
	policies []fileservice.Policy
}

func (fs *policyCaptureFS) Read(ctx context.Context, vector *fileservice.IOVector) error {
	fs.policies = append(fs.policies, vector.Policy)
	return fs.FileService.Read(ctx, vector)
}

func TestLoadColumnsDataReturnsErrorOnCorruptedColumnData(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()

	writer := ConstructWriter(0, []uint16{0}, -1, false, false, fs)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(11), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(22), false, mp))
	bat.SetRowCount(2)

	_, err := writer.WriteBatch(bat)
	require.NoError(t, err)
	blocks, _, err := writer.Sync(ctx)
	require.NoError(t, err)
	require.Len(t, blocks, 1)

	location := objectio.BuildLocation(
		writer.GetName(),
		blocks[0].GetExtent(),
		uint32(bat.RowCount()),
		blocks[0].GetID(),
	)

	cacheVectors := containers.NewVectors(1)
	cacheVectors[0] = *vector.NewVec(types.T_int32.ToType())

	dataMeta, release, err := LoadColumnsData(
		ctx,
		[]uint16{0},
		[]types.Type{types.T_int32.ToType()},
		&corruptColumnReadFS{FileService: fs},
		location,
		cacheVectors,
		mp,
		fileservice.Policy(0),
	)
	require.Error(t, err)
	require.Nil(t, release)
	require.NotNil(t, dataMeta)
	require.Contains(t, err.Error(), "invalid object meta")
}

func TestLoadColumnsDataPropagatesPolicy(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()

	writer := ConstructWriter(0, []uint16{0}, -1, false, false, fs)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(11), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(22), false, mp))
	bat.SetRowCount(2)

	_, err := writer.WriteBatch(bat)
	require.NoError(t, err)
	blocks, _, err := writer.Sync(ctx)
	require.NoError(t, err)
	require.Len(t, blocks, 1)

	location := objectio.BuildLocation(
		writer.GetName(),
		blocks[0].GetExtent(),
		uint32(bat.RowCount()),
		blocks[0].GetID(),
	)

	cacheVectors := containers.NewVectors(1)
	cacheVectors[0] = *vector.NewVec(types.T_int32.ToType())
	recordingFS := &policyCaptureFS{FileService: fs}
	_, release, err := LoadColumnsData(
		ctx,
		[]uint16{0},
		[]types.Type{types.T_int32.ToType()},
		recordingFS,
		location,
		cacheVectors,
		mp,
		fileservice.SkipFullFilePreloads,
	)
	require.NoError(t, err)
	defer release()
	require.GreaterOrEqual(t, len(recordingFS.policies), 2)
	require.Equal(t, fileservice.Policy(fileservice.SkipFullFilePreloads), recordingFS.policies[1])
}
