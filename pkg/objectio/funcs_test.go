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

package objectio

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

type releaseTrackingData struct {
	releases *atomic.Int32
}

func (r *releaseTrackingData) Size() int64 {
	return 0
}

func (r *releaseTrackingData) Bytes() []byte {
	return nil
}

func (r *releaseTrackingData) Slice(int) fscache.Data {
	return r
}

func (r *releaseTrackingData) Retain() {
}

func (r *releaseTrackingData) Release() {
	r.releases.Add(1)
}

type partialReadErrorFS struct {
	fileservice.FileService
	data fscache.Data
	err  error
}

func (p *partialReadErrorFS) Name() string {
	return "partial-read-error"
}

func (p *partialReadErrorFS) Read(_ context.Context, vector *fileservice.IOVector) error {
	if len(vector.Entries) > 0 {
		vector.Entries[0].CachedData = p.data
	}
	return p.err
}

func (p *partialReadErrorFS) ReadCache(context.Context, *fileservice.IOVector) error {
	return nil
}

func TestReadOneBlockWithMetaReleasesPartialReadOnError(t *testing.T) {
	var releases atomic.Int32
	readErr := errors.New("read canceled after partial cache fill")
	fs := &partialReadErrorFS{
		data: &releaseTrackingData{releases: &releases},
		err:  readErr,
	}

	meta := BuildMetaData(1, 1)
	col := meta.GetBlockMeta(0).ColumnMeta(0)
	col.setDataType(uint8(types.T_int8))
	col.setLocation(NewExtent(1, 0, 1, 1))

	_, err := ReadOneBlockWithMeta(
		context.Background(),
		&meta,
		"test-object",
		0,
		[]uint16{0},
		[]types.Type{types.T_int8.ToType()},
		mpool.MustNewZero(),
		fs,
		constructorFactory,
		fileservice.Policy(0),
	)
	require.ErrorIs(t, err, readErr)
	require.Equal(t, int32(1), releases.Load())
}

func TestReadExtentReleasesPartialReadOnError(t *testing.T) {
	var releases atomic.Int32
	readErr := errors.New("read canceled after partial extent fill")
	fs := &partialReadErrorFS{
		data: &releaseTrackingData{releases: &releases},
		err:  readErr,
	}
	extent := NewExtent(1, 0, 1, 1)

	_, err := ReadExtent(
		context.Background(),
		"test-object",
		&extent,
		fileservice.Policy(0),
		fs,
		constructorFactory,
	)
	require.ErrorIs(t, err, readErr)
	require.Equal(t, int32(1), releases.Load())
}

func TestReadOneBlockAllColumnsReleasesPartialReadOnError(t *testing.T) {
	var releases atomic.Int32
	readErr := errors.New("read canceled after partial all-columns fill")
	fs := &partialReadErrorFS{
		data: &releaseTrackingData{releases: &releases},
		err:  readErr,
	}

	meta := BuildMetaData(1, 1)
	col := meta.GetBlockMeta(0).ColumnMeta(0)
	col.setDataType(uint8(types.T_int8))
	col.setLocation(NewExtent(1, 0, 1, 1))

	_, err := ReadOneBlockAllColumns(
		context.Background(),
		&meta,
		"test-object",
		0,
		[]uint16{0},
		fileservice.Policy(0),
		fs,
	)
	require.ErrorIs(t, err, readErr)
	require.Equal(t, int32(1), releases.Load())
}
