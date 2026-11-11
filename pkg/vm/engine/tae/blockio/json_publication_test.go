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

package blockio

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

func TestJSONStoragePublicationMapsDeletedRows(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	typ := types.T_json.ToType()
	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(typ)
	defer input.Clean(mp)
	for _, text := range []string{`{"keep":1}`, `{"keep":2}`} {
		value, err := types.ParseStringToByteJson(text)
		require.NoError(t, err)
		require.NoError(t, vector.AppendByteJson(input.Vecs[0], value, false, mp))
	}
	input.SetRowCount(2)
	writer := ioutil.ConstructWriter(0, []uint16{0}, -1, false, false, fs)
	_, err := writer.WriteBatch(input)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	info := stats.ConstructBlockInfo(0)
	for _, deleted := range []bool{false, true} {
		func() {
			output := batch.NewWithSize(1)
			output.Vecs[0] = vector.NewOffHeapVecWithType(typ)
			defer output.Clean(mp)
			publication := objectio.NewJSONReadSet([]int{0})
			defer publication.Close()
			publication.BindColumns([]uint16{0})
			readCtx := objectio.WithJSONReadSet(ctx, publication)
			source := &blockReadTestDataSource{}
			if deleted {
				source.deleted = []uint64{1}
			}
			cached := containers.NewVectors(2)
			defer cached.Free(mp)
			require.NoError(t, BlockDataReadInner(readCtx, &info, source, []uint16{0}, []types.Type{typ}, -1, types.TS{}, nil, nil, 0, output, cached, mp, fs))
			if deleted {
				require.NotNil(t, publication.Readers()[0])
				require.Equal(t, 1, publication.Readers()[0].Length())
				require.Equal(t, 1, output.Vecs[0].Length())
				path, err := types.ParseStringToPath("$.keep")
				require.NoError(t, err)
				result, err := publication.Readers()[0].Query(0, &path)
				require.NoError(t, err)
				text, err := result.Text()
				result.Close()
				require.NoError(t, err)
				require.Equal(t, "1", text.Text)
				text.Close()
			} else {
				require.NotNil(t, publication.Readers()[0])
				path, err := types.ParseStringToPath("$.keep")
				require.NoError(t, err)
				result, err := publication.Readers()[0].Query(1, &path)
				require.NoError(t, err)
				text, err := result.Text()
				result.Close()
				require.NoError(t, err)
				require.Equal(t, "2", text.Text)
				text.Close()
			}
		}()
	}
}

func TestJSONStoragePublicationComposesLateFilterRows(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	typ := types.T_json.ToType()
	input := batch.NewWithSize(2)
	input.Vecs[0] = vector.NewVec(typ)
	input.Vecs[1] = vector.NewVec(types.T_bool.ToType())
	defer input.Clean(mp)
	for _, text := range []string{`{"keep":1}`, `{"keep":2}`, `{"keep":3}`} {
		value, err := types.ParseStringToByteJson(text)
		require.NoError(t, err)
		require.NoError(t, vector.AppendByteJson(input.Vecs[0], value, false, mp))
		require.NoError(t, vector.AppendFixed(input.Vecs[1], true, false, mp))
	}
	input.SetRowCount(3)
	writer := ioutil.ConstructWriter(0, []uint16{0, 1}, -1, false, false, fs)
	_, err := writer.WriteBatch(input)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	info := stats.ConstructBlockInfo(0)
	for _, earlyColumn := range []int{0, 1} {
		func() {
			output := batch.NewWithSize(2)
			output.Vecs[0] = vector.NewOffHeapVecWithType(typ)
			output.Vecs[1] = vector.NewOffHeapVecWithType(types.T_bool.ToType())
			defer output.Clean(mp)
			publication := objectio.NewJSONReadSet([]int{0})
			defer publication.Close()
			publication.BindColumns([]uint16{0, 1})
			readCtx := objectio.WithJSONReadSet(ctx, publication)
			source := &blockReadTestDataSource{deleted: []uint64{0}}
			cached := containers.NewVectors(2)
			defer cached.Free(mp)
			filter := &engine.ReaderFilterRequest{Apply: func(bat *batch.Batch, loaded []int) (engine.ReaderFilterResult, error) {
				require.Equal(t, 2, bat.RowCount())
				for _, column := range loaded {
					if err := bat.Vecs[column].Shrink([]int64{1}, false, mp); err != nil {
						return engine.ReaderFilterResult{}, err
					}
				}
				bat.SetRowCount(1)
				return engine.ReaderFilterResult{Sels: []int64{1}}, nil
			}}
			require.NoError(t, blockDataReadWithFilter(readCtx, &info, source, []uint16{0, 1}, []types.Type{typ, types.T_bool.ToType()}, -1, types.TS{}, nil, 0, output, cached, mp, fs, []int{earlyColumn}, filter, nil, nil))
			version := publication.Readers()[0]
			require.NotNil(t, version)
			require.Equal(t, 1, version.Length())
			path, err := types.ParseStringToPath("$.keep")
			require.NoError(t, err)
			result, err := version.Query(0, &path)
			require.NoError(t, err)
			text, err := result.Text()
			result.Close()
			require.NoError(t, err)
			require.Equal(t, "3", text.Text)
			text.Close()
		}()
	}
}
