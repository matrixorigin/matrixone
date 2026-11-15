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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

func TestLoadReadonlyPublishesJSONCacheVersion(t *testing.T) {
	ctx := context.Background()
	capacity := toml.ByteSize(1 << 20)
	fs, err := fileservice.NewLocalFS2(ctx, defines.SharedFileServiceName, t.TempDir(), fileservice.CacheConfig{MemoryCapacity: &capacity}, nil)
	require.NoError(t, err)
	fs.SetAsyncUpdate(false)
	defer fs.Close(ctx)
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	registry, err := mpool.NewAllocationAccountRegistry(1, 256)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.Zero(t, account.Seal().Used)
		_, err := registry.Finalize(account)
		require.NoError(t, err)
	})
	admit := func(capacity uint64) (func(), error) {
		reservation, err := account.ReserveCapacity(capacity, 1, 1)
		if err != nil {
			return nil, err
		}
		lease, err := reservation.Commit(capacity)
		if err != nil {
			reservation.Abort()
			return nil, err
		}
		return lease.Release, nil
	}
	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.T_json.ToType())
	defer input.Clean(mp)
	for _, text := range []string{`{"keep":1}`, `{"keep":2}`} {
		value, err := types.ParseStringToByteJson(text)
		require.NoError(t, err)
		require.NoError(t, vector.AppendByteJson(input.Vecs[0], value, false, mp))
	}
	input.SetRowCount(2)
	writer := ConstructWriter(0, []uint16{0}, -1, false, false, fs)
	_, err = writer.WriteBatch(input)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	location := stats.ObjectLocation()
	typ := types.T_json.ToType()
	warm := vector.NewOffHeapVecWithType(typ)
	defer warm.Free(mp)
	_, err = LoadColumnsDataIntoReadonly(ctx, []uint16{0}, []types.Type{typ}, fs, location, []*vector.Vector{warm}, mp, 0)
	require.NoError(t, err)
	before := mp.CurrNB()
	baseline := vector.NewOffHeapVecWithType(typ)
	defer baseline.Free(mp)
	hit, err := LoadColumnsDataIntoReadonly(ctx, []uint16{0}, []types.Type{typ}, fs, location, []*vector.Vector{baseline}, mp, 0)
	require.NoError(t, err)
	require.True(t, hit)
	backing := mp.CurrNB() - before
	baseline.Free(mp)
	publication := objectio.NewJSONReadSet([]int{0})
	publication.SetCachePinAdmission(admit)
	defer publication.Close()
	publication.BindColumns([]uint16{0})
	publishedCtx := objectio.WithJSONReadSet(ctx, publication)
	destination := vector.NewOffHeapVecWithType(typ)
	defer destination.Free(mp)
	before = mp.CurrNB()
	hit, err = LoadColumnsDataIntoReadonly(publishedCtx, []uint16{0}, []types.Type{typ}, fs, location, []*vector.Vector{destination}, mp, 0)
	require.NoError(t, err)
	require.True(t, hit)
	require.Greater(t, mp.CurrNB()-before, backing, "an unqualified existing cache entry needs a private snapshot")
	reader := publication.Readers()[0]
	require.NotNil(t, reader)
	path, err := types.ParseStringToPath("$.keep")
	require.NoError(t, err)
	result, err := reader.Query(1, &path)
	require.NoError(t, err)
	publication.Close()
	text, err := result.Text()
	result.Close()
	require.NoError(t, err)
	require.Equal(t, "2", text.Text)
	text.Close()
	destination.Free(mp)

	// On a cache miss with JSON_VALUE demand, the private constructor owns the
	// initial allocation. A subsequent hit retains that version with no second
	// document copy, even though the first reader and its batch have closed.
	fs.FlushCache(ctx)
	prime := objectio.NewJSONReadSet([]int{0})
	prime.SetCachePinAdmission(admit)
	prime.BindColumns([]uint16{0})
	primeDestination := vector.NewOffHeapVecWithType(typ)
	_, err = LoadColumnsDataIntoReadonly(objectio.WithJSONReadSet(ctx, prime), []uint16{0}, []types.Type{typ}, fs, location, []*vector.Vector{primeDestination}, mp, 0)
	require.NoError(t, err)
	require.NotNil(t, prime.Readers()[0])
	require.Equal(t, "*objectio.cacheJSONReader", fmt.Sprintf("%T", prime.Readers()[0]))
	prime.Close()
	primeDestination.Free(mp)
	warmPrivate := objectio.NewJSONReadSet([]int{0})
	warmPrivate.SetCachePinAdmission(admit)
	defer warmPrivate.Close()
	warmPrivate.BindColumns([]uint16{0})
	privateDestination := vector.NewOffHeapVecWithType(typ)
	defer privateDestination.Free(mp)
	before = mp.CurrNB()
	hit, err = LoadColumnsDataIntoReadonly(objectio.WithJSONReadSet(ctx, warmPrivate), []uint16{0}, []types.Type{typ}, fs, location, []*vector.Vector{privateDestination}, mp, 0)
	require.NoError(t, err)
	require.True(t, hit)
	require.NotNil(t, warmPrivate.Readers()[0])
	require.Equal(t, "*objectio.cacheJSONReader", fmt.Sprintf("%T", warmPrivate.Readers()[0]))
	require.Equal(t, backing, mp.CurrNB()-before, "a private cache version must not copy another JSON column")
}
