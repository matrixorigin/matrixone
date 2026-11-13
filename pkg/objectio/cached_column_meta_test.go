// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objectio

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func cachedColumnFixture(t *testing.T) (Location, []byte) {
	t.Helper()
	location := NewRandomLocation(0, 20)
	data := BuildMetaData(2, 1)
	data.BlockHeader().SetMaxSeqnum(0)
	data.BlockHeader().SetRows(20)
	copy(data.BlockHeader().ShortName()[:], location.ShortName()[:])
	zm := index.NewZM(types.T_datetime, 6)
	value := types.DatetimeFromClock(2026, 1, 1, 0, 0, 0, 0)
	index.UpdateZM(zm, types.EncodeDatetime(&value))
	data.MustGetColumn(0).SetZoneMap(zm)
	for i := uint32(0); i < 2; i++ {
		block := data.GetBlockMeta(i)
		block.BlockHeader().SetRows(10)
		block.ColumnMeta(0).SetZoneMap(zm)
	}
	h := IOEntryHeader{Type: IOET_ObjMeta, Version: IOET_ObjectMeta_V3}
	prefix := buildObjectMetaV3()
	prefix.SetDataMetaCount(2)
	prefix.SetDataMetaOffset(metaHeaderLen)
	raw := append(bytes.Clone(EncodeIOEntryHeader(&h)), prefix...)
	return location, append(raw, data...)
}

func TestCachedColumnMetaBoundsAndCopies(t *testing.T) {
	ctx := t.Context()
	location, raw := cachedColumnFixture(t)
	key := encodeCacheKey(*location.ShortName(), cacheKeyTypeMeta)
	t.Cleanup(func() { metaCache.Delete(key) })
	_, ok := TryCachedColumnMeta(ctx, location, 0, 2)
	require.False(t, ok, "a miss never enters a loader")
	metaCache.Set(key, raw, int64(len(raw)))
	view, ok := TryCachedColumnMeta(ctx, location, 0, 2)
	require.True(t, ok)
	require.EqualValues(t, 2, view.BlockCount())
	object, ok := view.Object()
	require.True(t, ok)
	require.EqualValues(t, 20, object.Rows)
	first, ok := view.Block(0)
	require.True(t, ok)
	require.EqualValues(t, 10, first.Rows)
	first.ZoneMap[0] ^= 255
	again, ok := view.Block(0)
	require.True(t, ok)
	require.Equal(t, object.ZoneMap, again.ZoneMap, "returned ZMs do not alias cached bytes")
	_, ok = view.Block(2)
	require.False(t, ok)
	for _, limit := range []int{-1, 0, 1} {
		_, ok := TryCachedColumnMeta(ctx, location, 0, limit)
		require.False(t, ok)
	}
	_, ok = TryCachedColumnMeta(ctx, location, 1, 2)
	require.False(t, ok)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, ok = TryCachedColumnMeta(cancelled, location, 0, 2)
	require.False(t, ok)

	t.Run("contended shard", func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		done := make(chan error, 1)
		go func() {
			value, releaseAdmission, ok, err := metaCache.Get(key, func(int64) (func(), error) {
				close(entered)
				<-release
				return nil, nil
			})
			if ok {
				value.Release()
			}
			if releaseAdmission != nil {
				releaseAdmission()
			}
			done <- err
		}()
		t.Cleanup(func() { close(release); require.NoError(t, <-done) })
		<-entered
		_, ok := TryCachedColumnMeta(ctx, location, 0, 2)
		require.False(t, ok, "optional metadata read cannot wait behind cache admission")
	})

	// Eviction does not invalidate an already borrowed immutable view.
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()
	go func() {
		defer wg.Done()
		for range 100 {
			metaCache.Delete(key)
			metaCache.Set(key, raw, int64(len(raw)))
		}
	}()
	for range 100 {
		block, valid := view.Block(1)
		require.True(t, valid)
		require.EqualValues(t, 10, block.Rows)
	}
	wg.Wait()
}

func TestCachedColumnMetaDeclinesIncompleteMetadata(t *testing.T) {
	for _, scenario := range []string{"v2", "v1", "future", "header", "data offset", "index", "block offset", "block length", "sequence", "row count", "zone map", "identity"} {
		t.Run(scenario, func(t *testing.T) {
			location, raw := cachedColumnFixture(t)
			data := objectDataMetaV1(raw[IOEntryHeaderSize+metaHeaderLen:])
			switch scenario {
			case "v2":
				DecodeIOEntryHeader(raw).Version = IOET_ObjectMeta_V2
			case "v1":
				DecodeIOEntryHeader(raw).Version = IOET_ObjectMeta_V1
			case "future":
				DecodeIOEntryHeader(raw).Version = 65535
			case "header":
				raw = raw[:IOEntryHeaderSize-1]
			case "data offset":
				objectMetaV3(raw[IOEntryHeaderSize:]).SetDataMetaOffset(^uint32(0))
			case "index":
				data.BlockIndex().SetBlockCount(^uint32(0))
			case "block offset":
				data.BlockIndex().SetBlockMetaPos(1, ^uint32(0), 16)
			case "block length":
				offset, _ := data.BlockIndex().BlockMetaPos(1)
				data.BlockIndex().SetBlockMetaPos(1, offset, ^uint32(0))
			case "sequence":
				data.GetBlockMeta(1).BlockHeader().SetSequence(0)
			case "row count":
				data.GetBlockMeta(1).BlockHeader().SetRows(BlockMaxRows + 1)
			case "zone map":
				data.GetBlockMeta(1).ColumnMeta(0).SetZoneMap(EmptyZm[:])
			case "identity":
				data.BlockHeader().ShortName()[0] ^= 255
			}
			key := encodeCacheKey(*location.ShortName(), cacheKeyTypeMeta)
			metaCache.Set(key, raw, int64(len(raw)))
			t.Cleanup(func() { metaCache.Delete(key) })
			view, ok := TryCachedColumnMeta(t.Context(), location, 0, 2)
			if ok {
				_, ok = view.Object()
			}
			if ok {
				_, ok = view.Block(1)
			}
			require.False(t, ok)
		})
	}
}
