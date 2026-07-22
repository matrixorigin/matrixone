// Copyright 2021 - 2023 Matrix Origin
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

package fileservice

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/pb/gossip"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockKeyRouter[K comparable] struct {
	target string
}

func (r *mockKeyRouter[K]) Target(_ fscache.CacheKey) string { return r.target }
func (r *mockKeyRouter[K]) AddItem(_ gossip.CommonItem)      {}

type remoteCacheTestRouter func(fscache.CacheKey) string

func (r remoteCacheTestRouter) Target(key fscache.CacheKey) string { return r(key) }
func (r remoteCacheTestRouter) AddItem(gossip.CommonItem)          {}

type remoteCacheTestQueryClient struct {
	responses    map[string]*query.Response
	releaseCount int
	closeCount   int
}

var _ client.QueryClient = new(remoteCacheTestQueryClient)

func (r *remoteCacheTestQueryClient) ServiceID() string { return "remote-cache-test" }

func (r *remoteCacheTestQueryClient) SendMessage(
	_ context.Context,
	target string,
	_ *query.Request,
) (*query.Response, error) {
	return r.responses[target], nil
}

func (r *remoteCacheTestQueryClient) NewRequest(method query.CmdMethod) *query.Request {
	return &query.Request{CmdMethod: method}
}

func (r *remoteCacheTestQueryClient) Release(*query.Response) {
	r.releaseCount++
}

func (r *remoteCacheTestQueryClient) Close() error {
	r.closeCount++
	return nil
}

type remoteReadTestFileService struct {
	FileService
	data fscache.Data
	err  error
}

func (r *remoteReadTestFileService) ReadCache(_ context.Context, vector *IOVector) error {
	vector.Entries[0].CachedData = r.data
	vector.Entries[0].fromCache = new(RemoteCache)
	return r.err
}

type releaseCountingCacheData struct {
	data         []byte
	releaseCount int
}

func (r *releaseCountingCacheData) Bytes() []byte { return r.data }
func (r *releaseCountingCacheData) Size() int64   { return int64(len(r.data)) }
func (r *releaseCountingCacheData) Slice(length int) fscache.Data {
	r.data = r.data[:length]
	return r
}
func (r *releaseCountingCacheData) Retain() {}
func (r *releaseCountingCacheData) Release() {
	r.releaseCount++
}

func TestHandleRemoteReadRejectsInvalidRequests(t *testing.T) {
	fs, err := NewMemoryFS("test", DisabledCacheConfig, nil)
	require.NoError(t, err)

	valid := func(path string) *query.RequestCacheKey {
		return &query.RequestCacheKey{
			CacheKey: &query.CacheKey{Path: path, Sz: 1},
		}
	}

	tests := []struct {
		name string
		keys []*query.RequestCacheKey
	}{
		// Empty lists and non-nil entries with an omitted nested CacheKey are
		// normal decoded protobuf shapes.
		{name: "decoded/empty key list"},
		{name: "decoded/missing first cache key", keys: []*query.RequestCacheKey{{}}},
		{name: "decoded/missing later cache key", keys: []*query.RequestCacheKey{valid("foo"), {}}},
		{name: "decoded/mixed paths", keys: []*query.RequestCacheKey{valid("foo"), valid("bar")}},
		// Generated protobuf decoding always allocates repeated message entries,
		// but direct package callers should still receive an error instead of a panic.
		{name: "in-process/nil repeated key", keys: []*query.RequestCacheKey{nil}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := &query.Request{
				GetCacheDataRequest: &query.GetCacheDataRequest{RequestCacheKey: test.keys},
			}
			wr := &query.WrappedResponse{Response: &query.Response{}}

			var readErr error
			require.NotPanics(t, func() {
				readErr = HandleRemoteRead(context.Background(), fs, req, wr)
			})
			require.Error(t, readErr)
			require.Nil(t, wr.GetCacheDataResponse)
			require.Nil(t, wr.ReleaseFunc)
		})
	}
}

func TestHandleRemoteReadTransfersCacheDataRelease(t *testing.T) {
	data := &releaseCountingCacheData{data: []byte{1, 2}}
	fs := &remoteReadTestFileService{data: data}
	req := &query.Request{
		GetCacheDataRequest: &query.GetCacheDataRequest{
			RequestCacheKey: []*query.RequestCacheKey{{
				Index:    3,
				CacheKey: &query.CacheKey{Path: "foo", Sz: 2},
			}},
		},
	}
	wr := &query.WrappedResponse{Response: &query.Response{}}

	require.NoError(t, HandleRemoteRead(context.Background(), fs, req, wr))
	require.Equal(t, []*query.ResponseCacheData{{Index: 3, Hit: true, Data: []byte{1, 2}}},
		wr.GetCacheDataResponse.ResponseCacheData)
	require.NotNil(t, wr.ReleaseFunc)
	require.Zero(t, data.releaseCount)

	wr.ReleaseFunc()
	require.Equal(t, 1, data.releaseCount)
}

func TestHandleRemoteReadReleasesPartialCacheDataOnError(t *testing.T) {
	data := &releaseCountingCacheData{data: []byte{1, 2}}
	readErr := fmt.Errorf("read cache failed")
	fs := &remoteReadTestFileService{data: data, err: readErr}
	req := &query.Request{
		GetCacheDataRequest: &query.GetCacheDataRequest{
			RequestCacheKey: []*query.RequestCacheKey{{
				CacheKey: &query.CacheKey{Path: "foo", Sz: 2},
			}},
		},
	}
	wr := &query.WrappedResponse{Response: &query.Response{}}

	require.ErrorIs(t, HandleRemoteRead(context.Background(), fs, req, wr), readErr)
	require.Equal(t, 1, data.releaseCount)
	require.Nil(t, wr.GetCacheDataResponse)
	require.Nil(t, wr.ReleaseFunc)
}

func TestRemoteCacheReadIgnoresInvalidResponses(t *testing.T) {
	tests := []struct {
		name         string
		responseData []*query.ResponseCacheData
		expectDone   bool
		expectData   []byte
	}{
		// A nil repeated response entry cannot be produced by generated protobuf
		// decoding, but can be returned by an in-process QueryClient fake.
		{name: "in-process/nil repeated response", responseData: []*query.ResponseCacheData{nil}},
		{name: "negative index", responseData: []*query.ResponseCacheData{{Index: -1, Hit: true, Data: []byte{1, 2}}}},
		{name: "out of bounds index", responseData: []*query.ResponseCacheData{{Index: 1, Hit: true, Data: []byte{1, 2}}}},
		{name: "wrong data length", responseData: []*query.ResponseCacheData{{Index: 0, Hit: true, Data: []byte{1}}}},
		{
			name: "invalid hit does not consume index",
			responseData: []*query.ResponseCacheData{
				{Index: 0, Hit: true, Data: []byte{1}},
				{Index: 0, Hit: true, Data: []byte{1, 2}},
			},
			expectDone: true,
			expectData: []byte{1, 2},
		},
		{
			name: "duplicate valid hit keeps first",
			responseData: []*query.ResponseCacheData{
				{Index: 0, Hit: true, Data: []byte{1, 2}},
				{Index: 0, Hit: true, Data: []byte{3, 4}},
			},
			expectDone: true,
			expectData: []byte{1, 2},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			qt := &remoteCacheTestQueryClient{
				responses: map[string]*query.Response{
					"target": {
						GetCacheDataResponse: &query.GetCacheDataResponse{
							ResponseCacheData: test.responseData,
						},
					},
				},
			}
			rc := NewRemoteCache(qt, func() client.KeyRouter[query.CacheKey] {
				return remoteCacheTestRouter(func(fscache.CacheKey) string { return "target" })
			})
			vector := &IOVector{
				FilePath: "foo",
				Entries:  []IOEntry{{Offset: 0, Size: 2}},
			}

			require.NotPanics(t, func() {
				require.NoError(t, rc.Read(context.Background(), vector))
			})
			require.Equal(t, test.expectDone, vector.Entries[0].done)
			if test.expectDone {
				require.Equal(t, test.expectData, vector.Entries[0].CachedData.Bytes())
				require.Same(t, rc, vector.Entries[0].fromCache)
			} else {
				require.Nil(t, vector.Entries[0].CachedData)
				require.Nil(t, vector.Entries[0].fromCache)
			}
			require.Equal(t, 1, qt.releaseCount)
		})
	}
}

func TestRemoteCacheReadAcceptsOnlyIndicesRequestedFromTarget(t *testing.T) {
	qt := &remoteCacheTestQueryClient{
		responses: map[string]*query.Response{
			"target-a": {
				GetCacheDataResponse: &query.GetCacheDataResponse{
					ResponseCacheData: []*query.ResponseCacheData{{Index: 1, Hit: true, Data: []byte{9}}},
				},
			},
			"target-b": {
				GetCacheDataResponse: &query.GetCacheDataResponse{
					ResponseCacheData: []*query.ResponseCacheData{{Index: 1, Hit: false}},
				},
			},
		},
	}
	rc := NewRemoteCache(qt, func() client.KeyRouter[query.CacheKey] {
		return remoteCacheTestRouter(func(key fscache.CacheKey) string {
			if key.Offset == 0 {
				return "target-a"
			}
			return "target-b"
		})
	})
	vector := &IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{Offset: 0, Size: 1},
			{Offset: 1, Size: 1},
		},
	}

	require.NoError(t, rc.Read(context.Background(), vector))
	for i := range vector.Entries {
		require.False(t, vector.Entries[i].done)
		require.Nil(t, vector.Entries[i].CachedData)
		require.Nil(t, vector.Entries[i].fromCache)
	}
	require.Equal(t, 2, qt.releaseCount)
}

func TestLocalFSFallsBackAfterInvalidRemoteCacheHit(t *testing.T) {
	ctx := context.Background()
	qt := &remoteCacheTestQueryClient{
		responses: map[string]*query.Response{
			"target": {
				GetCacheDataResponse: &query.GetCacheDataResponse{
					ResponseCacheData: []*query.ResponseCacheData{{Index: 0, Hit: true, Data: []byte{9}}},
				},
			},
		},
	}
	config := DisabledCacheConfig
	config.RemoteCacheEnabled = true
	config.QueryClient = qt
	config.KeyRouterFactory = func() client.KeyRouter[query.CacheKey] {
		return remoteCacheTestRouter(func(fscache.CacheKey) string { return "target" })
	}
	fs, err := NewLocalFS(ctx, "local", t.TempDir(), config, nil)
	require.NoError(t, err)
	defer fs.Close(ctx)

	require.NoError(t, fs.Write(ctx, IOVector{
		FilePath: "foo",
		Entries:  []IOEntry{{Offset: 0, Size: 3, Data: []byte{1, 2, 3}}},
	}))
	vector := &IOVector{
		FilePath: "foo",
		Entries:  []IOEntry{{Offset: 0, Size: 3}},
	}
	require.NoError(t, fs.Read(ctx, vector))
	require.Equal(t, []byte{1, 2, 3}, vector.Entries[0].Data)
	require.Nil(t, vector.Entries[0].fromCache)
	require.Equal(t, 1, qt.releaseCount)
}

type cacheFs struct {
	qs queryservice.QueryService
	qt client.QueryClient
	rc *RemoteCache
	fs FileService
}

func TestRemoteCache(t *testing.T) {
	runTestWithTwoFileServices(t, func(sf1 *cacheFs, sf2 *cacheFs) {
		ctx := context.Background()
		err := sf1.fs.Write(ctx, IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Offset: 0,
					Size:   2,
					Data:   []byte{1, 2},
				},
			},
		})
		assert.NoError(t, err)
		ioVec1 := &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Offset:      0,
					Size:        2,
					ToCacheData: CacheOriginalData,
				},
			},
		}
		err = sf1.fs.Read(ctx, ioVec1)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(ioVec1.Entries))
		assert.Equal(t, []byte{1, 2}, ioVec1.Entries[0].Data)
		ioVec1.Release()

		ioVec2 := &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Offset: 0,
					Size:   2,
				},
			},
		}
		err = sf2.rc.Read(ctx, ioVec2)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(ioVec2.Entries))
		assert.Equal(t, &Bytes{bytes: []byte{1, 2}}, ioVec2.Entries[0].CachedData)
		assert.Equal(t, true, ioVec2.Entries[0].done)
		assert.NotNil(t, ioVec2.Entries[0].fromCache)

		sf1.fs.Close(ctx)
	})
}

func runTestWithTwoFileServices(t *testing.T, fn func(sf1 *cacheFs, sf2 *cacheFs)) {
	defer leaktest.AfterTest(t)()

	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			address1 := fmt.Sprintf("unix:///tmp/sock1-%d.sock", time.Now().Nanosecond())
			if err := os.RemoveAll(address1[7:]); err != nil {
				panic(err)
			}
			address2 := fmt.Sprintf("unix:///tmp/sock2-%d.sock", time.Now().Nanosecond())
			if err := os.RemoveAll(address2[7:]); err != nil {
				panic(err)
			}

			create := func(selfAddr, pairAddr string) *cacheFs {
				dir := t.TempDir()
				ctx := context.Background()
				qt, err := client.NewQueryClient("", morpc.Config{})
				assert.NoError(t, err)
				assert.NotNil(t, qt)

				keyRouter := &mockKeyRouter[query.CacheKey]{target: pairAddr}
				cacheCfg := CacheConfig{
					RemoteCacheEnabled: true,
					KeyRouterFactory: func() client.KeyRouter[query.CacheKey] {
						return keyRouter
					},
					QueryClient:    qt,
					MemoryCapacity: ptrTo[toml.ByteSize](1 << 30),
				}
				cacheCfg.setDefaults()
				cacheCfg.SetRemoteCacheCallback()
				fs, err := NewLocalFS(ctx, "local-"+selfAddr, dir, cacheCfg, nil)
				assert.Nil(t, err)
				assert.NotNil(t, fs)
				qs, err := queryservice.NewQueryService("", selfAddr, morpc.Config{})
				assert.NoError(t, err)
				qs.AddHandleFunc(query.CmdMethod_GetCacheData,
					func(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
						wr := &query.WrappedResponse{
							Response: resp,
						}
						err = HandleRemoteRead(ctx, fs, req, wr)
						if err != nil {
							return err
						}
						qs.SetReleaseFunc(resp, wr.ReleaseFunc)
						return nil
					},
					false,
				)
				err = qs.Start()
				assert.NoError(t, err)

				rc := NewRemoteCache(qt, func() client.KeyRouter[query.CacheKey] { return keyRouter })
				assert.NotNil(t, rc)
				return &cacheFs{qs: qs, rc: rc, fs: fs, qt: qt}
			}

			cf1 := create(address1, address2)
			defer func() {
				assert.NoError(t, cf1.qs.Close())
				assert.NoError(t, cf1.qt.Close())
			}()
			cf2 := create(address2, address1)
			defer func() {
				assert.NoError(t, cf2.qs.Close())
				assert.NoError(t, cf2.qt.Close())
			}()

			fn(cf1, cf2)
		},
	)
}
