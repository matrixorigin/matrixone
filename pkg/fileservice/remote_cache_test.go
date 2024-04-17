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
	"github.com/matrixorigin/matrixone/pkg/pb/gossip"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/stretchr/testify/assert"
)

type mockKeyRouter[K comparable] struct {
	target string
}

func (r *mockKeyRouter[K]) Target(_ CacheKey) string    { return r.target }
func (r *mockKeyRouter[K]) AddItem(_ gossip.CommonItem) {}

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
		assert.Equal(t, Bytes{1, 2}, ioVec2.Entries[0].CachedData)
		assert.Equal(t, true, ioVec2.Entries[0].done)
		assert.NotNil(t, ioVec2.Entries[0].fromCache)

		sf1.fs.Close()
	})
}

func runTestWithTwoFileServices(t *testing.T, fn func(sf1 *cacheFs, sf2 *cacheFs)) {
	defer leaktest.AfterTest(t)()
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
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
			QueryClient: qt,
		}
		cacheCfg.setDefaults()
		cacheCfg.SetRemoteCacheCallback()
		fs, err := NewLocalFS(ctx, "local-"+selfAddr, dir, cacheCfg, nil)
		assert.Nil(t, err)
		assert.NotNil(t, fs)
		qs, err := queryservice.NewQueryService("", selfAddr, morpc.Config{})
		assert.NoError(t, err)
		qs.AddHandleFunc(query.CmdMethod_GetCacheData,
			func(ctx context.Context, req *query.Request, resp *query.Response) error {
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
}
