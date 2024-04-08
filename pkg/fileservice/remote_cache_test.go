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
	"github.com/matrixorigin/matrixone/pkg/cacheservice"
	"github.com/matrixorigin/matrixone/pkg/cacheservice/client"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/cache"
	"github.com/matrixorigin/matrixone/pkg/pb/gossip"
	"github.com/stretchr/testify/assert"
)

type mockKeyRouter struct {
	target string
}

func (r *mockKeyRouter) Target(_ CacheKey) string               { return r.target }
func (r *mockKeyRouter) AddItem(_ CacheKey, _ gossip.Operation) {}

type cacheFs struct {
	cs cacheservice.CacheService
	ct client.CacheClient
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
		ct, err := client.NewCacheClient(client.ClientConfig{RPC: morpc.Config{}})
		assert.NoError(t, err)
		assert.NotNil(t, ct)

		keyRouter := &mockKeyRouter{target: pairAddr}
		cacheCfg := CacheConfig{
			RemoteCacheEnabled: true,
			KeyRouterFactory: func() KeyRouter {
				return keyRouter
			},
			CacheClient: ct,
		}
		cacheCfg.setDefaults()
		cacheCfg.SetRemoteCacheCallback()
		fs, err := NewLocalFS(ctx, "local-"+selfAddr, dir, cacheCfg, nil)
		assert.Nil(t, err)
		assert.NotNil(t, fs)
		cs, err := cacheservice.NewCacheServer(selfAddr, morpc.Config{})
		assert.NoError(t, err)
		cs.AddHandleFunc(cache.CmdMethod_RemoteRead,
			func(ctx context.Context, req *cache.Request, resp *cache.CacheResponse) error {
				return HandleRemoteRead(ctx, fs, req, resp)
			},
			false,
		)
		err = cs.Start()
		assert.NoError(t, err)

		rc := NewRemoteCache(ct, func() KeyRouter { return keyRouter })
		assert.NotNil(t, rc)
		return &cacheFs{cs: cs, rc: rc, fs: fs, ct: ct}
	}

	cf1 := create(address1, address2)
	defer func() {
		assert.NoError(t, cf1.cs.Close())
		assert.NoError(t, cf1.ct.Close())
	}()
	cf2 := create(address2, address1)
	defer func() {
		assert.NoError(t, cf2.cs.Close())
		assert.NoError(t, cf2.ct.Close())
	}()

	fn(cf1, cf2)
}
