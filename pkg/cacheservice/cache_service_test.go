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

package cacheservice

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/cacheservice/client"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/cache"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/assert"
)

func testCreateCacheServer(t *testing.T) CacheService {
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	cluster := clusterservice.NewMOCluster(
		nil,
		0,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices([]metadata.CNService{{}}, nil))
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.ClusterService, cluster)
	address := fmt.Sprintf("unix:///tmp/%d.sock", time.Now().Nanosecond())
	err := os.RemoveAll(address[7:])
	assert.NoError(t, err)
	cs, err := NewCacheServer(address, morpc.Config{})
	assert.NoError(t, err)
	return cs
}

func TestNewCacheServer(t *testing.T) {
	cs := testCreateCacheServer(t)
	assert.NotNil(t, cs)
}

func TestCacheServerMain(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ct, err := client.NewCacheClient(client.ClientConfig{RPC: morpc.Config{}})
	assert.NoError(t, err)
	assert.NotNil(t, ct)
	defer func() { assert.NoError(t, ct.Close()) }()

	t.Run("main", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()
		memCap := toml.ByteSize(102400)
		fs, err := fileservice.NewLocalFS(ctx, "local", dir,
			fileservice.CacheConfig{
				MemoryCapacity:     &memCap,
				RemoteCacheEnabled: true,
				CacheClient:        ct,
			}, nil)
		assert.Nil(t, err)
		assert.NotNil(t, fs)

		runTestWithCacheServer(t, fs, func(svc CacheService, addr string) {
			readEntry0 := fileservice.IOEntry{
				Offset:      10,
				Size:        2,
				ToCacheData: fileservice.CacheOriginalData,
			}
			readEntry1 := fileservice.IOEntry{
				Offset:      20,
				Size:        3,
				ToCacheData: fileservice.CacheOriginalData,
			}
			writeEntry0 := readEntry0
			writeEntry0.Data = []byte{10, 20}
			writeEntry0.ToCacheData = nil
			writeEntry1 := readEntry1
			writeEntry1.Data = []byte{30, 40, 50}
			writeEntry1.ToCacheData = nil

			err = fs.Write(ctx, fileservice.IOVector{
				FilePath: "foo",
				Entries:  []fileservice.IOEntry{writeEntry0, writeEntry1},
			})
			assert.NoError(t, err)
			err = fs.Read(ctx, &fileservice.IOVector{
				FilePath: "foo",
				Entries:  []fileservice.IOEntry{readEntry0, readEntry1},
			})
			assert.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			req := ct.NewRequest(pb.CmdMethod_RemoteRead)
			req.RemoteReadRequest = &pb.RemoteReadRequest{
				RequestCacheKey: []*pb.RequestCacheKey{
					{
						Index: 0,
						CacheKey: &pb.CacheKey{
							Path:   "foo",
							Offset: 10,
							Sz:     2,
						},
					},
					{
						Index: 1,
						CacheKey: &pb.CacheKey{
							Path:   "foo",
							Offset: 20,
							Sz:     3,
						},
					},
				},
			}
			resp1, err := ct.SendMessage(ctx, addr, req)
			assert.NoError(t, err)
			defer ct.Release(resp1)
			assert.NotNil(t, resp1.RemoteReadResponse)
			assert.Equal(t, 2, len(resp1.RemoteReadResponse.ResponseCacheData))
			assert.Equal(t, pb.ResponseCacheData{Index: 0, Hit: true, Data: []byte{10, 20}},
				*resp1.RemoteReadResponse.GetResponseCacheData()[0])
			assert.Equal(t, pb.ResponseCacheData{Index: 1, Hit: true, Data: []byte{30, 40, 50}},
				*resp1.RemoteReadResponse.GetResponseCacheData()[1])

			req.RemoteReadRequest = &pb.RemoteReadRequest{
				RequestCacheKey: []*pb.RequestCacheKey{
					{
						Index: 0,
						CacheKey: &pb.CacheKey{
							Path:   "foo1",
							Offset: 40,
							Sz:     2,
						},
					},
				},
			}
			resp2, err := ct.SendMessage(ctx, addr, req)
			assert.NoError(t, err)
			defer ct.Release(resp2)
			assert.NotNil(t, resp2.RemoteReadResponse)
			assert.Equal(t, 1, len(resp2.RemoteReadResponse.ResponseCacheData))
			assert.Equal(t, pb.ResponseCacheData{Index: 0, Hit: false},
				*resp2.RemoteReadResponse.GetResponseCacheData()[0])
		})
	})

	t.Run("bad request", func(t *testing.T) {
		runTestWithCacheServer(t, nil, func(svc CacheService, addr string) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			req := ct.NewRequest(pb.CmdMethod_RemoteRead)
			_, err := ct.SendMessage(ctx, addr, req)
			assert.Error(t, err)
			assert.Equal(t, "internal error: bad request", err.Error())
		})
	})

	t.Run("unsupported cmd", func(t *testing.T) {
		runTestWithCacheServer(t, nil, func(svc CacheService, addr string) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			req := ct.NewRequest(pb.CmdMethod(10))
			_, err := ct.SendMessage(ctx, addr, req)
			assert.Error(t, err)
			assert.Equal(t, "not supported: 10 not support in current service", err.Error())
		})
	})
}

func runTestWithCacheServer(t *testing.T, fs fileservice.FileService, fn func(svc CacheService, addr string)) {
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	address := fmt.Sprintf("unix:///tmp/sock-%d.sock", time.Now().Nanosecond())
	if err := os.RemoveAll(address[7:]); err != nil {
		panic(err)
	}

	cs, err := NewCacheServer(address, morpc.Config{})
	assert.NoError(t, err)
	assert.NotNil(t, cs)
	defer func() { assert.NoError(t, cs.Close()) }()
	cs.AddHandleFunc(pb.CmdMethod_RemoteRead,
		func(ctx context.Context, req *pb.Request, resp *pb.CacheResponse) error {
			return fileservice.HandleRemoteRead(ctx, fs, req, resp)
		},
		false,
	)
	err = cs.Start()
	assert.NoError(t, err)

	fn(cs, address)
}
