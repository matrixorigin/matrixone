// Copyright 2021 - 2022 Matrix Origin
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

package tnservice

import (
	"context"
	"errors"
	"os"
	goruntime "runtime"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type metadataReadFailure struct {
	fileservice.ReplaceableFileService
	err error
}

func (f metadataReadFailure) Read(context.Context, *fileservice.IOVector) error { return f.err }

type constructorTNHA struct {
	*testHAKeeperClient
	closes int
}

func (h *constructorTNHA) Close() error { h.closes++; return nil }
func TestTNConstructorMetadataFailureRetiresOwners(t *testing.T) {
	ctx := context.Background()
	sid := t.Name()
	rt := runtime.NewRuntime(metadata.ServiceType_TN, sid, zap.NewNop(), runtime.WithClock(clock.NewUnixNanoHLCClock(ctx, 0)))
	runtime.SetupServiceBasedRuntime(sid, rt)
	fs, err := fileservice.NewMemoryFS(defines.LocalFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	sentinel := errors.New("metadata read refused")
	h := &constructorTNHA{testHAKeeperClient: newTestHAKeeperClient()}
	socketDir, err := os.MkdirTemp("/tmp", "qa-tn-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(socketDir)) })
	var acquired *store
	// Install caller cleanup before construction; the callback supplies its owner.
	t.Cleanup(func() {
		if acquired != nil {
			require.NoError(t, acquired.Close())
		}
	})
	cfg := &Config{UUID: sid, DataDir: t.TempDir(), InStandalone: true}
	cfg.LockService.ListenAddress = "unix://" + socketDir + "/lock.sock"
	cfg.LockService.ServiceAddress = cfg.LockService.ListenAddress
	owner, err := NewService(cfg, rt, metadataReadFailure{fs, sentinel}, nil, func(value Service) { acquired = value.(*store) },
		WithHAKeeperClientFactory(func() (logservice.TNHAKeeperClient, error) { return h, nil }))
	require.Nil(t, owner)
	require.Same(t, sentinel, err)
	require.Equal(t, 1, h.closes, "failed constructor must retire the acquired HA client")
	// The caller-owned FS is still usable; cleanup must not consume borrowed storage.
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{FilePath: "borrowed-control", Entries: []fileservice.IOEntry{{Offset: 0, Size: 1, Data: []byte{7}}}}))
	v := fileservice.IOVector{FilePath: "borrowed-control", Entries: []fileservice.IOEntry{{Offset: 0, Size: 1}}}
	require.NoError(t, fs.Read(ctx, &v))
	require.Equal(t, []byte{7}, v.Entries[0].Data)
}

func TestTNConstructorPrerequisiteFailureDoesNotStartIO(t *testing.T) {
	for _, hasClock := range []bool{false, true} {
		name := "missing clock"
		if hasClock {
			name = "HA acquisition"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			sid := t.Name()
			rt := runtime.NewRuntime(metadata.ServiceType_TN, sid, zap.NewNop())
			if hasClock {
				rt = runtime.NewRuntime(metadata.ServiceType_TN, sid, zap.NewNop(), runtime.WithClock(clock.NewUnixNanoHLCClock(ctx, 0)))
			}
			runtime.SetupServiceBasedRuntime(sid, rt)
			fs, err := fileservice.NewMemoryFS(defines.LocalFileServiceName, fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			t.Cleanup(func() { fs.Close(ctx) })
			var acquired Service
			t.Cleanup(func() {
				if acquired != nil {
					require.NoError(t, acquired.Close())
				}
				ioutil.Stop(sid)
			})
			sentinel := errors.New("HA acquisition refused")
			calls := 0
			owner, err := NewService(&Config{UUID: sid, DataDir: t.TempDir()}, rt, fs, nil, func(value Service) { acquired = value },
				WithHAKeeperClientFactory(func() (logservice.TNHAKeeperClient, error) { calls++; return nil, sentinel }))
			require.Nil(t, owner)
			if hasClock {
				require.Same(t, sentinel, err)
				require.Equal(t, 1, calls)
			} else {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadConfig))
				require.Zero(t, calls)
			}
			_, started := rt.GetGlobalVariables("blockio")
			require.False(t, started, "prerequisite failure must not acquire an I/O pipeline")
			require.Error(t, acquired.Start())
		})
	}
}

func TestTNConstructorUnwindPublishesOwner(t *testing.T) {
	for _, exit := range []bool{false, true} {
		name := "panic"
		if exit {
			name = "Goexit"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			rt := runtime.NewRuntime(metadata.ServiceType_TN, t.Name(), zap.NewNop())
			runtime.SetupServiceBasedRuntime(t.Name(), rt)
			fs, err := fileservice.NewMemoryFS(defines.LocalFileServiceName, fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			t.Cleanup(func() { fs.Close(ctx) })
			var owner Service
			t.Cleanup(func() {
				if owner != nil {
					require.NoError(t, owner.Close())
				}
			})
			sentinel := errors.New("option interrupted")
			var recovered any
			returned := false
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() { recovered = recover() }()
				_, _ = NewService(&Config{UUID: t.Name()}, rt, fs, nil,
					func(value Service) { owner = value },
					func(s *store) {
						if owner != s {
							panic("owner was not published before options")
						}
						if exit {
							goruntime.Goexit()
						}
						panic(sentinel)
					})
				returned = true
			}()
			<-done
			require.False(t, returned)
			if exit {
				require.Nil(t, recovered)
			} else {
				require.Same(t, sentinel, recovered)
			}
			require.NotNil(t, owner)
			require.True(t, moerr.IsMoErrCode(owner.Start(), moerr.ErrInvalidState))
			require.NoError(t, owner.Close())
		})
	}
}

func TestTNConstructorRetainsOwnerOnIncompleteCleanup(t *testing.T) {
	ctx := context.Background()
	sid := t.Name()
	rt := runtime.NewRuntime(metadata.ServiceType_TN, sid, zap.NewNop(), runtime.WithClock(clock.NewUnixNanoHLCClock(ctx, 0)))
	runtime.SetupServiceBasedRuntime(sid, rt)
	fs, err := fileservice.NewMemoryFS(defines.LocalFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	primary := errors.New("HA acquisition refused")
	cleanup := errors.New("RPC drain incomplete")
	var published *store
	// This fixture owns no live RPC handlers. Retire its retained stopper only
	// after proving construction preserves the owner and its cleanup error.
	t.Cleanup(func() {
		if published != nil && published.stopper != nil {
			published.stopper.Stop()
		}
	})
	owner, err := NewService(&Config{UUID: sid, DataDir: t.TempDir()}, rt, fs, nil,
		func(value Service) { published = value.(*store) },
		func(s *store) {
			s.server = &failingDrainServer{
				quiesce: func() error { return nil },
				drain:   func(context.Context) error { return cleanup },
			}
		},
		WithHAKeeperClientFactory(func() (logservice.TNHAKeeperClient, error) { return nil, primary }))
	require.Same(t, primary, err)
	require.Same(t, published, owner)
	require.Same(t, cleanup, owner.Close())
	require.True(t, moerr.IsMoErrCode(owner.Start(), moerr.ErrInvalidState))
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{FilePath: "retained-control", Entries: []fileservice.IOEntry{{Offset: 0, Size: 1, Data: []byte{9}}}}))
}
