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

package embed

import (
	"context"
	"errors"
	goruntime "runtime"
	"sync/atomic"
	"testing"

	commonruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/tnservice"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type embedGoexitRuntime struct {
	commonruntime.Runtime
	onClock func()
}

func (r *embedGoexitRuntime) Clock() clock.Clock {
	r.onClock()
	return r.Runtime.Clock()
}

type incompleteEmbedService struct {
	service
	err error
}

func (s *incompleteEmbedService) Close() error { return s.err }

type trackingEmbedFileService struct {
	fileservice.FileService
	closeCount atomic.Int32
}

func (s *trackingEmbedFileService) Close(ctx context.Context) {
	s.closeCount.Add(1)
	s.FileService.Close(ctx)
}

func TestTNConstructorNonReturningHandoffRetainsEmbedOwners(t *testing.T) {
	for _, exit := range []bool{false, true} {
		name := "panic"
		if exit {
			name = "Goexit"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			sid := t.Name()
			base := commonruntime.NewRuntime(
				metadata.ServiceType_TN,
				sid,
				zap.NewNop(),
				commonruntime.WithClock(clock.NewUnixNanoHLCClock(ctx, 0)),
			)
			commonruntime.SetupServiceBasedRuntime(sid, base)
			fs, err := fileservice.NewLocalFS(
				ctx,
				defines.LocalFileServiceName,
				t.TempDir(),
				fileservice.DisabledCacheConfig,
				nil,
			)
			require.NoError(t, err)
			cleanupErr := errors.New("embed owner cleanup incomplete")
			var actual tnservice.Service
			var recovered any
			returned := false
			cfg := newServiceConfig()
			cfg.DataDir = t.TempDir()
			cfg.ServiceType = metadata.ServiceType_TN.String()
			cfg.TN_please_use_getTNServiceConfig = &tnservice.Config{
				UUID:         sid,
				InStandalone: true,
			}
			op := &operator{
				cfg:         cfg,
				sid:         sid,
				serviceType: metadata.ServiceType_TN,
			}
			trackedFS := &trackingEmbedFileService{FileService: fs}
			op.reset.fs = trackedFS
			rt := &embedGoexitRuntime{Runtime: base}
			rt.onClock = func() {
				owner, ok := op.reset.svc.(tnservice.Service)
				if !ok || owner == nil {
					panic("production TN caller did not publish owner before clock access")
				}
				actual = owner
				op.reset.svc = &incompleteEmbedService{service: owner, err: cleanupErr}
				if exit {
					goruntime.Goexit()
				}
				panic(errors.New("constructor option interrupted"))
			}
			defer func() {
				if actual != nil {
					_ = actual.Close()
				}
				fs.Close(ctx)
			}()
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() { recovered = recover() }()
				op.reset.rt = rt
				_ = op.startTNServiceAfterReadyLocked(fs)
				returned = true
			}()
			<-done
			require.False(t, returned)
			if exit {
				require.Nil(t, recovered)
			} else {
				require.EqualError(t, recovered.(error), "constructor option interrupted")
			}
			require.NotNil(t, op.reset.svc)
			require.ErrorIs(t, op.Close(), cleanupErr)
			require.True(t, op.needsCleanup())
			require.Zero(t, trackedFS.closeCount.Load())
			require.NoError(t, fs.Write(ctx, fileservice.IOVector{
				FilePath: "embed-owner-control",
				Entries:  []fileservice.IOEntry{{Offset: 0, Size: 1, Data: []byte{3}}},
			}))
		})
	}
}
