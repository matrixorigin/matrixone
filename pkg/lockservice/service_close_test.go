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

package lockservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/stretchr/testify/require"
)

func TestAllocatorCloseReportRequiresConfirmedIdentityAndEpoch(t *testing.T) {
	for _, name := range []string{"confirmed", "pending", "unsupported", "wrong-service", "wrong-attempt", "missing-epoch", "zero-epoch", "rejected", "replaced", "terminal-error"} {
		t.Run(name, func(t *testing.T) {
			s := &service{serviceID: "1234567890123456789closed-cn", lastAllocatorID: "allocator", lastAllocatorVersion: 1}
			calls := 0
			transportError := errors.New("terminal transport failed")
			s.remote.client = &bindResponseClient{send: func(ctx context.Context, req *pb.Request) (*pb.Response, error) {
				calls++
				if req.Method == pb.Method_BeginDrain && name == "unsupported" {
					return nil, moerr.NewNotSupportedNoCtx("old allocator")
				}
				if req.Method == pb.Method_KeepLockTableBind && name == "terminal-error" {
					return nil, transportError
				}
				resp := acquireResponse()
				switch req.Method {
				case pb.Method_BeginDrain:
					require.Equal(t, s.serviceID, req.BeginDrain.ServiceID)
					resp.BeginDrain = pb.BeginDrainResponse{OK: name != "pending", ServiceID: req.BeginDrain.ServiceID,
						AttemptID: req.BeginDrain.AttemptID, AllocatorID: "allocator", AllocatorVersion: 1}
					if name == "wrong-service" {
						resp.BeginDrain.ServiceID = "another-incarnation"
					}
					if name == "wrong-attempt" {
						resp.BeginDrain.AttemptID = "another-attempt"
					}
					if name == "missing-epoch" {
						resp.BeginDrain.AllocatorID = ""
					}
					if name == "zero-epoch" {
						resp.BeginDrain.AllocatorVersion = 0
					}
				case pb.Method_KeepLockTableBind:
					require.Equal(t, s.serviceID, req.KeepLockTableBind.ServiceID)
					require.Equal(t, pb.Status_ServiceUnLockSucc, req.KeepLockTableBind.Status)
					require.Empty(t, req.KeepLockTableBind.TxnIDs)
					require.Equal(t, "allocator", req.KeepLockTableBind.ObservedAllocatorID)
					require.Equal(t, uint64(1), req.KeepLockTableBind.ObservedAllocatorVersion)
					resp.KeepLockTableBind = pb.KeepLockTableBindResponse{OK: name != "rejected",
						Status: pb.Status_ServiceCanRestart, AllocatorID: "allocator", AllocatorVersion: 1}
					if name == "replaced" {
						resp.KeepLockTableBind.AllocatorID = "replacement"
					}
				default:
					t.Fatalf("unexpected report method: %v", req.Method)
				}
				return resp, nil
			}}
			err := s.notifyAllocatorOfClose(context.Background())
			if name == "confirmed" || name == "pending" {
				require.NoError(t, err)
				require.Equal(t, 2, calls)
			} else {
				require.Error(t, err)
				if name == "terminal-error" {
					require.ErrorIs(t, err, transportError)
				}
			}
		})
	}
}

func TestAllocatorCloseReportStopsOnCancellation(t *testing.T) {
	started := make(chan struct{})
	done := make(chan error, 1)
	finished := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	s := &service{serviceID: "1234567890123456789closed-cn"}
	s.remote.client = &bindResponseClient{send: func(ctx context.Context, req *pb.Request) (*pb.Response, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	go func() { defer close(finished); done <- s.notifyAllocatorOfClose(ctx) }()
	defer func() { cancel(); <-finished }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("close report did not start")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("close report ignored cancellation")
	}
}
