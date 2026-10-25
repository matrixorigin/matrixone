// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package colexec

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestRemoteReceiverAttachmentLifecycle(t *testing.T) {
	for _, mode := range []string{"publication", "completion", "full", "waiter", "racing_completion", "cancel_generation"} {
		t.Run(mode, func(t *testing.T) {
			srv := &Server{uuidCsChanMap: UuidProcMap{mp: make(map[uuid.UUID]uuidProcMapItem), changed: make(chan struct{})}}
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			proc := &process.Process{Ctx: ctx, Cancel: cancel}
			uid := uuid.New()
			peer := &process.WrapCs{Uid: uid, Err: make(chan error, 1)}
			queue := make(process.RemotePipelineInformationChannel, 1)
			if mode == "full" {
				queue = make(process.RemotePipelineInformationChannel)
			}
			var changed <-chan struct{}
			if mode == "waiter" {
				owner, found, wait, err := srv.AttachRemoteReceiver(uid, peer)
				require.NoError(t, err)
				require.False(t, found)
				require.Nil(t, owner)
				require.NotNil(t, wait)
				changed = wait
			}
			require.NoError(t, srv.PutProcIntoUuidMap(uid, proc, queue))
			if changed != nil {
				select {
				case <-changed:
				default:
					t.Fatal("registration did not wake the notifier")
				}
			}
			if mode == "completion" {
				srv.DeleteUuids([]uuid.UUID{uid})
			}
			retired := make(chan struct{})
			if mode == "racing_completion" {
				go func() { srv.DeleteUuids([]uuid.UUID{uid}); close(retired) }()
			}
			owner, found, wait, err := srv.AttachRemoteReceiver(uid, peer)
			require.True(t, found)
			require.Nil(t, wait)
			if mode == "full" {
				require.ErrorContains(t, err, "queue is full")
				require.Nil(t, owner)
				registered, ch, ok := srv.GetProcByUuid(uid, false)
				require.True(t, ok)
				require.Same(t, proc, registered)
				require.Equal(t, queue, ch)
			} else {
				require.NoError(t, err)
				if mode == "completion" {
					require.Nil(t, owner)
				} else if mode != "racing_completion" {
					require.NotNil(t, owner)
				}
				if mode == "racing_completion" {
					<-retired
				}
				if owner == nil {
					require.Empty(t, queue)
				} else {
					require.Len(t, queue, 1)
					require.Same(t, peer, <-queue)
					if mode == "cancel_generation" {
						nextCtx, nextCancel := context.WithCancelCause(context.Background())
						defer nextCancel(nil)
						proc.Ctx, proc.Cancel = nextCtx, nextCancel
						cause := moerr.NewInternalErrorNoCtx("old receiver disconnected")
						owner(cause)
						require.ErrorIs(t, context.Cause(ctx), cause)
						require.NoError(t, nextCtx.Err())
					}
				}
			}
			srv.DeleteUuids([]uuid.UUID{uid})
			require.Empty(t, srv.uuidCsChanMap.mp)
		})
	}
}
