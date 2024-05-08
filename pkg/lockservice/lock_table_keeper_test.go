// Copyright 2023 Matrix Origin
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

package lockservice

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/stretchr/testify/assert"
)

func TestKeeper(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			n1 := atomic.Uint64{}
			n2 := atomic.Uint64{}
			c1 := make(chan struct{})
			c2 := make(chan struct{})
			s.RegisterMethodHandler(
				pb.Method_KeepLockTableBind,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {

					if n1.Add(1) == 10 {
						close(c1)
					}
					writeResponse(ctx, cancel, resp, nil, cs)
				})
			s.RegisterMethodHandler(
				pb.Method_KeepRemoteLock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {

					if n2.Add(1) == 10 {
						close(c2)
					}
					writeResponse(ctx, cancel, resp, nil, cs)
				})
			m := &lockTableHolders{service: "s1", holders: map[uint32]*lockTableHolder{}}
			m.set(
				0,
				0,
				newRemoteLockTable(
					"s1",
					time.Second,
					pb.LockTable{ServiceID: "s2"},
					c,
					func(lt pb.LockTable) {}))
			m.set(
				0,
				1,
				newRemoteLockTable(
					"s1",
					time.Second,
					pb.LockTable{ServiceID: "s1"},
					c,
					func(lt pb.LockTable) {}))
			k := NewLockTableKeeper(
				"s1",
				c,
				time.Millisecond*10,
				time.Millisecond*10,
				m,
				&service{})
			defer func() {
				assert.NoError(t, k.Close())
			}()
			<-c1
			<-c2
		},
	)
}

func TestKeepBindFailedWillRemoveAllLocalLockTable(t *testing.T) {
	runRPCTests(
		t,
		func(c Client, s Server) {
			events := newWaiterEvents(1, nil, nil, nil)
			defer events.close()

			s.RegisterMethodHandler(
				pb.Method_KeepLockTableBind,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					resp.KeepLockTableBind.OK = false
					writeResponse(ctx, cancel, resp, nil, cs)
				})

			s.RegisterMethodHandler(
				pb.Method_KeepRemoteLock,
				func(
					ctx context.Context,
					cancel context.CancelFunc,
					req *pb.Request,
					resp *pb.Response,
					cs morpc.ClientSession) {
					writeResponse(ctx, cancel, resp, nil, cs)
				})

			m := &lockTableHolders{service: "s1", holders: map[uint32]*lockTableHolder{}}
			m.set(
				0,
				1,
				newLocalLockTable(
					pb.LockTable{ServiceID: "s1"},
					nil,
					events,
					runtime.DefaultRuntime().Clock(),
					nil))
			m.set(
				0,
				2,
				newLocalLockTable(
					pb.LockTable{ServiceID: "s1"},
					nil,
					events,
					runtime.DefaultRuntime().Clock(),
					nil))
			m.set(
				0,
				3,
				newRemoteLockTable(
					"s1",
					time.Second,
					pb.LockTable{ServiceID: "s2"},
					c,
					func(lt pb.LockTable) {}))
			k := NewLockTableKeeper(
				"s1",
				c,
				time.Millisecond*10,
				time.Millisecond*10,
				m,
				&service{})
			defer func() {
				assert.NoError(t, k.Close())
			}()

			for {
				v := 0
				m.iter(func(key uint64, value lockTable) bool {
					v++
					return true
				})
				if v == 1 {
					v := m.get(0, 3)
					assert.NotNil(t, v)
					return
				}
				time.Sleep(time.Millisecond * 100)
			}
		},
	)
}
