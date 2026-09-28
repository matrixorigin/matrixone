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

package lockservice

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/stretchr/testify/require"
)

func TestGetBindRejectsInvalidAllocatorResult(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		const serviceID = "1234567890123456789bind-owner"
		binds := a.registerService(serviceID)
		if !a.canGetBind(serviceID) {
			t.Fatal("initial bind admission failed")
		}
		// Model the revocation between the admission check and Get. A
		// disabled bind still passes the old status-only check, but Get must
		// not turn its empty result into a successful RPC response.
		binds.disable()
		req := &pb.Request{Method: pb.Method_GetBind}
		req.GetBind.ServiceID = serviceID
		req.GetBind.Table = 42
		resp := &pb.Response{Method: pb.Method_GetBind}
		cs := &testClientSession{ctx: context.Background()}
		a.handleGetBind(context.Background(), nil, req, resp, cs)
		if !cs.writeCalled || !moerr.IsMoErrCode(resp.UnwrapError(), moerr.ErrLockTableBindChanged) {
			t.Fatalf("invalid bind was reported as success: response=%+v", resp.GetBind)
		}
	})
}

func TestInstanceBoundDrainColdCNNeedsFreshHeartbeat(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		const id = "1234567890123456789s1"
		const attempt = "cold-attempt"
		request := pb.BeginDrainRequest{ServiceID: id, AttemptID: attempt}
		if a.beginDrain(request).OK {
			t.Fatal("unobserved cold CN was accepted before its heartbeat")
		}
		bind := a.getServiceBinds(id)
		if bind == nil || !bind.drainAwaitingHeartbeat || bind.getStatus() != pb.Status_ServiceLockWaiting {
			t.Fatal("cold CN did not get a pending, non-admitting bind")
		}
		if a.canGetBind(id) || a.beginDrain(request).OK {
			t.Fatal("pending CN admitted a new bind or drain request")
		}
		if a.Get(id, 0, 1, 0, pb.Sharding_None).Valid {
			t.Fatal("GetBind admitted a table after the cold drain began")
		}
		query := pb.QueryDrainRequest{ServiceID: id, AttemptID: attempt,
			AllocatorID: a.allocatorID, AllocatorVersion: a.version}
		if a.queryDrain(query).Safe {
			t.Fatal("pending registration was mistaken for completion")
		}

		client, err := NewClient("", morpc.Config{})
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		sendHeartbeat := func(status pb.Status) pb.KeepLockTableBindResponse {
			req := acquireRequest()
			defer releaseRequest(req)
			req.Method = pb.Method_KeepLockTableBind
			req.KeepLockTableBind.ServiceID = id
			req.KeepLockTableBind.Status = status
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := client.Send(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			defer releaseResponse(resp)
			return resp.KeepLockTableBind
		}
		if sendHeartbeat(pb.Status_ServiceUnLockSucc).OK || a.beginDrain(request).OK || a.queryDrain(query).Safe {
			t.Fatal("stale completion heartbeat adopted a pending cold CN")
		}
		observed := sendHeartbeat(pb.Status_ServiceLockEnable)
		if !observed.OK || observed.Status != pb.Status_ServiceLockWaiting || !a.beginDrain(request).OK {
			t.Fatal("normal heartbeat did not establish the exact cold CN drain")
		}
		if a.queryDrain(query).Safe {
			t.Fatal("heartbeat was mistaken for completed drain")
		}
		if completed := sendHeartbeat(pb.Status_ServiceUnLockSucc); !completed.OK ||
			completed.Status != pb.Status_ServiceCanRestart || !a.queryDrain(query).Safe {
			t.Fatal("completed cold CN drain was not accepted")
		}
	})
}

func TestInstanceBoundDrainProtocol(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		const oldID = "1234567890123456789uuid1"
		const newID = "1234567890123456790uuid1"
		old := a.registerService(oldID)
		current := a.registerService(newID)
		if a.beginDrain(pb.BeginDrainRequest{ServiceID: "uuid1", AttemptID: "attempt-1"}).OK {
			t.Fatal("UUID alias was accepted as an incarnation")
		}
		if a.beginDrain(pb.BeginDrainRequest{ServiceID: newID, AttemptID: ""}).OK {
			t.Fatal("empty attempt was accepted")
		}
		begin := a.beginDrain(pb.BeginDrainRequest{ServiceID: newID, AttemptID: "attempt-1"})
		if !begin.OK || begin.ServiceID != newID || begin.AttemptID != "attempt-1" ||
			begin.AllocatorID != a.allocatorID || begin.AllocatorVersion != a.version {
			t.Fatalf("incorrect begin proof: %+v", begin)
		}
		if !a.beginDrain(pb.BeginDrainRequest{ServiceID: newID, AttemptID: "attempt-1"}).OK {
			t.Fatal("retry of the same attempt must be idempotent")
		}
		if a.beginDrain(pb.BeginDrainRequest{ServiceID: newID, AttemptID: "attempt-2"}).OK {
			t.Fatal("second attempt adopted a drain already in progress")
		}
		query := pb.QueryDrainRequest{
			ServiceID: newID, AttemptID: "attempt-1",
			AllocatorID: begin.AllocatorID, AllocatorVersion: begin.AllocatorVersion,
		}
		if a.queryDrain(query).Safe {
			t.Fatal("remote lock state was not safe yet")
		}
		// The real KeepLockTableBind completion disables this bind before
		// publishing CanRestart. That disabled flag is not an unsafe timeout.
		current.disable()
		current.setStatus(pb.Status_ServiceCanRestart)
		if !a.beginDrain(pb.BeginDrainRequest{ServiceID: newID, AttemptID: "attempt-1"}).OK {
			t.Fatal("retry of completed live attempt must remain idempotent")
		}
		if !a.queryDrain(query).Safe {
			t.Fatal("completed exact attempt was not accepted")
		}
		wrong := query
		wrong.AttemptID = "attempt-2"
		if a.queryDrain(wrong).Safe {
			t.Fatal("different attempt inherited completion")
		}
		wrong = query
		wrong.AllocatorVersion++
		if a.queryDrain(wrong).Safe {
			t.Fatal("allocator epoch mismatch inherited completion")
		}
		wrong = query
		wrong.AllocatorID = "other-allocator"
		if a.queryDrain(wrong).Safe {
			t.Fatal("allocator identity mismatch inherited completion")
		}
		wrong = query
		wrong.ServiceID = oldID
		old.setStatus(pb.Status_ServiceCanRestart)
		if a.queryDrain(wrong).Safe {
			t.Fatal("old incarnation inherited the current attempt")
		}
		a.disableTableBinds(current)
		if !a.beginDrain(pb.BeginDrainRequest{ServiceID: newID, AttemptID: "attempt-1"}).OK {
			t.Fatal("retry of safely retired attempt must remain idempotent")
		}
		if !a.queryDrain(query).Safe {
			t.Fatal("safe retirement lost its exact attempt proof")
		}
		if a.canGetBind(newID) || a.Get(newID, 0, 10, 0, pb.Sharding_None).Valid {
			t.Fatal("completed drain allowed its exact incarnation to rebind")
		}
		if !a.queryDrain(query).Safe {
			t.Fatal("late GetBind revoked the completed drain proof")
		}
	})
}

func TestInstanceBoundDrainRejectsLegacyOrLostState(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		const id = "1234567890123456789uuid1"
		legacy := a.registerService(id)
		legacy.requestRestart()
		if a.beginDrain(pb.BeginDrainRequest{ServiceID: id, AttemptID: "attempt"}).OK {
			t.Fatal("v2 adopted a legacy drain without an attempt proof")
		}
		a.disableTableBinds(legacy)
		if a.queryDrain(pb.QueryDrainRequest{
			ServiceID: id, AttemptID: "attempt",
			AllocatorID: a.allocatorID, AllocatorVersion: a.version,
		}).Safe {
			t.Fatal("missing or unsafe retirement was treated as complete")
		}
	})
}

func TestInstanceBoundDrainRetirementExpiresFailClosed(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		const id = "1234567890123456789uuid1"
		const attempt = "attempt-1"
		bind := a.registerService(id)
		begin := a.beginDrain(pb.BeginDrainRequest{ServiceID: id, AttemptID: attempt})
		if !begin.OK {
			t.Fatal("drain was not accepted")
		}
		bind.setStatus(pb.Status_ServiceCanRestart)
		a.disableTableBinds(bind)
		query := pb.QueryDrainRequest{
			ServiceID: id, AttemptID: attempt,
			AllocatorID: begin.AllocatorID, AllocatorVersion: begin.AllocatorVersion,
		}
		retiredAt := a.retiredAt[id]
		a.cleanRetiredServices(retiredAt.Add(23*time.Hour), 24*time.Hour)
		if !a.queryDrain(query).Safe ||
			!a.beginDrain(pb.BeginDrainRequest{ServiceID: id, AttemptID: attempt}).OK {
			t.Fatal("short lost-response retry window lost its exact proof")
		}
		a.cleanRetiredServices(retiredAt.Add(24*time.Hour), 24*time.Hour)
		if a.queryDrain(query).Safe ||
			a.beginDrain(pb.BeginDrainRequest{ServiceID: id, AttemptID: attempt}).OK {
			t.Fatal("expired retirement was treated as safe")
		}
		if len(a.retiredServices) != 0 || len(a.retiredDrainAttempts) != 0 || len(a.retiredAt) != 0 {
			t.Fatal("expired retirement records were retained")
		}
	})
}

func TestInstanceBoundDrainWireRoundTrip(t *testing.T) {
	request := pb.Request{
		Method: pb.Method_BeginDrain,
		BeginDrain: pb.BeginDrainRequest{
			ServiceID: "1234567890123456789uuid1", AttemptID: "attempt-1",
		},
	}
	data, err := request.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	var decoded pb.Request
	if err := decoded.Unmarshal(data); err != nil {
		t.Fatal(err)
	}
	if decoded.Method != pb.Method_BeginDrain || !reflect.DeepEqual(decoded.BeginDrain, request.BeginDrain) {
		t.Fatalf("drain request changed on the wire: %+v", decoded.BeginDrain)
	}
	response := pb.Response{
		Method: pb.Method_QueryDrain,
		QueryDrain: pb.QueryDrainResponse{
			Safe: true, ServiceID: request.BeginDrain.ServiceID,
			AttemptID:   request.BeginDrain.AttemptID,
			AllocatorID: "allocator-1", AllocatorVersion: 42,
		},
	}
	data, err = response.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	var decodedResponse pb.Response
	if err := decodedResponse.Unmarshal(data); err != nil {
		t.Fatal(err)
	}
	if decodedResponse.Method != pb.Method_QueryDrain || !reflect.DeepEqual(decodedResponse.QueryDrain, response.QueryDrain) {
		t.Fatalf("drain proof changed on the wire: %+v", decodedResponse.QueryDrain)
	}
}

// TestGetBindDrainInterleaving pauses the real handler, not a second admission
// probe. The allocator option is installed before the RPC server starts.
func TestGetBindDrainInterleaving(t *testing.T) {
	for _, tc := range []struct {
		name       string
		existing   bool
		transition string
	}{
		{"begin-new", false, "begin"},
		{"begin-existing", true, "begin"},
		{"legacy", false, "legacy"},
		{"retired", true, "retire"},
		{"no-drain", false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var admitted, resume chan struct{}
			runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
				const id = "1234567890123456789interleaving"
				const table = uint64(42)
				b := a.registerService(id)
				var original pb.LockTable
				if tc.existing {
					original = a.Get(id, 0, table, table, pb.Sharding_None)
					require.True(t, original.Valid)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				var release sync.Once
				done := make(chan struct{})
				defer func() {
					release.Do(func() { close(resume) })
					cancel()
					<-done
				}()
				req := &pb.Request{Method: pb.Method_GetBind}
				req.GetBind.ServiceID, req.GetBind.Table = id, table
				resp := &pb.Response{Method: pb.Method_GetBind}
				cs := &testClientSession{ctx: ctx}
				go func() {
					defer close(done)
					a.handleGetBind(ctx, nil, req, resp, cs)
				}()
				select {
				case <-admitted:
				case <-ctx.Done():
					t.Fatal("GetBind did not reach admission barrier")
				}
				var query pb.QueryDrainRequest
				switch tc.transition {
				case "begin", "retire":
					begin := a.beginDrain(pb.BeginDrainRequest{ServiceID: id, AttemptID: "attempt"})
					require.True(t, begin.OK)
					query = pb.QueryDrainRequest{ServiceID: id, AttemptID: "attempt",
						AllocatorID: begin.AllocatorID, AllocatorVersion: begin.AllocatorVersion}
					if tc.transition == "retire" {
						b.setStatus(pb.Status_ServiceCanRestart)
						a.disableTableBinds(b)
						require.True(t, a.queryDrain(query).Safe)
					}
				case "legacy":
					require.True(t, a.setRestartService(id))
				}
				release.Do(func() { close(resume) })
				select {
				case <-done:
				case <-ctx.Done():
					t.Fatal("GetBind did not finish after drain")
				}
				require.True(t, cs.writeCalled)
				if tc.transition == "" {
					require.NoError(t, resp.UnwrapError())
					require.True(t, resp.GetBind.LockTable.Valid)
					require.Equal(t, id, resp.GetBind.LockTable.ServiceID)
					require.Equal(t, table, resp.GetBind.LockTable.Table)
					return
				}
				require.True(t, moerr.IsMoErrCode(resp.UnwrapError(), moerr.ErrLockTableBindChanged))
				require.False(t, resp.GetBind.LockTable.Valid)
				// Read only after the handler has completed; no background heartbeat uses id.
				a.mu.RLock()
				stored, exists := a.mu.lockTables[0][table]
				a.mu.RUnlock()
				if tc.transition == "retire" {
					require.Nil(t, a.getServiceBinds(id), "late GetBind resurrected retirement")
					require.False(t, stored.Valid)
					require.True(t, a.queryDrain(query).Safe, "late GetBind revoked the proof")
				} else if tc.existing {
					require.Equal(t, original, stored, "stale request changed an existing bind")
				} else {
					require.False(t, exists, "stale request allocated a table")
				}
				// The next request sees drain at admission and must not enter the hook.
				next := &pb.Response{Method: pb.Method_GetBind}
				a.handleGetBind(ctx, nil, req, next, &testClientSession{ctx: ctx})
				require.True(t, moerr.IsMoErrCode(next.UnwrapError(), moerr.ErrNewTxnInCNRollingRestart))
			}, func(a *lockTableAllocator) {
				admitted, resume = make(chan struct{}), make(chan struct{})
				a.options.afterGetBindAdmission = func() { close(admitted); <-resume }
			})
		})
	}
}

func TestInstanceBoundDrainCanonicalClient(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		rt := moruntime.ServiceRuntime("")
		version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
		require.True(t, ok)
		defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		var calls atomic.Int32
		// No incoming requests exist until this client is constructed.
		a.server.RegisterMethodHandler(pb.Method_BeginDrain, func(ctx context.Context, cancel context.CancelFunc,
			req *pb.Request, resp *pb.Response, cs morpc.ClientSession) {
			calls.Add(1)
			a.handleBeginDrain(ctx, cancel, req, resp, cs)
		})
		a.server.RegisterMethodHandler(pb.Method_QueryDrain, func(ctx context.Context, cancel context.CancelFunc,
			req *pb.Request, resp *pb.Response, cs morpc.ClientSession) {
			calls.Add(1)
			a.handleQueryDrain(ctx, cancel, req, resp, cs)
		})
		client, err := NewClient("", morpc.Config{})
		require.NoError(t, err)
		defer client.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		send := func(method pb.Method, fill func(*pb.Request)) *pb.Response {
			req := acquireRequest()
			defer releaseRequest(req)
			req.Method = method
			fill(req)
			resp, err := client.Send(ctx, req)
			require.NoError(t, err)
			return resp
		}
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion99)
		for _, method := range []pb.Method{pb.Method_BeginDrain, pb.Method_QueryDrain} {
			req := acquireRequest()
			req.Method = method
			resp, err := client.Send(ctx, req)
			releaseRequest(req)
			require.Nil(t, resp)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
		}
		require.Zero(t, calls.Load(), "unsupported methods reached transport")
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion100)
		const id = "1234567890123456789canonical"
		a.registerService(id)
		resp := send(pb.Method_BeginDrain, func(req *pb.Request) {
			req.BeginDrain = pb.BeginDrainRequest{ServiceID: id, AttemptID: "attempt"}
		})
		begin := resp.BeginDrain
		releaseResponse(resp)
		require.True(t, begin.OK)
		require.Equal(t, id, begin.ServiceID)
		require.Equal(t, "attempt", begin.AttemptID)
		require.Equal(t, a.allocatorID, begin.AllocatorID)
		require.Equal(t, a.version, begin.AllocatorVersion)
		query := pb.QueryDrainRequest{ServiceID: id, AttemptID: "attempt",
			AllocatorID: begin.AllocatorID, AllocatorVersion: begin.AllocatorVersion}
		querySafe := func(q pb.QueryDrainRequest) bool {
			resp := send(pb.Method_QueryDrain, func(req *pb.Request) { req.QueryDrain = q })
			defer releaseResponse(resp)
			require.Equal(t, q.ServiceID, resp.QueryDrain.ServiceID)
			require.Equal(t, q.AttemptID, resp.QueryDrain.AttemptID)
			require.Equal(t, a.allocatorID, resp.QueryDrain.AllocatorID)
			require.Equal(t, a.version, resp.QueryDrain.AllocatorVersion)
			return resp.QueryDrain.Safe
		}
		require.False(t, querySafe(query))
		resp = send(pb.Method_KeepLockTableBind, func(req *pb.Request) {
			req.KeepLockTableBind.ServiceID = id
			req.KeepLockTableBind.Status = pb.Status_ServiceUnLockSucc
		})
		heartbeat := resp.KeepLockTableBind
		releaseResponse(resp)
		require.True(t, heartbeat.OK)
		require.Equal(t, pb.Status_ServiceCanRestart, heartbeat.Status)
		require.True(t, querySafe(query))
		for _, change := range []func(*pb.QueryDrainRequest){
			func(q *pb.QueryDrainRequest) { q.AttemptID = "other" },
			func(q *pb.QueryDrainRequest) { q.AllocatorID = "other" },
			func(q *pb.QueryDrainRequest) { q.AllocatorVersion++ },
		} {
			wrong := query
			change(&wrong)
			require.False(t, querySafe(wrong))
		}
		require.Equal(t, int32(6), calls.Load())
		canceled, stop := context.WithCancel(ctx)
		stop()
		req := acquireRequest()
		defer releaseRequest(req)
		req.Method, req.QueryDrain = pb.Method_QueryDrain, query
		resp, err = client.Send(canceled, req)
		require.Error(t, err)
		require.Nil(t, resp)
	})
}
