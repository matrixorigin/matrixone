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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
)

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
