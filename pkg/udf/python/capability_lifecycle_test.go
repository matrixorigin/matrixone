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

package python

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/udf/protocol"
	"google.golang.org/grpc"
)

type blockedCapabilityClient struct {
	flight.FlightServiceClient
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockedCapabilityClient) DoAction(ctx context.Context, _ *flight.Action, _ ...grpc.CallOption) (flight.FlightService_DoActionClient, error) {
	c.once.Do(func() { close(c.started) })
	select {
	case <-c.release:
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, errors.New("released test capability probe")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestCancelledReadinessDoesNotWaitForOtherProbe(t *testing.T) {
	gateway, err := NewGateway(ClientConfig{Enabled: true, AllowUnisolated: true, ServerAddress: "127.0.0.1:1", RequestTimeout: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer gateway.Close()
	client := &blockedCapabilityClient{started: make(chan struct{}), release: make(chan struct{})}
	gateway.flight = client
	firstDone := make(chan error, 1)
	go func() { firstDone <- gateway.CheckLanguageReady(context.Background(), udf.LanguagePython) }()
	select {
	case <-client.started:
	case <-time.After(time.Second):
		t.Fatal("first probe did not reach the Flight boundary")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	secondStarted := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		close(secondStarted)
		secondDone <- gateway.CheckLanguageReady(ctx, udf.LanguagePython)
	}()
	<-secondStarted
	blocked := false
	select {
	case err := <-secondDone:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("cancelled probe returned %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		blocked = true
	}
	close(client.release)
	<-firstDone
	if blocked {
		if err := <-secondDone; !errors.Is(err, context.Canceled) {
			t.Errorf("released cancelled probe returned %v", err)
		}
		t.Fatal("already-cancelled readiness blocked behind another request's capabilityMu/Flight probe; it returned only after the unrelated probe was released")
	}
}

func TestCapabilityInvalidationDoesNotWaitForProbe(t *testing.T) {
	g, err := NewGateway(ClientConfig{Enabled: true, AllowUnisolated: true, ServerAddress: "127.0.0.1:1", RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	c := &blockedCapabilityClient{started: make(chan struct{}), release: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- g.ensureCapabilities(context.Background(), c, true) }()
	<-c.started
	invalidated := make(chan struct{})
	go func() { g.invalidateCapabilities(); close(invalidated) }()
	select {
	case <-invalidated:
	case <-time.After(time.Second):
		close(c.release)
		<-done
		t.Fatal("invalidation blocked on RPC")
	}
	if err := <-done; err == nil {
		t.Fatal("invalidated probe succeeded")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.capabilityReady || g.workerLeaseEpoch != 0 || g.capabilityProbe != nil {
		t.Fatal("probe retained state")
	}
}

func TestStaleCapabilityFailurePreservesNewGeneration(t *testing.T) {
	g := &Gateway{capabilityGeneration: 2, capabilityReady: true, workerLeaseEpoch: 42}
	g.invalidateCapabilities(1)
	if !g.capabilityReady || g.workerLeaseEpoch != 42 {
		t.Fatal("old failure invalidated new lease")
	}
	g.invalidateCapabilities(2)
	if g.capabilityReady || g.workerLeaseEpoch != 0 {
		t.Fatal("current failure retained lease")
	}
}

func currentTestCapabilities() capabilityResponse {
	response := capabilityResponse{
		ProtocolVersion:            protocol.Version,
		ABIContract:                udf.PythonABIContract,
		AdapterVersion:             udf.PythonAdapterVersion,
		SDKVersion:                 udf.PythonSDKVersion,
		DefinitionSchemaVersion:    udf.PythonDefinitionSchemaVersion,
		PlanContractVersion:        udf.PythonPlanContractVersion,
		TypeDescriptorContract:     udf.PythonTypeDescriptorContract,
		TimezoneDatabaseVersion:    currentTimezoneDatabaseVersion(),
		Modes:                      []string{ModeScalar, ModeVector},
		NullPolicies:               []string{NullCallHandler, NullReturnNull},
		WindowBatches:              1,
		MaxExecutionFrameBytes:     1 << 30,
		MaxHandlerProcesses:        DefaultWorkerMaxHandlerProcesses,
		MaxAccountHandlerProcesses: DefaultWorkerMaxAccountHandlers,
		MaxOwnerHandlerProcesses:   DefaultWorkerMaxOwnerHandlers,
	}
	response.LeaseEpoch = 42
	return response
}

type delayedCapabilityClient struct {
	flight.FlightServiceClient
	started  chan struct{}
	release  chan struct{}
	response capabilityResponse
}

func (c *delayedCapabilityClient) DoAction(ctx context.Context, _ *flight.Action, _ ...grpc.CallOption) (flight.FlightService_DoActionClient, error) {
	close(c.started)
	<-c.release // Deliberately ignore cancellation: exercise the publication fence.
	payload, err := json.Marshal(c.response)
	return &finishAckActionStream{result: &flight.Result{Body: payload}}, err
}
func TestCapabilityLateResponseCannotRepublish(t *testing.T) {
	for _, closeGateway := range []bool{false, true} {
		t.Run(map[bool]string{false: "invalidate", true: "close"}[closeGateway], func(t *testing.T) {
			g, err := NewGateway(ClientConfig{Enabled: true, AllowUnisolated: true, ServerAddress: "127.0.0.1:1", RequestTimeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			c := &delayedCapabilityClient{started: make(chan struct{}), release: make(chan struct{}), response: currentTestCapabilities()}
			done := make(chan error, 1)
			go func() { done <- g.ensureCapabilities(context.Background(), c, true) }()
			<-c.started
			// Close/invalidation must return while a misbehaving transport is blocked.
			if closeGateway {
				if err := g.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				g.invalidateCapabilities()
			}
			g.mu.Lock()
			occupied := g.capabilityProbe != nil
			g.mu.Unlock()
			if !occupied {
				t.Error("cancelled physical probe released its slot too early")
			}
			close(c.release)
			if err := <-done; err == nil {
				t.Fatal("late success escaped generation fence")
			}
			g.mu.Lock()
			defer g.mu.Unlock()
			if g.capabilityReady || g.workerLeaseEpoch != 0 || g.capability != nil || g.capabilityProbe != nil {
				t.Fatal("late response republished state")
			}
		})
	}
}
