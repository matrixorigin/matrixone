// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package compile

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func callMemoryControlRaw(t *testing.T, service string, request MemoryControlRequest) MemoryControlResponse {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := &memoryControlSession{ctx: ctx, replies: make(chan *pipeline.Message, 1)}
	wire, err := EncodeMemoryControlRequest(request)
	require.NoError(t, err)
	handled, err := handleMemoryControl(ctx, service, &pipeline.Message{Id: 19, Cmd: pipeline.Method_StopSending, Data: wire}, session, func() morpc.Message { return &pipeline.Message{} })
	require.True(t, handled)
	require.NoError(t, err)
	return receiveMemoryControl(t, session)
}

func TestMemoryV3RejectsIncompleteOrDifferentContractBeforeAdmission(t *testing.T) {
	require.Equal(t, uint32(3), query.MemoryAdmissionProtocolVersion)
	for _, action := range []string{MemoryControlOpen, MemoryControlTryOpen} {
		for _, kind := range []string{"nil", "stage-only", "v0", "v2", "v4"} {
			t.Run(action+"/"+kind, func(t *testing.T) {
				rt := runtime.NewRuntime(metadata.ServiceType_CN, t.Name(), zap.NewNop())
				runtime.SetupServiceBasedRuntime(t.Name(), rt)
				m := &compilerMemoryAdmission{failure: errors.New("version rejection must precede all admission")}
				rt.SetGlobalVariables(runtime.CNMemoryThrottler, m)
				manifest := memoryControlFixtureManifest(action == MemoryControlTryOpen)
				switch kind {
				case "stage-only":
					manifest.Producers = nil
				case "v0":
					manifest.Version = 0
				case "v2":
					manifest.Version = 2
				case "v4":
					manifest.Version = 4
				}
				request := MemoryControlRequest{Action: action, Manifest: &manifest, ExpectedProcessID: runtime.ProcessInstanceID()}
				if kind == "nil" {
					request.Manifest = nil
				}
				response := callMemoryControlRaw(t, t.Name(), request)
				require.NotEmpty(t, response.Error)
				require.Empty(t, response.Token)
				require.Zero(t, m.tries)
				require.Zero(t, m.waits)
				require.Zero(t, m.reclaims)
				statementMemoryOwners.Lock()
				defer statementMemoryOwners.Unlock()
				for key := range statementMemoryOwners.owners {
					require.NotEqual(t, t.Name(), key.service)
				}
			})
		}
	}
}

func TestMemoryV3CapabilityRequiresExactContractForEveryPeer(t *testing.T) {
	for _, version := range []uint32{0, 2, 3, 4} {
		for bad := 0; bad < 3; bad++ {
			t.Run(fmt.Sprintf("v%d/peer%d", version, bad), func(t *testing.T) {
				c := memoryCompileFixture(t, 1)
				entries := []*memoryCNManifest{{queryAddr: "A"}, {queryAddr: "B"}, {queryAddr: "C"}}
				badAddr := entries[bad].queryAddr
				client := &memoryCapabilityClient{send: func(addr string, _ *query.Request) (*query.Response, error) {
					peerVersion := uint32(3)
					if addr == badAddr {
						peerVersion = version
					}
					return &query.Response{GetProtocolVersion: &query.GetProtocolVersionResponse{MemoryAdmission: true, MemoryAdmissionProcessId: addr, MemoryAdmissionVersion: peerVersion}}, nil
				}}
				c.proc.Base.QueryClient = client
				capable, err := c.memoryCapabilities(context.Background(), entries)
				require.NoError(t, err)
				require.Equal(t, version == 3, capable)
				require.Equal(t, 3, client.releases)
				require.Empty(t, c.importMemoryControls)
				require.Nil(t, c.importMemory)
			})
		}
	}
}

// Frozen admission-front-door code from old2ca's handleMemoryControl, with its
// old constant fixed at2 and only its eventual AdmitMemoryManifest call replaced
// by the reserve seam. It tests the actual older receiver's semantic rejection;
// it must not follow the new protocol constant when this file is updated.
func literalV2ControlOpen(request MemoryControlRequest, reserve func() error) error {
	if request.Action == MemoryControlOpen || request.Action == MemoryControlTryOpen {
		if request.Manifest != nil && request.Manifest.NoWait != (request.Action == MemoryControlTryOpen) {
			return errors.New("memory control open wait policy mismatch")
		}
		if request.Manifest != nil && request.Manifest.Version != 2 {
			return errors.New("unsupported memory manifest version")
		}
		if request.Manifest == nil || request.Token != "" {
			return errors.New("invalid memory control open")
		}
		if request.ExpectedProcessID != runtime.ProcessInstanceID() {
			return errors.New("memory admission process changed after capability discovery")
		}
		return reserve()
	}
	return errors.New("not an open action")
}

func TestMemoryV3ManifestRejectedByLiteralV2Server(t *testing.T) {
	for _, action := range []string{MemoryControlOpen, MemoryControlTryOpen} {
		t.Run(action, func(t *testing.T) {
			manifest := memoryControlFixtureManifest(action == MemoryControlTryOpen)
			request := MemoryControlRequest{Action: action, Manifest: &manifest, ExpectedProcessID: runtime.ProcessInstanceID()}
			calls := 0
			sentinel := errors.New("literal old reserve reached")
			reserve := func() error { calls++; return sentinel }
			manifest.Version = 3
			require.ErrorContains(t, literalV2ControlOpen(request, reserve), "unsupported memory manifest version")
			require.Zero(t, calls)
			manifest.Version = 2
			require.ErrorIs(t, literalV2ControlOpen(request, reserve), sentinel)
			require.Equal(t, 1, calls, "matching old contract is the positive reserve control")
		})
	}
}

func TestLiteralV2OriginToV3ServerReturnsErrorAndReverseCloses(t *testing.T) {
	// The old origin's >=2 capability predicate accepts the new server. Its
	// version2 manifest then fails at the strict new front door, with no fallback.
	response := &query.GetProtocolVersionResponse{MemoryAdmission: true, MemoryAdmissionProcessId: "new-process", MemoryAdmissionVersion: 3}
	oldCapable := response.MemoryAdmission && response.MemoryAdmissionVersion >= uint32(2) && response.MemoryAdmissionProcessId != ""
	require.True(t, oldCapable)
	var order []string
	controls, _, err := acquireMemoryControls(context.Background(), []*memoryCNManifest{memoryControlCN("C"), memoryControlCN("B"), memoryControlCN("A")}, func(_ context.Context, entry *memoryCNManifest) (*statementMemoryControl, error) {
		order = append(order, "open"+entry.id)
		if entry.id == "C" {
			manifest := memoryControlFixtureManifest(false)
			manifest.Version = 2
			response := callMemoryControlRaw(t, "v2-to-v3-rejection-has-no-runtime", MemoryControlRequest{Action: MemoryControlOpen, Manifest: &manifest, ExpectedProcessID: runtime.ProcessInstanceID()})
			require.Empty(t, response.Token)
			return nil, errors.New(response.Error)
		}
		return &statementMemoryControl{unit: 64, closeLocal: func(error) { order = append(order, "close"+entry.id) }}, nil
	})
	require.ErrorContains(t, err, "unsupported memory manifest version")
	require.Nil(t, controls)
	require.Equal(t, []string{"openA", "openB", "openC", "closeB", "closeA"}, order)
}
