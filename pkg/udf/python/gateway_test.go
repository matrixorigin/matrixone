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
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/udf/protocol"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type gatewayResultStream struct {
	grpc.ClientStream
	results []*flight.FlightData
	index   int
}

type scriptedGatewayResultStream struct {
	grpc.ClientStream
	results []*flight.FlightData
	errors  []error
	index   int
}

func (s *scriptedGatewayResultStream) Recv() (*flight.FlightData, error) {
	index := s.index
	s.index++
	if index < len(s.results) {
		return s.results[index], nil
	}
	if index < len(s.errors) {
		return nil, s.errors[index]
	}
	return nil, io.EOF
}

func (s *scriptedGatewayResultStream) Send(*flight.FlightData) error {
	return nil
}

type gatedGatewayResultStream struct {
	grpc.ClientStream
	results  []*flight.FlightData
	index    int
	release  <-chan struct{}
	waiting  chan<- struct{}
	waitOnce sync.Once
}

type finishAckActionStream struct {
	grpc.ClientStream
	result *flight.Result
	err    error
	read   bool
}

func (s *finishAckActionStream) Recv() (*flight.Result, error) {
	if !s.read {
		s.read = true
		if s.err != nil {
			return nil, s.err
		}
		return s.result, nil
	}
	return nil, io.EOF
}

type finishAckFlightClient struct {
	flight.FlightServiceClient
	calls   int
	actions []*flight.Action
}

type resultAckFlightClient struct {
	flight.FlightServiceClient
	response []byte
	err      error
}

func (c *resultAckFlightClient) DoAction(context.Context, *flight.Action, ...grpc.CallOption) (flight.FlightService_DoActionClient, error) {
	return &finishAckActionStream{result: &flight.Result{Body: c.response}, err: c.err}, nil
}

type capabilityActionClient struct {
	flight.FlightServiceClient
	response []byte
	err      error
}

func (c *capabilityActionClient) DoAction(
	_ context.Context,
	action *flight.Action,
	_ ...grpc.CallOption,
) (flight.FlightService_DoActionClient, error) {
	if c.err != nil {
		return nil, c.err
	}
	if action.Type != "GetPythonCapabilities" {
		return nil, errors.New("unexpected capability action")
	}
	return &finishAckActionStream{result: &flight.Result{Body: c.response}}, nil
}

type scriptedCapabilityActionStream struct {
	grpc.ClientStream
	results []*flight.Result
	errors  []error
	index   int
}

func (s *scriptedCapabilityActionStream) Recv() (*flight.Result, error) {
	index := s.index
	s.index++
	if index < len(s.results) {
		return s.results[index], nil
	}
	if index < len(s.errors) {
		return nil, s.errors[index]
	}
	return nil, io.EOF
}

type scriptedCapabilityFlightClient struct {
	flight.FlightServiceClient
	actionErr error
	stream    *scriptedCapabilityActionStream
	actions   []*flight.Action
}

func (c *scriptedCapabilityFlightClient) DoAction(
	_ context.Context,
	action *flight.Action,
	_ ...grpc.CallOption,
) (flight.FlightService_DoActionClient, error) {
	c.actions = append(c.actions, action)
	if c.actionErr != nil {
		return nil, c.actionErr
	}
	return c.stream, nil
}

type recordingArtifactResolver struct {
	source  string
	calls   int
	account uint64
	handler string
	digest  string
}

type blockingArtifactResolver struct{}

func (blockingArtifactResolver) Resolve(ctx context.Context, _ uint64, _, _ string) (string, error) {
	<-ctx.Done()
	return "", ctx.Err()
}

type closeBlockingArtifactResolver struct {
	started chan struct{}
}

func (r *closeBlockingArtifactResolver) Resolve(ctx context.Context, _ uint64, _, _ string) (string, error) {
	close(r.started)
	<-ctx.Done()
	return "", ctx.Err()
}

func (r *recordingArtifactResolver) Resolve(_ context.Context, accountID uint64, handler, digest string) (string, error) {
	r.calls++
	r.account = accountID
	r.handler = handler
	r.digest = digest
	return r.source, nil
}

func TestValidateDefinitionBoundsArtifactResolution(t *testing.T) {
	invocation := validInvocation()
	gateway, err := NewGatewayWithArtifactStore(ClientConfig{
		Enabled:         true,
		AllowUnisolated: true,
		ServerAddress:   "127.0.0.1:1",
		RequestTimeout:  20 * time.Millisecond,
	}, blockingArtifactResolver{})
	require.NoError(t, err)
	defer gateway.Close()

	definition := &udf.RoutineDefinition{
		Language:                udf.LanguagePython,
		AccountID:               invocation.FunctionRef.AccountID,
		Handler:                 invocation.Handler,
		Args:                    append([]types.Type(nil), invocation.Args...),
		ReturnType:              invocation.ReturnType,
		Mode:                    invocation.Mode,
		NullPolicy:              invocation.NullPolicy,
		ABIContract:             invocation.ABIContract,
		AdapterVersion:          invocation.AdapterVersion,
		SDKVersion:              invocation.SDKVersion,
		DefinitionSchemaVersion: invocation.DefinitionSchemaVersion,
		ArtifactDigest:          invocation.ArtifactDigest,
		EnvironmentDigest:       invocation.EnvironmentDigest,
		DefinitionFingerprint:   invocation.DefinitionFingerprint,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- gateway.ValidateDefinition(ctx, definition) }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(500 * time.Millisecond):
		cancel()
		require.FailNow(t, "artifact resolution was not bounded by the gateway request timeout")
	}
}

func TestGatewayCloseCancelsDefinitionResolution(t *testing.T) {
	invocation := validInvocation()
	resolver := &closeBlockingArtifactResolver{started: make(chan struct{})}
	gateway, err := NewGatewayWithArtifactStore(ClientConfig{
		Enabled:         true,
		AllowUnisolated: true,
		ServerAddress:   "127.0.0.1:1",
		RequestTimeout:  30 * time.Second,
	}, resolver)
	require.NoError(t, err)

	definition := &udf.RoutineDefinition{
		Language:                udf.LanguagePython,
		AccountID:               invocation.FunctionRef.AccountID,
		Handler:                 invocation.Handler,
		Args:                    append([]types.Type(nil), invocation.Args...),
		ReturnType:              invocation.ReturnType,
		Mode:                    invocation.Mode,
		NullPolicy:              invocation.NullPolicy,
		ABIContract:             invocation.ABIContract,
		AdapterVersion:          invocation.AdapterVersion,
		SDKVersion:              invocation.SDKVersion,
		DefinitionSchemaVersion: invocation.DefinitionSchemaVersion,
		ArtifactDigest:          invocation.ArtifactDigest,
		EnvironmentDigest:       invocation.EnvironmentDigest,
		DefinitionFingerprint:   invocation.DefinitionFingerprint,
	}
	done := make(chan error, 1)
	go func() { done <- gateway.ValidateDefinition(context.Background(), definition) }()
	select {
	case <-resolver.started:
	case <-time.After(time.Second):
		require.FailNow(t, "artifact resolution did not start")
	}
	require.NoError(t, gateway.Close())
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		require.FailNow(t, "Gateway.Close did not cancel artifact resolution")
	}
}

func (c *finishAckFlightClient) DoAction(
	ctx context.Context,
	action *flight.Action,
	opts ...grpc.CallOption,
) (flight.FlightService_DoActionClient, error) {
	c.calls++
	c.actions = append(c.actions, action)
	if c.calls == 1 {
		return &finishAckActionStream{err: errors.New("injected ACK response loss")}, nil
	}
	tuple := validInvocation().Tuple
	ack, err := protocol.MarshalControl(protocol.Control{
		Kind: "Ack", Tuple: tuple, Status: statusOK, FinishID: "finish-1",
	})
	if err != nil {
		return nil, err
	}
	return &finishAckActionStream{result: &flight.Result{Body: ack}}, nil
}

func (s *gatewayResultStream) Recv() (*flight.FlightData, error) {
	if s.index == len(s.results) {
		return nil, io.EOF
	}
	result := s.results[s.index]
	s.index++
	return result, nil
}

func (s *gatewayResultStream) Send(*flight.FlightData) error {
	return nil
}

func (s *gatedGatewayResultStream) Recv() (*flight.FlightData, error) {
	if s.index < len(s.results) {
		result := s.results[s.index]
		s.index++
		return result, nil
	}
	s.waitOnce.Do(func() { close(s.waiting) })
	<-s.release
	return nil, io.EOF
}

func (s *gatedGatewayResultStream) Send(*flight.FlightData) error {
	return nil
}

func gatewayControl(t *testing.T, kind string, tuple protocol.FencingTuple, fields ...func(*protocol.Control)) *flight.FlightData {
	t.Helper()
	control := protocol.Control{Kind: kind, Tuple: tuple}
	for _, field := range fields {
		field(&control)
	}
	encoded, err := protocol.MarshalControl(control)
	require.NoError(t, err)
	return &flight.FlightData{AppMetadata: encoded}
}

func validInvocation() *udf.Invocation {
	return &udf.Invocation{
		FunctionRef: udf.FunctionRef{
			AccountID: 1, DatabaseID: 2, FunctionID: 3, Revision: 1, NamespaceVersion: 1,
		},
		Language:                udf.LanguagePython,
		Handler:                 "add",
		Source:                  "def add(ctx, value): return value",
		Args:                    []types.Type{types.T_int64.ToType()},
		ReturnType:              types.T_int64.ToType(),
		Length:                  0,
		Inputs:                  []*vector.Vector{nil},
		Mode:                    ModeScalar,
		NullPolicy:              NullCallHandler,
		ABIContract:             udf.PythonABIContract,
		AdapterVersion:          udf.PythonAdapterVersion,
		SDKVersion:              udf.PythonSDKVersion,
		DefinitionSchemaVersion: udf.PythonDefinitionSchemaVersion,
		ArtifactDigest:          udf.PythonInlineArtifactDigest("add", "def add(ctx, value): return value"),
		EnvironmentDigest:       func() string { digest, _ := udf.PythonEnvironmentDigest(); return digest }(),
		DefinitionFingerprint: func() string {
			argument, _ := NewTypeDescriptor(types.T_int64.ToType())
			fingerprint, _ := DefinitionFingerprint(
				udf.PythonDefinitionSchemaVersion,
				"add", ModeScalar, NullCallHandler,
				udf.PythonABIContract, udf.PythonAdapterVersion,
				udf.PythonInlineArtifactDigest("add", "def add(ctx, value): return value"),
				func() string { digest, _ := udf.PythonEnvironmentDigest(); return digest }(),
				udf.PythonSDKVersion, []TypeDescriptor{argument}, argument,
			)
			return fingerprint
		}(),
		CallsiteID:   "python/test",
		MayError:     true,
		SecurityMode: "INVOKER",
		StatementContext: &udf.StatementContext{
			ContractVersion:       udf.StatementContextContractVersion,
			StatementTimestampUTC: 1704067200123456,
			TimezoneKind:          "FIXED_OFFSET",
			TimezoneOffsetMinutes: 480,
			SQLMode:               []string{"ANSI", "STRICT_TRANS_TABLES"},
			CurrentDatabase:       "udf",
			CurrentUser:           "root",
			CurrentRole:           "writer",
			ConnectionCollation:   "utf8mb4_bin",
		},
		SecurityFrame: &udf.SecurityFrame{
			ContractVersion: udf.SecurityFrameContractVersion,
			Mode:            "INVOKER",
		},
		Tuple: protocol.FencingTuple{
			AccountID: 1, StatementID: "statement", GroupID: "group",
			GroupEpoch: 1, InvocationID: "invocation", LeaseEpoch: 1,
		},
	}
}

func TestValidateInvocationHeaderRejectsUnfrozenContracts(t *testing.T) {
	require.NoError(t, validateInvocationHeader(validInvocation()))

	cases := []struct {
		name   string
		mutate func(*udf.Invocation)
	}{
		{"nil invocation", nil},
		{"language", func(i *udf.Invocation) { i.Language = "PYTHON" }},
		{"empty handler", func(i *udf.Invocation) { i.Handler = "" }},
		{"external handler", func(i *udf.Invocation) { i.Handler = "package:handler" }},
		{"negative length", func(i *udf.Invocation) { i.Length = -1 }},
		{"argument input count", func(i *udf.Invocation) { i.Inputs = nil }},
		{"unsupported mode", func(i *udf.Invocation) { i.Mode = "BATCH" }},
		{"empty callsite", func(i *udf.Invocation) { i.CallsiteID = "" }},
		{"oversized callsite", func(i *udf.Invocation) { i.CallsiteID = strings.Repeat("x", 257) }},
		{"control character callsite", func(i *udf.Invocation) { i.CallsiteID = "bad\nsite" }},
		{"must be allowed to error", func(i *udf.Invocation) { i.MayError = false }},
		{"unsupported security mode", func(i *udf.Invocation) { i.SecurityMode = "DEFINER" }},
		{"leakproof", func(i *udf.Invocation) { i.Leakproof = true }},
		{"missing statement context", func(i *udf.Invocation) { i.StatementContext = nil }},
		{"invalid statement context", func(i *udf.Invocation) { i.StatementContext.TimezoneKind = "UNKNOWN" }},
		{"missing security frame", func(i *udf.Invocation) { i.SecurityFrame = nil }},
		{"changed security principal", func(i *udf.Invocation) { i.SecurityFrame.EffectiveUserID = 8 }},
		{"security mode mismatch", func(i *udf.Invocation) { i.SecurityFrame.Mode = "DEFINER" }},
		{"unsupported null policy", func(i *udf.Invocation) { i.NullPolicy = "STRICT" }},
		{"unsupported ABI", func(i *udf.Invocation) { i.ABIContract = "PYTHON_NATIVE" }},
		{"unsupported adapter", func(i *udf.Invocation) { i.AdapterVersion = "future" }},
		{"unsupported SDK", func(i *udf.Invocation) { i.SDKVersion = "future" }},
		{"unsupported schema", func(i *udf.Invocation) { i.DefinitionSchemaVersion++ }},
		{"malformed artifact digest", func(i *udf.Invocation) { i.ArtifactDigest = "bad" }},
		{"environment digest mismatch", func(i *udf.Invocation) { i.EnvironmentDigest = strings.Repeat("d", 64) }},
		{"malformed definition fingerprint", func(i *udf.Invocation) { i.DefinitionFingerprint = "bad" }},
		{"unsupported argument type", func(i *udf.Invocation) { i.Args[0] = types.T_any.ToType() }},
		{"unsupported return type", func(i *udf.Invocation) { i.ReturnType = types.T_any.ToType() }},
		{"invalid function identity", func(i *udf.Invocation) { i.FunctionRef.FunctionID = 0 }},
		{"fencing account mismatch", func(i *udf.Invocation) { i.Tuple.AccountID++ }},
		{"invalid fencing tuple", func(i *udf.Invocation) { i.Tuple.StatementID = "" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			invocation := validInvocation()
			if tc.mutate == nil {
				require.Error(t, validateInvocationHeader(nil))
				return
			}
			tc.mutate(invocation)
			require.Error(t, validateInvocationHeader(invocation))
		})
	}
}

func TestGatewayStatusSnapshotReportsAdmissionAndWorkerContract(t *testing.T) {
	response, err := json.Marshal(currentTestCapabilities())
	require.NoError(t, err)
	for _, tc := range []struct {
		name         string
		language     string
		config       ClientConfig
		client       *capabilityActionClient
		closeGateway bool
		directConfig bool
		wantReady    bool
		wantClass    string
		wantReason   string
		wantProtocol int32
		wantLease    uint64
	}{
		{
			name:       "unsupported language",
			language:   "sql",
			wantClass:  udf.RuntimeStatusInvalid,
			wantReason: udf.RuntimeStatusReasonInvalidLanguage,
		},
		{
			name:       "disabled",
			language:   udf.LanguagePython,
			wantClass:  udf.RuntimeStatusDisabled,
			wantReason: udf.RuntimeStatusReasonRuntimeDisabled,
		},
		{
			name:     "unisolated execution not admitted",
			language: udf.LanguagePython,
			config: ClientConfig{
				Enabled: true,
			},
			directConfig: true,
			wantClass:    udf.RuntimeStatusNotAllowed,
			wantReason:   udf.RuntimeStatusReasonUnisolatedNotAllowed,
		},
		{
			name:     "worker transport failure",
			language: udf.LanguagePython,
			config: ClientConfig{
				Enabled: true, AllowUnisolated: true, ServerAddress: "127.0.0.1:1", RequestTimeout: time.Second,
			},
			client:     &capabilityActionClient{err: errors.New("connection refused")},
			wantClass:  udf.RuntimeStatusUnavailable,
			wantReason: udf.RuntimeStatusReasonWorkerUnavailable,
		},
		{
			name:     "capability contract mismatch",
			language: udf.LanguagePython,
			config: ClientConfig{
				Enabled: true, AllowUnisolated: true, ServerAddress: "127.0.0.1:1", RequestTimeout: time.Second,
			},
			client: func() *capabilityActionClient {
				mismatch := currentTestCapabilities()
				mismatch.ProtocolVersion++
				body, marshalErr := json.Marshal(mismatch)
				require.NoError(t, marshalErr)
				return &capabilityActionClient{response: body}
			}(),
			wantClass:  udf.RuntimeStatusContractMismatch,
			wantReason: udf.RuntimeStatusReasonCapabilityMismatch,
		},
		{
			name:     "ready worker",
			language: udf.LanguagePython,
			config: ClientConfig{
				Enabled: true, AllowUnisolated: true, ServerAddress: "127.0.0.1:1", RequestTimeout: time.Second,
			},
			client:       &capabilityActionClient{response: response},
			wantReady:    true,
			wantClass:    "",
			wantReason:   udf.RuntimeStatusReasonReady,
			wantProtocol: int32(protocol.Version),
			wantLease:    42,
		},
		{
			name:         "closed gateway",
			language:     udf.LanguagePython,
			config:       ClientConfig{Enabled: true, AllowUnisolated: true, ServerAddress: "127.0.0.1:1"},
			closeGateway: true,
			wantClass:    udf.RuntimeStatusClosed,
			wantReason:   udf.RuntimeStatusReasonRuntimeClosed,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.config
			if cfg.ServerAddress == "" && cfg.Enabled {
				cfg.ServerAddress = "127.0.0.1:1"
			}
			if cfg.Enabled && cfg.AllowUnisolated && cfg.RequestTimeout == 0 {
				cfg.RequestTimeout = time.Second
			}
			var gateway *Gateway
			var err error
			if tc.directConfig {
				// NewGateway correctly rejects this production configuration. A
				// direct value exercises the status fail-closed branch itself.
				gateway = &Gateway{cfg: cfg}
			} else {
				gateway, err = NewGateway(cfg)
				require.NoError(t, err)
			}
			if tc.client != nil {
				gateway.flight = tc.client
			}
			if tc.closeGateway {
				require.NoError(t, gateway.Close())
			} else {
				defer gateway.Close()
			}

			snapshot := gateway.StatusSnapshot(nil, tc.language)
			require.Equal(t, tc.language, snapshot.Language)
			require.Equal(t, tc.wantReady, snapshot.Ready)
			require.Equal(t, tc.wantClass, snapshot.ErrorClass)
			require.Equal(t, tc.wantReason, snapshot.Reason)
			if tc.wantReady {
				require.Equal(t, tc.wantProtocol, snapshot.ProtocolVersion)
				require.Equal(t, tc.wantLease, snapshot.LeaseEpoch)
				require.Equal(t, []string{ModeScalar, ModeVector}, snapshot.Modes)
				require.Equal(t, []string{NullCallHandler, NullReturnNull}, snapshot.NullPolicies)
				require.Equal(t, udf.PythonABIContract, snapshot.ABIContract)
			}
		})
	}
}

func TestReadCapabilitiesRequiresOneStrictCurrentResponse(t *testing.T) {
	validBody, err := json.Marshal(currentTestCapabilities())
	require.NoError(t, err)
	unknownFieldBody := append(append([]byte(nil), validBody[:len(validBody)-1]...), []byte(`,"future_contract":true}`)...)

	for _, tc := range []struct {
		name       string
		actionErr  error
		stream     *scriptedCapabilityActionStream
		wantError  string
		wantResult bool
	}{
		{
			name: "one current response followed by EOF",
			stream: &scriptedCapabilityActionStream{
				results: []*flight.Result{{Body: validBody}},
			},
			wantResult: true,
		},
		{
			name:      "action RPC failure",
			actionErr: errors.New("injected action failure"),
			wantError: "capability handshake",
		},
		{
			name: "first response transport failure",
			stream: &scriptedCapabilityActionStream{
				errors: []error{errors.New("injected response failure")},
			},
			wantError: "capability handshake response",
		},
		{
			name:      "empty response",
			stream:    &scriptedCapabilityActionStream{results: []*flight.Result{nil}},
			wantError: "empty capability handshake response",
		},
		{
			name: "malformed JSON",
			stream: &scriptedCapabilityActionStream{
				results: []*flight.Result{{Body: []byte("{")}},
			},
			wantError: "decode capability handshake",
		},
		{
			name: "unknown contract field",
			stream: &scriptedCapabilityActionStream{
				results: []*flight.Result{{Body: unknownFieldBody}},
			},
			wantError: "decode capability handshake",
		},
		{
			name: "trailing JSON value",
			stream: &scriptedCapabilityActionStream{
				results: []*flight.Result{{Body: append(append([]byte(nil), validBody...), []byte(" {}")...)}},
			},
			wantError: "trailing JSON",
		},
		{
			name: "unsupported current contract",
			stream: func() *scriptedCapabilityActionStream {
				mismatch := currentTestCapabilities()
				mismatch.ProtocolVersion++
				body, marshalErr := json.Marshal(mismatch)
				require.NoError(t, marshalErr)
				return &scriptedCapabilityActionStream{results: []*flight.Result{{Body: body}}}
			}(),
			wantError: "contract does not match",
		},
		{
			name: "multiple responses",
			stream: &scriptedCapabilityActionStream{
				results: []*flight.Result{{Body: validBody}, {Body: validBody}},
			},
			wantError: "multiple results",
		},
		{
			name: "stream failure after response",
			stream: &scriptedCapabilityActionStream{
				results: []*flight.Result{{Body: validBody}},
				errors:  []error{nil, errors.New("injected trailing stream failure")},
			},
			wantError: "capability handshake stream",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &scriptedCapabilityFlightClient{actionErr: tc.actionErr, stream: tc.stream}
			got, err := readCapabilities(context.Background(), client)
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				require.Nil(t, got)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.wantResult, got != nil)
				require.Equal(t, protocol.Version, got.ProtocolVersion)
			}
			require.Len(t, client.actions, 1)
			require.Equal(t, "GetPythonCapabilities", client.actions[0].Type)
			var request capabilityRequest
			require.NoError(t, json.Unmarshal(client.actions[0].Body, &request))
			require.Equal(t, protocol.Version, request.ProtocolVersion)
		})
	}
}

func TestValidateDefinitionRejectsMalformedCurrentContractsBeforeRPC(t *testing.T) {
	valid := func() *udf.RoutineDefinition {
		invocation := validInvocation()
		return &udf.RoutineDefinition{
			Language:                invocation.Language,
			AccountID:               invocation.FunctionRef.AccountID,
			Handler:                 invocation.Handler,
			Source:                  invocation.Source,
			Args:                    append([]types.Type(nil), invocation.Args...),
			ReturnType:              invocation.ReturnType,
			Mode:                    invocation.Mode,
			NullPolicy:              invocation.NullPolicy,
			ABIContract:             invocation.ABIContract,
			AdapterVersion:          invocation.AdapterVersion,
			SDKVersion:              invocation.SDKVersion,
			DefinitionSchemaVersion: invocation.DefinitionSchemaVersion,
			ArtifactDigest:          invocation.ArtifactDigest,
			EnvironmentDigest:       invocation.EnvironmentDigest,
			DefinitionFingerprint:   invocation.DefinitionFingerprint,
		}
	}
	for _, tc := range []struct {
		name   string
		mutate func(*udf.RoutineDefinition)
	}{
		{"unsupported language", func(d *udf.RoutineDefinition) { d.Language = "sql" }},
		{"empty handler", func(d *udf.RoutineDefinition) { d.Handler = "  " }},
		{"external handler", func(d *udf.RoutineDefinition) { d.Handler = "module:handler" }},
		{"unsupported schema", func(d *udf.RoutineDefinition) { d.DefinitionSchemaVersion++ }},
		{"unsupported ABI", func(d *udf.RoutineDefinition) { d.ABIContract = "future" }},
		{"unsupported adapter", func(d *udf.RoutineDefinition) { d.AdapterVersion = "future" }},
		{"unsupported SDK", func(d *udf.RoutineDefinition) { d.SDKVersion = "future" }},
		{"unsupported mode", func(d *udf.RoutineDefinition) { d.Mode = "BATCH" }},
		{"unsupported NULL policy", func(d *udf.RoutineDefinition) { d.NullPolicy = "STRICT" }},
		{"invalid artifact digest", func(d *udf.RoutineDefinition) { d.ArtifactDigest = "invalid" }},
		{"invalid environment digest", func(d *udf.RoutineDefinition) { d.EnvironmentDigest = "invalid" }},
		{"environment digest mismatch", func(d *udf.RoutineDefinition) { d.EnvironmentDigest = strings.Repeat("a", 64) }},
		{"invalid fingerprint", func(d *udf.RoutineDefinition) { d.DefinitionFingerprint = "invalid" }},
		{"unsupported argument descriptor", func(d *udf.RoutineDefinition) { d.Args[0] = types.T_any.ToType() }},
		{"unsupported return descriptor", func(d *udf.RoutineDefinition) { d.ReturnType = types.T_any.ToType() }},
		{"fingerprint does not bind definition", func(d *udf.RoutineDefinition) { d.Mode = ModeVector }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gateway, err := NewGateway(ClientConfig{
				Enabled: true, AllowUnisolated: true, ServerAddress: "127.0.0.1:1", RequestTimeout: time.Second,
			})
			require.NoError(t, err)
			defer gateway.Close()
			definition := valid()
			tc.mutate(definition)
			err = gateway.ValidateDefinition(context.Background(), definition)
			require.Error(t, err)
			require.Contains(t, err.Error(), "UNSUPPORTED_ROUTINE_VERSION")
			gateway.mu.Lock()
			defer gateway.mu.Unlock()
			require.Nil(t, gateway.conn, "invalid definitions must be rejected before opening a Flight connection")
		})
	}
	t.Run("nil definition", func(t *testing.T) {
		gateway, err := NewGateway(ClientConfig{Enabled: true, AllowUnisolated: true, ServerAddress: "127.0.0.1:1"})
		require.NoError(t, err)
		defer gateway.Close()
		require.ErrorContains(t, gateway.ValidateDefinition(context.Background(), nil), "nil Python routine definition")
	})
	t.Run("disabled runtime", func(t *testing.T) {
		gateway, err := NewGateway(ClientConfig{})
		require.NoError(t, err)
		defer gateway.Close()
		require.ErrorContains(t, gateway.ValidateDefinition(context.Background(), valid()), "runtime is disabled")
	})
	t.Run("unisolated opt in is required", func(t *testing.T) {
		gateway := &Gateway{cfg: ClientConfig{Enabled: true, ServerAddress: "127.0.0.1:1"}}
		require.ErrorContains(t, gateway.ValidateDefinition(context.Background(), valid()), "explicit unisolated opt-in")
	})
}

func TestValidateInvocationInputsChecksZeroRowVectorTypes(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	input := vector.NewVec(types.T_int32.ToType())
	defer func() {
		input.Free(mp)
		mpool.DeleteMPool(mp)
	}()

	descriptor, err := NewTypeDescriptor(types.T_int64.ToType())
	require.NoError(t, err)
	err = validateInvocationInputs(&udf.Invocation{
		Length: 0,
		Args:   []types.Type{types.T_int64.ToType()},
		Inputs: []*vector.Vector{input},
	}, []TypeDescriptor{descriptor})
	require.ErrorContains(t, err, "does not match the frozen argument type")
}

func TestValidateInvocationRequiresFrozenContract(t *testing.T) {
	base := validInvocation()
	require.NoError(t, validateInvocation(base))

	cases := []struct {
		name   string
		mutate func(*udf.Invocation)
	}{
		{name: "language", mutate: func(invocation *udf.Invocation) { invocation.Language = "" }},
		{name: "mode", mutate: func(invocation *udf.Invocation) { invocation.Mode = "" }},
		{name: "external handler", mutate: func(invocation *udf.Invocation) { invocation.Handler = "module:add" }},
		{name: "null policy", mutate: func(invocation *udf.Invocation) { invocation.NullPolicy = "" }},
		{name: "ABI", mutate: func(invocation *udf.Invocation) { invocation.ABIContract = "" }},
		{name: "adapter", mutate: func(invocation *udf.Invocation) { invocation.AdapterVersion = "" }},
		{name: "SDK", mutate: func(invocation *udf.Invocation) { invocation.SDKVersion = "" }},
		{name: "definition schema", mutate: func(invocation *udf.Invocation) { invocation.DefinitionSchemaVersion = 0 }},
		{name: "artifact digest", mutate: func(invocation *udf.Invocation) { invocation.ArtifactDigest = "" }},
		{name: "environment digest", mutate: func(invocation *udf.Invocation) { invocation.EnvironmentDigest = "" }},
		{name: "definition fingerprint", mutate: func(invocation *udf.Invocation) { invocation.DefinitionFingerprint = "" }},
		{name: "definition fingerprint contents", mutate: func(invocation *udf.Invocation) {
			invocation.DefinitionFingerprint = strings.Repeat("b", 64)
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			invocation := validInvocation()
			test.mutate(invocation)
			require.Error(t, validateInvocation(invocation))
		})
	}
}

func TestValidateInvocationBoundsResolvedArtifact(t *testing.T) {
	oversized := validInvocation()
	oversized.Source = strings.Repeat("x", int(DefaultMaxArtifactBytes)+1)
	require.ErrorContains(
		t,
		validateInvocation(oversized),
		"Python artifact exceeds",
	)

	invalidUTF8 := validInvocation()
	invalidUTF8.Source = string([]byte{0xff})
	require.ErrorContains(
		t,
		validateInvocation(invalidUTF8),
		"source is not valid UTF-8",
	)
}

func TestGatewayValidatesIdentityBeforeResolvingArtifact(t *testing.T) {
	resolver := &recordingArtifactResolver{source: validInvocation().Source}
	gateway, err := NewGatewayWithArtifactStore(ClientConfig{
		Enabled:         true,
		AllowUnisolated: true,
		ServerAddress:   "127.0.0.1:50051",
	}, resolver)
	require.NoError(t, err)

	invocation := validInvocation()
	invocation.Source = ""
	invocation.FunctionRef.FunctionID = 0
	resultMP := mpool.MustNewZeroNoFixed()
	defer mpool.DeleteMPool(resultMP)
	result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), resultMP)
	defer result.Free()

	err = gateway.Execute(context.Background(), invocation, result, resultMP)
	require.ErrorContains(t, err, "incomplete UDF FunctionRef")
	require.Zero(t, resolver.calls, "a malformed typed plan must not probe an account-scoped artifact")
}

func TestGatewayResolvesArtifactOnlyAfterHeaderValidation(t *testing.T) {
	base := validInvocation()
	resolver := &recordingArtifactResolver{source: base.Source}
	gateway, err := NewGatewayWithArtifactStore(ClientConfig{
		Enabled:         true,
		AllowUnisolated: true,
		ServerAddress:   "127.0.0.1:50051",
	}, resolver)
	require.NoError(t, err)

	invocation := *base
	invocation.Source = ""
	resultMP := mpool.MustNewZeroNoFixed()
	defer mpool.DeleteMPool(resultMP)
	result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), resultMP)
	defer result.Free()

	require.NoError(t, gateway.Execute(context.Background(), &invocation, result, resultMP))
	require.Equal(t, 1, resolver.calls)
	require.Equal(t, uint64(1), resolver.account)
	require.Equal(t, "add", resolver.handler)
	require.Equal(t, base.ArtifactDigest, resolver.digest)
}

func TestValidateInvocationRejectsMismatchedInputBacking(t *testing.T) {
	cases := []struct {
		name string
		make func(*mpool.MPool) *vector.Vector
		want string
	}{
		{
			name: "nil backing",
			make: func(*mpool.MPool) *vector.Vector { return nil },
			want: "has no backing vector",
		},
		{
			name: "wrong type",
			make: func(mp *mpool.MPool) *vector.Vector {
				input := vector.NewVec(types.T_float64.ToType())
				require.NoError(t, vector.AppendFixed(input, float64(1), false, mp))
				return input
			},
			want: "does not match",
		},
		{
			name: "short flat vector",
			make: func(mp *mpool.MPool) *vector.Vector {
				input := vector.NewVec(types.T_int64.ToType())
				require.NoError(t, vector.AppendFixed(input, int64(1), false, mp))
				return input
			},
			want: "expected at least 2",
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() { mpool.DeleteMPool(mp) })
			input := test.make(mp)
			if input != nil {
				t.Cleanup(func() { input.Free(mp) })
			}
			invocation := validInvocation()
			invocation.Length = 2
			invocation.Inputs = []*vector.Vector{input}
			require.ErrorContains(t, validateInvocation(invocation), test.want)
		})
	}
}

func TestValidateInvocationAcceptsConstInputWithFrozenType(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	input, err := vector.NewConstFixed(types.T_int64.ToType(), int64(7), 2, mp)
	require.NoError(t, err)
	t.Cleanup(func() { input.Free(mp) })

	invocation := validInvocation()
	invocation.Length = 2
	invocation.Inputs = []*vector.Vector{input}
	require.NoError(t, validateInvocation(invocation))
}

func TestValidateInvocationBudgetRejectsBeforeOpen(t *testing.T) {
	require.NoError(t, validateInvocationBudget(executionBudget{
		rows: 2, maxRows: 2, returnType: types.T_int64.ToType(), maxResultBytes: 32,
	}))
	require.ErrorContains(t, validateInvocationBudget(executionBudget{
		rows: 3, maxRows: 2, returnType: types.T_int64.ToType(), maxResultBytes: 32,
	}), "invocation has 3 rows")
	// Two int64 values plus the validity bitmap need more than sixteen bytes;
	// this is rejected before OpenInvocation rather than after user code runs.
	require.ErrorContains(t, validateInvocationBudget(executionBudget{
		rows: 2, maxRows: 2, returnType: types.T_int64.ToType(), maxResultBytes: 16,
	}), "result reservation")
}

func TestGatewayFeatureGateRejectsDisabledRuntime(t *testing.T) {
	gateway, err := NewGateway(ClientConfig{})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.ErrorContains(t, gateway.CheckLanguageReady(ctx, udf.LanguagePython), "runtime is disabled")
	require.ErrorContains(t, gateway.Execute(ctx, validInvocation(), nil, nil), "runtime is disabled")
}

func TestValidateCapabilitiesRequiresWorkerInstanceLease(t *testing.T) {
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
	require.ErrorContains(t, validateCapabilities(response), "contract does not match")
	response.LeaseEpoch = 17
	require.NoError(t, validateCapabilities(response))
	response.MaxOwnerHandlerProcesses++
	require.ErrorContains(t, validateCapabilities(response), "contract does not match")
}

func TestValidateActionAckCorrelatesTheRequestedFence(t *testing.T) {
	request := protocol.Control{
		Kind:        "AcknowledgeResults",
		Tuple:       validInvocation().Tuple,
		AckSequence: 3,
	}
	ack := protocol.Control{Kind: "Ack", Tuple: request.Tuple, Status: statusOK, AckSequence: 3}
	require.NoError(t, validateActionAck(request.Kind, request, ack))
	ack.AckSequence = 2
	require.ErrorContains(t, validateActionAck(request.Kind, request, ack), "ACK sequence")

	finish := protocol.Control{Kind: "AcknowledgeFinish", Tuple: request.Tuple, FinishID: "finish-1"}
	finishAck := protocol.Control{Kind: "Ack", Tuple: request.Tuple, Status: statusOK, FinishID: "finish-1"}
	require.NoError(t, validateActionAck(finish.Kind, finish, finishAck))
	finishAck.FinishID = "finish-2"
	require.ErrorContains(t, validateActionAck(finish.Kind, finish, finishAck), "finish ID")
}

func TestAcknowledgeFinishRetriesTheSameIdempotentRequest(t *testing.T) {
	request := protocol.Control{
		Kind:     "AcknowledgeFinish",
		Tuple:    validInvocation().Tuple,
		FinishID: "finish-1",
	}
	client := &finishAckFlightClient{}
	gateway := &Gateway{cfg: ClientConfig{RequestTimeout: time.Second}}

	require.NoError(t, gateway.acknowledgeFinish(context.Background(), client, request.Kind, request))
	require.Equal(t, 2, client.calls)
	require.Len(t, client.actions, 2)
	require.Equal(t, client.actions[0].Type, client.actions[1].Type)
	require.Equal(t, client.actions[0].Body, client.actions[1].Body)
}

func TestGatewayCloseIsTerminal(t *testing.T) {
	gateway, err := NewGateway(ClientConfig{})
	require.NoError(t, err)
	require.NoError(t, gateway.Close())
	require.ErrorIs(t, gateway.connect(), errGatewayClosed)
	require.NoError(t, gateway.Close())
}

func TestGatewayFinishRejectsLateHalfStreamControls(t *testing.T) {
	tuple := validInvocation().Tuple
	for _, kind := range []string{"InputConsumed", "ResultSchema"} {
		t.Run(kind, func(t *testing.T) {
			sequence := protocol.Sequence{}
			require.NoError(t, sequence.AcceptInput(1))
			require.NoError(t, sequence.EndInput(1))
			require.NoError(t, sequence.AcceptResult(1))
			require.NoError(t, sequence.AcknowledgeResults(1))
			stream := &gatewayResultStream{results: []*flight.FlightData{
				gatewayControl(t, kind, tuple, func(control *protocol.Control) {
					if kind == "InputConsumed" {
						control.Sequence = 1
						control.ReleasedBytes = 1
						control.ReleasedBatches = 1
					}
				}),
			}}
			gateway := &Gateway{cfg: ClientConfig{RequestTimeout: time.Second}}
			err := gateway.receiveFinish(context.Background(), nil, stream, tuple, 1, 1, &sequence)
			require.ErrorContains(t, err, "arrived after result stream was drained")
		})
	}
}

func TestGatewayFinishWaitsForExchangeEOF(t *testing.T) {
	tuple := validInvocation().Tuple
	sequence := protocol.Sequence{}
	require.NoError(t, sequence.AcceptInput(1))
	require.NoError(t, sequence.EndInput(1))
	require.NoError(t, sequence.AcceptResult(1))
	require.NoError(t, sequence.AcknowledgeResults(1))
	finish := gatewayControl(t, "Finish", tuple, func(value *protocol.Control) {
		value.Status = statusOK
		value.FinishID = "finish-1"
		value.LastSequence = 1
		value.LastResultSequence = 1
	})
	release := make(chan struct{})
	waiting := make(chan struct{})
	stream := &gatedGatewayResultStream{
		results: []*flight.FlightData{finish},
		release: release,
		waiting: waiting,
	}
	gateway := &Gateway{cfg: ClientConfig{RequestTimeout: time.Second}}
	done := make(chan error, 1)
	go func() {
		done <- gateway.receiveFinish(context.Background(), &finishAckFlightClient{}, stream, tuple, 1, 1, &sequence)
	}()

	<-waiting
	select {
	case err := <-done:
		require.Failf(t, "Finish returned before the exchange reached EOF", "%v", err)
	default:
	}
	close(release)
	require.NoError(t, <-done)
}

func TestGatewayRejectsArrowBodyOnControlFrame(t *testing.T) {
	tuple := validInvocation().Tuple
	sequence := protocol.Sequence{}
	require.NoError(t, sequence.AcceptInput(1))
	require.NoError(t, sequence.EndInput(1))
	require.NoError(t, sequence.AcceptResult(1))
	require.NoError(t, sequence.AcknowledgeResults(1))
	control := gatewayControl(t, "Finish", tuple, func(value *protocol.Control) {
		value.Status = statusOK
		value.FinishID = "finish-1"
		value.LastSequence = 1
		value.LastResultSequence = 1
	})
	control.DataBody = []byte("hidden Arrow payload")
	gateway := &Gateway{cfg: ClientConfig{RequestTimeout: time.Second}}
	err := gateway.receiveFinish(
		context.Background(), nil,
		&gatewayResultStream{results: []*flight.FlightData{control}},
		tuple, 1, 1, &sequence,
	)
	require.ErrorContains(t, err, "unexpected Arrow data")
}

func TestReceiveResultBatchRejectsMalformedProtocolProgress(t *testing.T) {
	tuple := validInvocation().Tuple
	resultBatch := func(controlTuple protocol.FencingTuple) *flight.FlightData {
		return &flight.FlightData{
			DataHeader: []byte("record"),
			AppMetadata: func() []byte {
				encoded, err := protocol.MarshalControl(protocol.Control{
					Kind: "ResultBatch", Tuple: controlTuple, Sequence: 1,
				})
				require.NoError(t, err)
				return encoded
			}(),
		}
	}
	inputConsumed := func(sequence uint64, releasedBytes int64, releasedBatches uint64) *flight.FlightData {
		return gatewayControl(t, "InputConsumed", tuple, func(control *protocol.Control) {
			control.Sequence = sequence
			control.ReleasedBytes = releasedBytes
			control.ReleasedBatches = releasedBatches
		})
	}
	for _, tc := range []struct {
		name          string
		results       []*flight.FlightData
		recvErrors    []error
		initialSchema bool
		schemaReady   bool
		want          string
	}{
		{name: "EOF before result", want: "ended before result sequence"},
		{name: "receive failure", recvErrors: []error{errors.New("injected receive failure")}, want: "receive result"},
		{name: "nil FlightData", results: []*flight.FlightData{nil}, want: "empty Flight result"},
		{name: "empty FlightData", results: []*flight.FlightData{{}}, want: "empty Flight result"},
		{
			name: "control frame with Arrow body",
			results: []*flight.FlightData{func() *flight.FlightData {
				data := inputConsumed(1, 1, 1)
				data.DataBody = []byte("unexpected")
				return data
			}()},
			want: "control frame contains an Arrow body",
		},
		{name: "invalid control metadata", results: []*flight.FlightData{{AppMetadata: []byte("not-json")}}, want: "invalid character"},
		{
			name: "changed tuple",
			results: []*flight.FlightData{gatewayControl(t, "InputConsumed", protocol.FencingTuple{
				AccountID: tuple.AccountID + 1, StatementID: tuple.StatementID, GroupID: tuple.GroupID,
				GroupEpoch: tuple.GroupEpoch, InvocationID: tuple.InvocationID, LeaseEpoch: tuple.LeaseEpoch,
			}, func(control *protocol.Control) {
				control.Sequence = 1
				control.ReleasedBytes = 1
				control.ReleasedBatches = 1
			})},
			want: "result tuple changed",
		},
		{name: "result schema control before schema", results: []*flight.FlightData{gatewayControl(t, "ResultSchema", tuple)}, want: "invalid result schema metadata"},
		{
			name:    "input sequence mismatch",
			results: []*flight.FlightData{inputConsumed(2, 1, 1)},
			want:    "was consumed while waiting for result",
		},
		{
			name:    "invalid released credits",
			results: []*flight.FlightData{inputConsumed(1, 2048, 1)},
			want:    "released invalid bytes/batches",
		},
		{name: "worker error control", results: []*flight.FlightData{gatewayControl(t, "Error", tuple, func(control *protocol.Control) {
			control.Status = "ERROR"
			control.Reason = "handler failed"
		})}, want: "worker error: handler failed"},
		{name: "Finish before result", results: []*flight.FlightData{gatewayControl(t, "Finish", tuple, func(control *protocol.Control) {
			control.FinishID = "finish-1"
			control.Status = statusOK
			control.LastSequence = 1
			control.LastResultSequence = 0
		})}, want: "Finish arrived before result"},
		{name: "unknown control", results: []*flight.FlightData{{AppMetadata: []byte(`{"version":1,"kind":"Unknown","tuple":{"statement_id":"stmt","group_id":"group","group_epoch":1,"invocation_id":"invocation","lease_epoch":1}}`)}}, want: "unsupported control kind"},
		{
			name:    "schema frame contains a body",
			results: []*flight.FlightData{{DataHeader: []byte("schema"), DataBody: []byte("body")}},
			want:    "result schema contains a body",
		},
		{
			name: "schema metadata has wrong tuple",
			results: []*flight.FlightData{func() *flight.FlightData {
				data := gatewayControl(t, "ResultSchema", tuple)
				data.DataHeader = []byte("schema")
				otherTuple := tuple
				otherTuple.AccountID++
				data.AppMetadata, _ = protocol.MarshalControl(protocol.Control{Kind: "ResultSchema", Tuple: otherTuple})
				return data
			}()},
			want: "invalid result schema metadata",
		},
		{
			name: "record omitted schema metadata",
			results: []*flight.FlightData{
				{DataHeader: []byte("schema")},
				resultBatch(tuple),
			},
			want: "result schema metadata was not received",
		},
		{
			name:          "result before input consumed",
			initialSchema: true,
			schemaReady:   true,
			results:       []*flight.FlightData{resultBatch(tuple)},
			want:          "arrived before InputConsumed",
		},
		{
			name: "duplicate input consumed",
			results: []*flight.FlightData{
				inputConsumed(1, 1, 1),
				inputConsumed(1, 1, 1),
			},
			want: "was consumed more than once",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZeroNoFixed()
			defer mpool.DeleteMPool(mp)
			result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), mp)
			defer result.Free()
			returnDescriptor, err := NewTypeDescriptor(types.T_int64.ToType())
			require.NoError(t, err)
			sequence := protocol.Sequence{}
			require.NoError(t, sequence.AcceptInput(1))
			var schemaFrame *ArrowFrame
			if tc.initialSchema {
				schemaFrame = &ArrowFrame{Header: []byte("schema")}
			}
			schemaReady := tc.schemaReady
			gateway := &Gateway{cfg: ClientConfig{MaxBatchBytes: 1024}}
			_, err = gateway.receiveResultBatch(
				context.Background(), nil,
				&scriptedGatewayResultStream{results: tc.results, errors: tc.recvErrors},
				tuple, 1, 1, &schemaFrame, &schemaReady, returnDescriptor, result, mp, &sequence,
			)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestReceiveResultBatchValidatesArrowPayloadAndResultAck(t *testing.T) {
	type encodedResult struct {
		frames     []ArrowFrame
		descriptor TypeDescriptor
	}
	encodeResult := func(t *testing.T, fieldName string) encodedResult {
		t.Helper()
		descriptor, err := NewTypeDescriptor(types.T_int64.ToType())
		require.NoError(t, err)
		field, err := descriptor.Field(fieldName)
		require.NoError(t, err)
		builder := array.NewInt64Builder(memory.NewGoAllocator())
		builder.AppendValues([]int64{17}, nil)
		values := builder.NewInt64Array()
		defer values.Release()
		record := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{field}, nil), []arrow.Array{values}, 1)
		defer record.Release()
		frames, err := EncodeRecordBatch(record, DefaultMaxBatchBytes)
		require.NoError(t, err)
		return encodedResult{frames: frames, descriptor: descriptor}
	}

	tuple := validInvocation().Tuple
	encoded := encodeResult(t, "result")
	wrongName := encodeResult(t, "other")
	validAck, err := protocol.MarshalControl(protocol.Control{
		Kind: "Ack", Tuple: tuple, AckSequence: 1, Status: statusOK,
	})
	require.NoError(t, err)
	wrongAck, err := protocol.MarshalControl(protocol.Control{
		Kind: "Ack", Tuple: tuple, AckSequence: 2, Status: statusOK,
	})
	require.NoError(t, err)
	resultMetadata, err := protocol.MarshalControl(protocol.Control{
		Kind: "ResultBatch", Tuple: tuple, Sequence: 1,
	})
	require.NoError(t, err)
	schemaMetadata, err := protocol.MarshalControl(protocol.Control{
		Kind: "ResultSchema", Tuple: tuple,
	})
	require.NoError(t, err)
	inputConsumedMetadata, err := protocol.MarshalControl(protocol.Control{
		Kind: "InputConsumed", Tuple: tuple, Sequence: 1, ReleasedBytes: 1, ReleasedBatches: 1,
	})
	require.NoError(t, err)

	for _, tc := range []struct {
		name              string
		frames            []ArrowFrame
		descriptor        TypeDescriptor
		expectedRows      int64
		maxBatchBytes     int64
		maxResultBytes    int64
		resultMetadata    []byte
		ackResponse       []byte
		ackError          error
		resultAlreadySeen bool
		wantError         string
	}{
		{name: "valid one-row result is appended and acknowledged", frames: encoded.frames, descriptor: encoded.descriptor, expectedRows: 1, maxBatchBytes: DefaultMaxBatchBytes, resultMetadata: resultMetadata, ackResponse: validAck},
		{name: "malformed result metadata", frames: encoded.frames, descriptor: encoded.descriptor, expectedRows: 1, maxBatchBytes: DefaultMaxBatchBytes, resultMetadata: []byte("not-json"), wantError: "invalid character"},
		{name: "result sequence does not match input", frames: encoded.frames, descriptor: encoded.descriptor, expectedRows: 1, maxBatchBytes: DefaultMaxBatchBytes, resultMetadata: func() []byte {
			metadata, marshalErr := protocol.MarshalControl(protocol.Control{Kind: "ResultBatch", Tuple: tuple, Sequence: 2})
			require.NoError(t, marshalErr)
			return metadata
		}(), wantError: "unexpected result sequence"},
		{name: "duplicate result sequence", frames: encoded.frames, descriptor: encoded.descriptor, expectedRows: 1, maxBatchBytes: DefaultMaxBatchBytes, resultMetadata: resultMetadata, resultAlreadySeen: true, wantError: "sequence violation"},
		{name: "corrupt Arrow record body", frames: func() []ArrowFrame {
			frames := append([]ArrowFrame(nil), encoded.frames...)
			frames[1].Body = []byte("not an Arrow body")
			return frames
		}(), descriptor: encoded.descriptor, expectedRows: 1, maxBatchBytes: DefaultMaxBatchBytes, resultMetadata: resultMetadata, wantError: "decode result"},
		{name: "unexpected row count", frames: encoded.frames, descriptor: encoded.descriptor, expectedRows: 2, maxBatchBytes: DefaultMaxBatchBytes, resultMetadata: resultMetadata, wantError: "result batch row count"},
		{name: "unexpected field name", frames: wrongName.frames, descriptor: wrongName.descriptor, expectedRows: 1, maxBatchBytes: DefaultMaxBatchBytes, resultMetadata: resultMetadata, wantError: "field has unexpected name"},
		{name: "descriptor mismatch", frames: encoded.frames, descriptor: func() TypeDescriptor {
			descriptor, descriptorErr := NewTypeDescriptor(types.T_int32.ToType())
			require.NoError(t, descriptorErr)
			return descriptor
		}(), expectedRows: 1, maxBatchBytes: DefaultMaxBatchBytes, resultMetadata: resultMetadata, wantError: "Arrow type int64 does not match int32"},
		{name: "complete IPC payload exceeds limit", frames: encoded.frames, descriptor: encoded.descriptor, expectedRows: 1, maxBatchBytes: 1, resultMetadata: resultMetadata, wantError: "exceeds"},
		{name: "materialized result exceeds invocation budget", frames: encoded.frames, descriptor: encoded.descriptor, expectedRows: 1, maxBatchBytes: DefaultMaxBatchBytes, maxResultBytes: 1, resultMetadata: resultMetadata, wantError: "invocation result exceeds"},
		{name: "result ACK response lost", frames: encoded.frames, descriptor: encoded.descriptor, expectedRows: 1, maxBatchBytes: DefaultMaxBatchBytes, resultMetadata: resultMetadata, ackError: errors.New("injected ACK response loss"), wantError: "injected ACK response loss"},
		{name: "result ACK sequence mismatch", frames: encoded.frames, descriptor: encoded.descriptor, expectedRows: 1, maxBatchBytes: DefaultMaxBatchBytes, resultMetadata: resultMetadata, ackResponse: wrongAck, wantError: "ACK sequence 2, expected 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZeroNoFixed()
			defer mpool.DeleteMPool(mp)
			result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), mp)
			defer result.Free()
			require.NoError(t, result.PreExtendAndReset(int(tc.expectedRows)))
			sequence := protocol.Sequence{}
			require.NoError(t, sequence.AcceptInput(1))
			require.NoError(t, sequence.EndInput(1))
			if tc.resultAlreadySeen {
				require.NoError(t, sequence.AcceptResult(1))
			}
			schema := &flight.FlightData{DataHeader: tc.frames[0].Header, AppMetadata: schemaMetadata}
			consumed := &flight.FlightData{AppMetadata: inputConsumedMetadata}
			record := &flight.FlightData{
				DataHeader: tc.frames[1].Header, DataBody: tc.frames[1].Body,
				AppMetadata: tc.resultMetadata,
			}
			stream := &gatewayResultStream{results: []*flight.FlightData{schema, consumed, record}}
			client := &resultAckFlightClient{response: tc.ackResponse, err: tc.ackError}
			gateway := &Gateway{cfg: ClientConfig{
				MaxBatchBytes: tc.maxBatchBytes, MaxInvocationResultBytes: tc.maxResultBytes,
			}}
			schemaFrame := (*ArrowFrame)(nil)
			schemaReady := false
			rows, callErr := gateway.receiveResultBatch(
				context.Background(), client, stream, tuple, 1, tc.expectedRows,
				&schemaFrame, &schemaReady, tc.descriptor, result, mp, &sequence,
			)
			if tc.wantError != "" {
				require.ErrorContains(t, callErr, tc.wantError)
				return
			}
			require.NoError(t, callErr)
			require.Equal(t, 1, rows)
			require.Equal(t, []int64{17}, vector.MustFixedColNoTypeCheck[int64](result.GetResultVector()))
			lastInput, lastResult, ackedResult, inputEnded := sequence.State()
			require.Equal(t, uint64(1), lastInput)
			require.Equal(t, uint64(1), lastResult)
			require.Equal(t, uint64(1), ackedResult)
			require.True(t, inputEnded)
		})
	}
}

func TestFreezeResultFramesFreezesBothPartsAndChargesTotal(t *testing.T) {
	header := []byte("record-header")
	body := []byte("record-body")
	headerSnapshot, bodySnapshot, err := freezeResultFrames(header, body, int64(len(header)+len(body)))
	require.NoError(t, err)
	require.NoError(t, headerSnapshot.Validate(len(header), headerSnapshot.Digest()))
	require.NoError(t, bodySnapshot.Validate(len(body), bodySnapshot.Digest()))

	header[0] = 'X'
	body[0] = 'Y'
	require.Equal(t, "record-header", string(headerSnapshot.TrustedBytes()))
	require.Equal(t, "record-body", string(bodySnapshot.TrustedBytes()))

	_, _, err = freezeResultFrames(header, body, int64(len(header)+len(body)-1))
	require.ErrorContains(t, err, "exceeds")
}

func TestChooseInputBatchBoundsEncodingAttemptsAndPropagatesErrors(t *testing.T) {
	frame := ArrowFrame{Header: []byte("schema"), Body: []byte("record")}
	t.Run("full candidate avoids a search when it fits", func(t *testing.T) {
		var attempts []int64
		got, err := chooseInputBatchWithFullCandidate(8, 64, func(rows int64) ([]ArrowFrame, error) {
			attempts = append(attempts, rows)
			return []ArrowFrame{frame}, nil
		}, true)
		require.NoError(t, err)
		require.Equal(t, int64(8), got.Rows)
		require.Equal(t, []ArrowFrame{frame}, got.Frames)
		require.Equal(t, []int64{8}, attempts)
	})

	t.Run("size limit uses bounded exponential and binary search", func(t *testing.T) {
		var attempts []int64
		got, err := chooseInputBatchWithFullCandidate(10, 64, func(rows int64) ([]ArrowFrame, error) {
			attempts = append(attempts, rows)
			if rows > 5 {
				return nil, errArrowBatchTooLarge
			}
			return []ArrowFrame{frame}, nil
		}, true)
		require.NoError(t, err)
		require.Equal(t, int64(5), got.Rows)
		require.Equal(t, []int64{10, 1, 2, 4, 8, 6, 5}, attempts)
	})

	t.Run("non-size errors do not trigger retries", func(t *testing.T) {
		wantErr := errors.New("injected Arrow encoder failure")
		attempts := 0
		_, err := chooseInputBatchWithFullCandidate(4, 64, func(int64) ([]ArrowFrame, error) {
			attempts++
			return nil, wantErr
		}, true)
		require.ErrorIs(t, err, wantErr)
		require.Equal(t, 1, attempts)
	})

	t.Run("one row larger than the byte limit is rejected", func(t *testing.T) {
		got, err := chooseInputBatch(4, 1, func(int64) ([]ArrowFrame, error) {
			return nil, errArrowBatchTooLarge
		})
		require.ErrorIs(t, err, errArrowBatchTooLarge)
		require.Zero(t, got.Rows)
	})
}
