// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package python

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
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
