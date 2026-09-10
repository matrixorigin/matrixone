// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package python

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/udf/protocol"
	"github.com/stretchr/testify/require"
)

func validInvocation() *udf.Invocation {
	return &udf.Invocation{
		Language:       udf.LanguagePython,
		Handler:        "add",
		Source:         "def add(ctx, value): return value",
		Args:           []types.Type{types.T_int64.ToType()},
		ReturnType:     types.T_int64.ToType(),
		Length:         0,
		Inputs:         []*vector.Vector{nil},
		Mode:           ModeScalar,
		NullPolicy:     NullCallHandler,
		ABIContract:    udf.PythonABIContract,
		AdapterVersion: udf.PythonAdapterVersion,
		SDKVersion:     udf.PythonSDKVersion,
		Tuple: protocol.FencingTuple{
			AccountID: 1, StatementID: "statement", GroupID: "group",
			GroupEpoch: 1, InvocationID: "invocation", LeaseEpoch: 1,
		},
	}
}

func TestValidateInvocationRequiresFrozenContract(t *testing.T) {
	base := validInvocation()
	require.NoError(t, validateInvocation(base))

	cases := []struct {
		name   string
		mutate func(*udf.Invocation)
	}{
		{name: "mode", mutate: func(invocation *udf.Invocation) { invocation.Mode = "" }},
		{name: "null policy", mutate: func(invocation *udf.Invocation) { invocation.NullPolicy = "" }},
		{name: "ABI", mutate: func(invocation *udf.Invocation) { invocation.ABIContract = "" }},
		{name: "adapter", mutate: func(invocation *udf.Invocation) { invocation.AdapterVersion = "" }},
		{name: "SDK", mutate: func(invocation *udf.Invocation) { invocation.SDKVersion = "" }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			invocation := validInvocation()
			test.mutate(invocation)
			require.Error(t, validateInvocation(invocation))
		})
	}
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

func TestGatewayCloseIsTerminal(t *testing.T) {
	gateway, err := NewGateway(ClientConfig{})
	require.NoError(t, err)
	require.NoError(t, gateway.Close())
	require.ErrorIs(t, gateway.connect(), errGatewayClosed)
	require.NoError(t, gateway.Close())
}
