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

package compile

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func TestCRC32JSONDestinationProtocolValidation(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	expr := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_uint64)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{
				Obj:     function.EncodeOverloadID(function.CRC32, function.CRC32JSONTextOverload),
				ObjName: "crc32",
			},
		}},
	}
	op := projection.NewArgument()
	defer op.Release()
	op.ProjectList = []*planpb.Expr{expr}
	scope := &Scope{
		Magic:    Remote,
		Proc:     c.proc,
		NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"},
		RootOp:   op,
	}

	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}}
	// Main through v106 implements other contracts but not CRC32 overload 1.
	for _, version := range []int64{defines.MORPCVersion100, defines.MORPCVersion101, defines.MORPCVersion106} {
		client.version = version
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
		calls, releases := client.calls, client.releases
		require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
		require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
		require.Equal(t, 1, client.calls-calls, "placement shares one probe for all expression floors")
		require.Equal(t, 1, client.releases-releases)
		calls = client.calls
		_, err := encodeRemoteScope(scope, c.proc)
		require.ErrorContains(t, err, "remote destination")
		require.Equal(t, 1, client.calls-calls)
	}

	client.version = defines.MORPCVersion107
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	calls := client.calls
	data, err := encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.NotEmpty(t, data)
	require.Equal(t, 1, client.calls-calls, "CRC32 and its UINT64 result share the send probe")
	// A downgrade after successful planning must fail at the send boundary.
	client.version = defines.MORPCVersion106
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination")
	// The old identity remains dispatchable to an actual old peer.
	expr.GetF().Func.Obj = function.EncodeOverloadID(function.CRC32, function.CRC32LegacyOverload)
	data, err = encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	decoded, err := decodeScope(data, c.proc, true, nil)
	require.NoError(t, err)
	decoded.release()
	expr.GetF().Func.Obj = function.EncodeOverloadID(function.CRC32, function.CRC32JSONTextOverload)
	require.Equal(t, client.calls, client.releases)
	client.customResponse = true
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination")
	client.sendErr = errors.New("probe failed")
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination")
	ctx, cancel := context.WithCancel(c.proc.Ctx)
	cancel()
	c.proc.Ctx = ctx
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorIs(t, err, context.Canceled)
}

func TestCRC32JSONPlacementLocalFloor(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	expr := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{
		Obj: function.EncodeOverloadID(function.CRC32, function.CRC32JSONTextOverload),
	}}}}
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}}
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	for _, kind := range []plan2.ExecType{plan2.ExecTypeTP, plan2.ExecTypeAP_ONECN, plan2.ExecTypeAP_MULTICN} {
		c.execType = kind
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion106)
		require.ErrorContains(t, c.constrainRemoteExpressionWorkers(qry), "protocol version 107")
		require.Equal(t, kind, c.execType)
	}
	require.Zero(t, client.calls)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion107)
	c.execType = plan2.ExecTypeAP_ONECN
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Zero(t, client.calls)
}
