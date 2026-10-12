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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
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
	// Functional-index v107, migration-regexp v108 and Unicode metadata v109
	// and MEDIUMINT v110 do not implement CRC32 overload 1.
	for _, version := range []int64{defines.MORPCVersion100, defines.MORPCVersion101, defines.MORPCVersion106, defines.MORPCVersion107, defines.MORPCVersion108, defines.MORPCVersion109, defines.MORPCVersion110} {
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

	client.version = defines.MORPCVersion111
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
	client.version = defines.MORPCVersion109
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
	for _, version := range []int64{defines.MORPCVersion109, defines.MORPCVersion110} {
		for _, kind := range []plan2.ExecType{plan2.ExecTypeTP, plan2.ExecTypeAP_ONECN, plan2.ExecTypeAP_MULTICN} {
			c.execType = kind
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
			require.ErrorContains(t, c.constrainRemoteExpressionWorkers(qry), "protocol version 111")
			require.Equal(t, kind, c.execType)
		}
	}
	require.Zero(t, client.calls)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion111)
	c.execType = plan2.ExecTypeAP_ONECN
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Zero(t, client.calls)
}

func TestCRC32JSONProtocolIncludesMergedMediumIntBounds(t *testing.T) {
	mediumInt := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_int32), Width: 24},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.CAST_ASSIGN, 0), ObjName: "cast_assign"},
			Args: []*planpb.Expr{
				{Typ: planpb.Type{Id: int32(types.T_int32)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
				{Typ: planpb.Type{Id: int32(types.T_int32), Width: 24}, Expr: &planpb.Expr_T{T: &planpb.TargetType{}}},
			},
		}},
	}
	// Preserve actual main's widened-result/width-24 assignment-target contract.
	mediumInt.Typ.Width = 32
	crc32JSON := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_uint64)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.CRC32, function.CRC32JSONTextOverload), ObjName: "crc32"},
			Args: []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_json)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}}},
		}},
	}
	for _, tc := range []struct {
		name      string
		exprs     []*planpb.Expr
		mediumInt bool
		crc32JSON bool
		version   int64
	}{
		{"mediumint_only", []*planpb.Expr{mediumInt}, true, false, defines.MORPCVersion110},
		{"crc32_only", []*planpb.Expr{crc32JSON}, false, true, defines.MORPCVersion111},
		{"mediumint_then_crc32", []*planpb.Expr{mediumInt, crc32JSON}, true, true, defines.MORPCVersion111},
		{"crc32_then_mediumint", []*planpb.Expr{crc32JSON, mediumInt}, true, true, defines.MORPCVersion111},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: tc.exprs}}, Steps: []int32{0}}
			features, err := planpb.RequiredRemoteExpressionFeatures(qry)
			require.NoError(t, err)
			require.Equal(t, tc.mediumInt, features.MediumIntAssignmentBounds)
			require.Equal(t, tc.crc32JSON, features.CRC32JSONTextBytes)
			require.Equal(t, tc.version, remoteExpressionProtocolVersion(features))
			owner := &planpb.TableDef{}
			for _, expr := range tc.exprs {
				owner.Cols = append(owner.Cols, &planpb.ColDef{GeneratedCol: &planpb.GeneratedCol{Expr: expr}})
			}
			persisted, err := plan2.RequiredPersistedExpressionProtocolVersion(owner)
			require.NoError(t, err)
			require.Equal(t, tc.version, persisted)

			op := projection.NewArgument()
			defer op.Release()
			op.ProjectList = tc.exprs
			scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
			client.version = tc.version - 1
			c.execType = plan2.ExecTypeAP_MULTICN
			c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
			require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
			require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
			_, err = encodeRemoteScope(scope, c.proc)
			require.ErrorContains(t, err, "remote destination")
			require.ErrorContains(t, err, fmt.Sprintf("MORPC version %d", tc.version))

			client.version = tc.version
			c.execType = plan2.ExecTypeAP_MULTICN
			c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
			require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
			require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
			encoded, err := encodeRemoteScope(scope, c.proc)
			require.NoError(t, err)
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, tc.version-1)
			decoded, err := decodeScope(encoded, c.proc, true, nil)
			require.ErrorContains(t, err, fmt.Sprintf("protocol version %d", tc.version))
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
			require.Nil(t, decoded)
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, tc.version)
			decoded, err = decodeScope(encoded, c.proc, true, nil)
			require.NoError(t, err)
			decoded.release()
			// Analyze the decoded expression owner as well as the source query.
			p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: tc.exprs}}}
			wire, err := p.Marshal()
			require.NoError(t, err)
			var restored pipeline.Pipeline
			require.NoError(t, restored.Unmarshal(wire))
			restoredFeatures, err := planpb.RequiredRemoteExpressionFeatures(&restored)
			require.NoError(t, err)
			require.Equal(t, features, restoredFeatures)
			require.Equal(t, client.calls, client.releases)
		})
	}
}
