// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"fmt"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func integerProtocolExpr(id int32) *planpb.Expr {
	source, target := types.T_float64, types.T_int64
	if id == function.TextIntegerBitsCastOverload {
		source, target = types.T_varchar, types.T_uint64
	}
	if id == function.TemporalIntegerArgumentCastOverload {
		source = types.T_time
	}
	typ := planpb.Type{Id: int32(target)}
	return &planpb.Expr{Typ: typ, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.CAST, id), ObjName: "cast"},
		Args: []*planpb.Expr{
			{Typ: planpb.Type{Id: int32(source)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
			{Typ: typ, Expr: &planpb.Expr_T{T: &planpb.TargetType{}}},
		},
	}}}
}

func TestIntegerArgumentProtocolBoundaries(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	require.Equal(t, int32(21), int32(function.CAST), "protobuf capability identity must track the registry")
	for id := int32(0); id <= function.TemporalIntegerArgumentCastOverload; id++ {
		t.Run(fmt.Sprint(id), func(t *testing.T) {
			expr := integerProtocolExpr(id)
			p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
			features, err := planpb.RequiredRemoteExpressionFeatures(p)
			require.NoError(t, err)
			require.Equal(t, id >= function.IntegerArgumentCastOverload, features.IntegerParameterCoercion)
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion84)
			err = validateRemoteExpressionPipelineProtocol(c.proc, p)
			if !features.IntegerParameterCoercion {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, "version 85")
			require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(nil, p), "version 85")
			data, err := p.Marshal()
			require.NoError(t, err)
			_, err = decodeScope(data, c.proc, true, nil)
			require.ErrorContains(t, err, "version 85")
			table := &planpb.TableDef{Cols: []*planpb.ColDef{{Default: &planpb.Default{Expr: expr}}}}
			require.ErrorContains(t, plan2.RequirePersistedExpressionProtocol(c.proc.Ctx, c.proc, table), "version 85")
			// Existing publication hooks must cover the newly shared feature too.
			require.ErrorContains(t, plan2.RequirePersistedIPFunctionProtocol(c.proc.Ctx, c.proc, table), "version 85")
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, nil)
			require.ErrorContains(t, plan2.RequirePersistedExpressionProtocol(nil, c.proc, expr), "version 85")
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion85)
			require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, p))
			require.NoError(t, plan2.RequirePersistedExpressionProtocol(c.proc.Ctx, c.proc, table))
		})
	}
}

func TestIntegerArgumentReceiverRejectsInvalidSignatures(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion85)

	tests := []struct {
		name   string
		mutate func(*planpb.Expr)
		want   string
	}{
		{
			name: "arity",
			mutate: func(expr *planpb.Expr) {
				expr.GetF().Args = expr.GetF().Args[:1]
			},
			want: "arity",
		},
		{
			name: "source allowlist",
			mutate: func(expr *planpb.Expr) {
				expr.GetF().Args[0].Typ.Id = int32(types.T_date)
			},
			want: "source type",
		},
		{
			name: "int128 source",
			mutate: func(expr *planpb.Expr) {
				expr.GetF().Args[0].Typ.Id = int32(types.T_int128)
			},
			want: "source type",
		},
		{
			name: "uint128 source",
			mutate: func(expr *planpb.Expr) {
				expr.GetF().Args[0].Typ.Id = int32(types.T_uint128)
			},
			want: "source type",
		},
		{
			name: "target marker",
			mutate: func(expr *planpb.Expr) {
				expr.GetF().Args[1].Expr = &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}
			},
			want: "target marker",
		},
		{
			name: "target type",
			mutate: func(expr *planpb.Expr) {
				expr.GetF().Args[1].Typ.Id = int32(types.T_uint64)
			},
			want: "target marker",
		},
		{
			name: "result type",
			mutate: func(expr *planpb.Expr) {
				expr.Typ.Id = int32(types.T_date)
			},
			want: "result type",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expr := integerProtocolExpr(function.IntegerArgumentCastOverload)
			test.mutate(expr)
			p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
			_, err := planpb.RequiredRemoteExpressionFeatures(p)
			require.ErrorContains(t, err, test.want)
			data, err := p.Marshal()
			require.NoError(t, err)
			_, err = decodeScope(data, c.proc, true, nil)
			require.ErrorContains(t, err, test.want)
		})
	}

	// The temporal-only source and forced INT64 target remain valid for overload 8.
	expr := integerProtocolExpr(function.TemporalIntegerArgumentCastOverload)
	p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
	data, err := p.Marshal()
	require.NoError(t, err)
	_, err = decodeScope(data, c.proc, true, nil)
	require.NoError(t, err)
}

func TestIntegerArgumentAdmissionSourceAllowlistMatchesRegistry(t *testing.T) {
	registrySupports := func(overload int32, source types.T) bool {
		standard := source == types.T_any || source.IsInteger() || source.IsFloat() || source.IsDecimal() ||
			source == types.T_bool || source == types.T_bit || source == types.T_year ||
			source == types.T_enum || source.IsMySQLString()
		switch overload {
		case function.TextIntegerBitsCastOverload:
			return source == types.T_any || source.IsMySQLString()
		case function.TemporalIntegerArgumentCastOverload:
			return standard || source == types.T_date || source == types.T_time ||
				source == types.T_datetime || source == types.T_timestamp || source == types.T_uuid
		default:
			return standard
		}
	}

	for overload := function.IntegerArgumentCastOverload; overload <= function.TemporalIntegerArgumentCastOverload; overload++ {
		for sourceID := int32(0); sourceID <= 255; sourceID++ {
			expr := integerProtocolExpr(overload)
			expr.GetF().Args[0].Typ.Id = sourceID
			p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
			_, err := planpb.RequiredRemoteExpressionFeatures(p)
			if registrySupports(overload, types.T(sourceID)) {
				require.NoErrorf(t, err, "overload=%d source=%d", overload, sourceID)
			} else {
				require.Errorf(t, err, "overload=%d source=%d", overload, sourceID)
			}
		}
	}
}

func TestIntegerArgumentProtocolPlacementAndSend(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	expr := integerProtocolExpr(function.IntegerArgumentCastOverload)
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
	op := projection.NewArgument()
	defer op.Release()
	op.ProjectList = []*planpb.Expr{expr}
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	place := func(version int64) {
		client.version = version
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
		require.NoError(t, c.constrainIntegerArgumentWorkers(qry))
	}
	place(defines.MORPCVersion84)
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
	_, err := encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination")
	place(defines.MORPCVersion85)
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	data, err := encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.NotEmpty(t, data)
	// A downgrade/replacement after successful placement must fail at send time.
	client.version = defines.MORPCVersion84
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination")
	require.Error(t, validateIntegerArgumentDestination(c.proc, nil))
	require.Equal(t, client.calls, client.releases)
	ctx, cancel := context.WithCancel(c.proc.Ctx)
	cancel()
	c.proc.Ctx = ctx
	require.ErrorIs(t, validateIntegerArgumentDestination(c.proc, &pipeline.Pipeline{Node: &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"}}), context.Canceled)
}
