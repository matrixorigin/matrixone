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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func remoteSpatialDistanceProtocolPipeline(functionID, overloadID int32) *pipeline.Pipeline {
	return &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{
		ProjectList: []*planpb.Expr{{
			Typ: planpb.Type{Id: int32(types.T_float64)},
			Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{
					Obj:     function.EncodeOverloadID(functionID, overloadID),
					ObjName: "spatial-distance",
				},
			}},
		}},
	}}}
}

func TestRemoteSpatialDistanceProtocolValidation(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	rt := runtime.ServiceRuntime(proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(runtime.MOProtocolVersion, previous)
		} else {
			for _, value := range []int64{defines.MORPCVersion79, defines.MORPCVersion80} {
				rt.CompareAndDeleteGlobalVariables(runtime.MOProtocolVersion, value)
			}
		}
	})

	for _, tc := range []struct {
		name     string
		id       int32
		overload int32
		fenced   bool
	}{
		{name: "frechet legacy", id: function.ST_FRECHETDISTANCE, overload: 0, fenced: false},
		{name: "frechet unit", id: function.ST_FRECHETDISTANCE, overload: 2, fenced: true},
		{name: "frechet geodetic", id: function.ST_FRECHETDISTANCE, overload: 4, fenced: true},
		{name: "hausdorff legacy", id: function.ST_HAUSDORFFDISTANCE, overload: 1, fenced: false},
		{name: "hausdorff unit", id: function.ST_HAUSDORFFDISTANCE, overload: 3, fenced: true},
		{name: "hausdorff geodetic", id: function.ST_HAUSDORFFDISTANCE, overload: 4, fenced: true},
		{name: "distance unit", id: function.ST_DISTANCE, overload: 4, fenced: true},
		{name: "distance unit32", id: function.ST_DISTANCE, overload: 5, fenced: true},
		{name: "distance two-geometry overload", id: function.ST_DISTANCE, overload: 0, fenced: false},
		{name: "distance explicit SRID overload", id: function.ST_DISTANCE, overload: 1, fenced: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			remotePipeline := remoteSpatialDistanceProtocolPipeline(tc.id, tc.overload)
			rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion79)
			err := validateRemoteExpressionPipelineProtocol(proc, remotePipeline)
			if tc.fenced {
				require.ErrorContains(t, err, "geodetic spatial-distance semantics require MORPC protocol version 80")
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
			} else {
				require.NoError(t, err)
			}

			rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion80)
			require.NoError(t, validateRemoteExpressionPipelineProtocol(proc, remotePipeline))
		})
	}
}

func TestSpatialDistanceDestinationProtocolValidation(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	expr := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_float64)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{
				Obj:     function.EncodeOverloadID(function.ST_FRECHETDISTANCE, 4),
				ObjName: "st_frechetdistance",
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

	c.proc.Base.QueryClient = client
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	client.version = defines.MORPCVersion79
	require.NoError(t, c.constrainSpatialDistanceWorkers(&planpb.Query{
		Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}},
	}))
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
	_, err := encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination")

	client.version = defines.MORPCVersion80
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainSpatialDistanceWorkers(&planpb.Query{
		Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}},
	}))
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	data, err := encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.NotEmpty(t, data)
	require.Equal(t, client.calls, client.releases)
}

func TestSpatialDistanceProtocolFeatureIDsRemainStable(t *testing.T) {
	for _, tc := range []struct {
		name     string
		id       int32
		overload int32
		want     bool
	}{
		{name: "distance", id: function.ST_DISTANCE, overload: 0, want: false},
		{name: "frechet legacy", id: function.ST_FRECHETDISTANCE, overload: 0, want: false},
		{name: "frechet geodetic", id: function.ST_FRECHETDISTANCE, overload: 4, want: true},
		{name: "hausdorff legacy", id: function.ST_HAUSDORFFDISTANCE, overload: 1, want: false},
		{name: "hausdorff geodetic", id: function.ST_HAUSDORFFDISTANCE, overload: 4, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			features, err := planpb.RequiredRemoteExpressionFeatures(
				remoteSpatialDistanceProtocolPipeline(tc.id, tc.overload))
			require.NoError(t, err)
			require.Equal(t, tc.want, features.SpatialDistanceSemantics)
		})
	}
}
