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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestNamedGroupConcatNegotiatesPlacementAndFencesOldWorkers(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	loc, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	c.proc.Base.SessionInfo.TimeZone = loc
	expr := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: int64(function.GROUP_CONCAT) << 32}, Args: []*plan.Expr{{Typ: plan.Type{Id: int32(types.T_timestamp)}}}}}}
	qry := &plan.Query{Nodes: []*plan.Node{{AggList: []*plan.Expr{expr}}}}
	wire := &pipeline.Pipeline{Node: &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"}, InstructionList: []*pipeline.Instruction{{ProjectList: []*plan.Expr{expr}}}}
	for _, v := range []int64{defines.MORPCVersion64, defines.MORPCVersion65} {
		client.version = v
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
		require.NoError(t, c.constrainGroupConcatTimeZoneWorkers(qry))
		err := validateGroupConcatTimeZoneDestination(c.proc, wire)
		if v < defines.MORPCVersion65 {
			require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
			require.Equal(t, c.addr, c.cnList[0].Addr)
			require.Error(t, err)
		} else {
			require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
			require.NoError(t, err)
		}
	}
	c.proc.Base.SessionInfo.TimeZone = time.Local
	require.Error(t, validateGroupConcatTimeZoneDestination(nil, wire))
	if process.IsFixedTimeZone(time.Local) {
		require.NoError(t, validateGroupConcatTimeZoneDestination(c.proc, wire))
	} else {
		require.Error(t, validateGroupConcatTimeZoneDestination(c.proc, wire))
	}
	c.proc.Base.SessionInfo.TimeZone = time.FixedZone("FixedZone", 8*3600)
	client.version = defines.MORPCVersion64
	require.NoError(t, validateGroupConcatTimeZoneDestination(c.proc, wire))
	c.proc.Base.SessionInfo.TimeZone = time.UTC
	require.NoError(t, validateGroupConcatTimeZoneDestination(c.proc, wire))
}
