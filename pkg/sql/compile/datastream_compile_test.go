// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/stretchr/testify/require"
)

func datastreamTestNode(recheck bool) *plan.Node {
	pushable := &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "<"},
			Args: []*plan.Expr{
				{Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0, Name: "t.a"}}},
				{
					Typ:  plan.Type{Id: int32(types.T_int64)},
					Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 5}}},
				},
			},
		}},
	}
	unpushable := &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "abs"},
			Args: []*plan.Expr{
				{Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0, Name: "t.a"}}},
			},
		}},
	}
	return &plan.Node{
		NodeType: plan.Node_EXTERNAL_SCAN,
		TableDef: &plan.TableDef{
			Cols: []*plan.ColDef{{Name: "a", Typ: plan.Type{Id: int32(types.T_int32)}}},
		},
		ExternScan: &plan.ExternScan{
			Type: int32(plan.ExternType_DATASTREAM_TB),
			DatastreamScan: &plan.DataStreamScan{
				Server:  "127.0.0.1",
				Port:    4444,
				Table:   "src",
				Recheck: recheck,
			},
		},
		FilterList: []*plan.Expr{pushable, unpushable},
	}
}

func TestCompileDatastreamScan(t *testing.T) {
	newCompile := func() *Compile {
		c := NewMockCompile(t)
		c.addr = "local:6001"
		c.anal = &AnalyzeModule{qry: &plan.Query{}}
		c.proc.Base.SessionInfo.TimeZone = time.UTC
		return c
	}

	// recheck=true: the pushed text is only a hint, the local filter list is
	// untouched
	c := newCompile()
	node := datastreamTestNode(true)
	scopes, err := c.compileDatastreamScan(node, true)
	require.NoError(t, err)
	require.Len(t, scopes, 1)
	require.Equal(t, "(`a` < 5)", node.ExternScan.DatastreamScan.PushedFilter)
	require.Len(t, node.FilterList, 2)
	op, ok := scopes[0].RootOp.(*external.External)
	require.True(t, ok)
	require.Same(t, node.ExternScan.DatastreamScan, op.Es.DatastreamScan)
	require.NotNil(t, op.Es.Extern)

	// recheck=false: exactly the pushed conjunct is trimmed; the
	// non-deparsable one stays local
	c = newCompile()
	node = datastreamTestNode(false)
	scopes, err = c.compileDatastreamScan(node, true)
	require.NoError(t, err)
	require.Len(t, scopes, 1)
	require.Equal(t, "(`a` < 5)", node.ExternScan.DatastreamScan.PushedFilter)
	require.Len(t, node.FilterList, 1)
	require.Equal(t, "abs", node.FilterList[0].GetF().Func.GetObjName())

	// missing scan metadata is a clean error
	c = newCompile()
	_, err = c.compileDatastreamScan(&plan.Node{ExternScan: &plan.ExternScan{}}, true)
	require.Error(t, err)
}
