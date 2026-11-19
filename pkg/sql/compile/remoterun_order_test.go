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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestRemoteScanOrderRoundTrip(t *testing.T) {
	proc := &process.Process{Base: &process.BaseProcess{}, Ctx: t.Context()}
	defer proc.Free()
	order := []*plan.OrderBySpec{{
		Expr: &plan.Expr{Typ: plan.Type{Id: int32(types.T_datetime), NotNullable: true},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0, Name: "request_at"}}},
		Flag: plan.OrderBySpec_DESC,
	}}
	original := &Scope{Proc: proc, DataSource: &Source{
		OrderBy:     order,
		node:        &plan.Node{OrderBy: order, TableDef: &plan.TableDef{Name: "history"}},
		RecvMsgList: []plan.MsgHeader{{MsgTag: 42, MsgType: int32(message.MsgTopValue)}},
	}}
	data, err := encodeScope(original)
	require.NoError(t, err)
	wire := new(pipeline.Pipeline)
	require.NoError(t, wire.Unmarshal(data))
	restored, err := generateScope(proc, wire, &scopeContext{}, true)
	require.NoError(t, err)
	defer restored.release()
	require.Equal(t, original.DataSource.OrderBy, restored.DataSource.OrderBy)
	require.Equal(t, original.DataSource.RecvMsgList, restored.DataSource.RecvMsgList)
}
