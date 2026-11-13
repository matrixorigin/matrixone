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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/require"
)

func TestRuntimeFilterSpecAssociation(t *testing.T) {
	for _, nonPK := range []bool{false, true} {
		name := "primary-key"
		if nonPK {
			name = "non-primary-key"
		}
		t.Run(name, func(t *testing.T) {
			c := NewMockCompile(t)
			c.proc.SetMessageBoard(message.NewMessageBoard())
			col := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "k", ColPos: 0}}}
			specs := []*plan.RuntimeFilterSpec{{Tag: 1, Expr: col, NotOnPk: !nonPK}, {Tag: 2, Expr: col, NotOnPk: nonPK}}
			scan := table_scan.NewArgument()
			t.Cleanup(scan.Release)
			s := &Scope{Proc: c.proc, DataSource: &Source{RuntimeFilterSpecs: specs}, RootOp: scan}
			vec := vector.NewVec(types.T_int32.ToType())
			t.Cleanup(func() { vec.Free(c.proc.Mp()) })
			require.NoError(t, vector.AppendFixed(vec, int32(7), false, c.proc.Mp()))
			data, err := vec.MarshalBinary()
			require.NoError(t, err)
			message.SendMessage(message.RuntimeFilterMessage{Tag: 1, Typ: message.RuntimeFilter_PASS}, c.proc.GetMessageBoard())
			message.SendMessage(message.RuntimeFilterMessage{Tag: 2, Typ: message.RuntimeFilter_IN, Card: 1, Data: data}, c.proc.GetMessageBoard())
			filters, drop, err := s.waitForRuntimeFilters(c)
			require.NoError(t, err)
			require.False(t, drop)
			block, err := s.handleRuntimeFilters(c, filters)
			require.NoError(t, err)
			require.Len(t, block, 1)
			if nonPK {
				require.Nil(t, s.DataSource.FilterExpr, "PASS must not shift the remaining IN expression onto a PK spec")
				require.Len(t, scan.RuntimeFilterExprs, 1)
			} else {
				require.NotNil(t, s.DataSource.FilterExpr)
				require.Empty(t, scan.RuntimeFilterExprs)
			}
		})
	}
}

func TestRuntimeFilterTerminalMessageContract(t *testing.T) {
	for _, tc := range []struct {
		name      string
		types     []int32
		wantDrop  bool
		wantError bool
	}{
		{name: "pass", types: []int32{message.RuntimeFilter_PASS}},
		{name: "parallel-shuffle-pass", types: []int32{message.RuntimeFilter_PASS, message.RuntimeFilter_PASS}},
		{name: "drop", types: []int32{message.RuntimeFilter_DROP}, wantDrop: true},
		{name: "duplicate-in", types: []int32{message.RuntimeFilter_IN, message.RuntimeFilter_IN}, wantError: true},
		{name: "conflicting-terminal", types: []int32{message.RuntimeFilter_PASS, message.RuntimeFilter_DROP}, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewMockCompile(t)
			c.proc.SetMessageBoard(message.NewMessageBoard())
			s := &Scope{Proc: c.proc, DataSource: &Source{RuntimeFilterSpecs: []*plan.RuntimeFilterSpec{{Tag: 1}}}}
			for _, typ := range tc.types {
				message.SendMessage(message.RuntimeFilterMessage{Tag: 1, Typ: typ}, c.proc.GetMessageBoard())
			}
			filters, drop, err := s.waitForRuntimeFilters(c)
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.wantDrop, drop)
			}
			require.Empty(t, filters)
		})
	}
}

func TestRuntimeFilterCancellationIsError(t *testing.T) {
	for _, name := range []string{"waiting", "already-queued"} {
		t.Run(name, func(t *testing.T) {
			c := &Compile{proc: testutil.NewProcess(t)}
			c.proc.SetMessageBoard(message.NewMessageBoard())
			if name == "already-queued" {
				message.SendMessage(message.RuntimeFilterMessage{Tag: 1, Typ: message.RuntimeFilter_PASS}, c.proc.GetMessageBoard())
			}
			ctx, cancel := context.WithCancel(c.proc.Ctx)
			cancel()
			c.proc.Ctx = ctx
			s := &Scope{Proc: c.proc, DataSource: &Source{RuntimeFilterSpecs: []*plan.RuntimeFilterSpec{{Tag: 1}}}}
			_, _, err := s.waitForRuntimeFilters(c)
			require.ErrorIs(t, err, context.Canceled, "canceled filtering must not become a successful query with no output")
		})
	}
}
