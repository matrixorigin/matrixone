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

package frontend

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestIntegerArithmeticPresentationPackets(t *testing.T) {
	for _, tc := range []struct {
		expr   string
		length uint32
	}{
		{"2*3", 3}, {strings.Repeat("0", 69) + "2+1", 72}, {"cast(2 as unsigned)*2", 22},
		{"case when rand()>0.5 then 00000002+1 else 3+4 end", 10},
		{"case when rand()>0.5 then 00000002+1 else cast(null as signed) end", 21},
		{"attr_seqnum*attr_seqnum", 7}, {"attnum+1", 12}, {"account_id*account_id", 20},
	} {
		t.Run(tc.expr, func(t *testing.T) {
			conn := &prepareResponseCaptureConn{}
			query := "select " + tc.expr + " as result"
			compiler := plan2.CompilerContext(plan2.NewEmptyCompilerContext(newPlanTestProcess(t)))
			if strings.Contains(tc.expr, "attr_seqnum") || strings.Contains(tc.expr, "attnum") || strings.Contains(tc.expr, "account_id") {
				query += " from mo_catalog.mo_columns"
				compiler = plan2.NewMockCompilerContext(false, newPlanTestProcess(t))
			}
			protocol, proc, stmt := newBinaryPrepareProtocolTestCaseWithConnAndContext(t, query, conn, compiler)
			defer func() { proc.SetPrepareParams(nil); stmt.clearBinaryParamState(proc) }()
			protocol.capability &^= CLIENT_DEPRECATE_EOF
			require.NoError(t, protocol.SendPrepareResponse(context.Background(), stmt))
			packets := splitProtocolPackets(t, conn.writes)
			require.Len(t, packets, 3)
			meta := parsePrepareColumnDefinition(t, packets[1])
			require.Equal(t, defines.MYSQL_TYPE_LONGLONG, meta.typ)
			require.Equal(t, tc.length, meta.length)
			require.Zero(t, meta.decimals)
			wantUnsigned := strings.Contains(tc.expr, "unsigned") || strings.Contains(tc.expr, "account_id")
			require.Equal(t, wantUnsigned, meta.flags&uint16(defines.UNSIGNED_FLAG) != 0)
			cols := plan2.GetResultColumnsFromPlan(stmt.PreparePlan.GetDcl().GetPrepare().Plan)
			payloads, err := protocol.MakeColumnDefData(context.Background(), cols)
			require.NoError(t, err)
			require.Len(t, payloads, 1)
			require.Equal(t, meta, parsePrepareColumnDefinition(t, payloads[0]), "direct and prepared central conversion")
		})
	}
}

func TestIntegerArithmeticDomainHistoryAndCacheKey(t *testing.T) {
	q := &pb.Query{Headings: []string{"x"}, IntegerPrecision: &pb.IntegerPrecisionGraph{SourceBudget: 65, ParameterCount: 1, Nodes: []*pb.IntegerPrecisionNode{{Kind: pb.IntegerPrecisionNode_PARAMETER, Precision: 20, ParamPos: 0}, {Kind: pb.IntegerPrecisionNode_ADD, Inputs: []uint32{1, 1}, Arithmetic: true}}, Outputs: []*pb.IntegerPrecisionOutput{{Node: 2, RootClass: pb.IntegerPrecisionOutput_DIRECT_FUNCTION}}}}
	p := &pb.Plan{Plan: &pb.Plan_Query{Query: q}}
	stmt := new(PrepareStmt)
	initial := []plan2.PreparedSourceBinding{{Position: 0}}
	null := []any{plan2.ParamValue{Value: nil, IsBinaryProtocol: true}}
	stmt.rememberIntegerArithmeticBindings(p, initial, null)
	require.Equal(t, types.T_int64, initial[0].Type.Oid)
	unsigned := []plan2.PreparedSourceBinding{{Position: 0, Type: types.T_uint64.ToType()}}
	values := []any{plan2.ParamValue{Value: uint64(2), IsBinaryProtocol: true}}
	stmt.rememberIntegerArithmeticBindings(p, unsigned, values)
	unsignedKey := preparedExecutionBindingKey(unsigned, values)
	next := []plan2.PreparedSourceBinding{{Position: 0}}
	stmt.rememberIntegerArithmeticBindings(p, next, null)
	require.Equal(t, types.T_uint64, next[0].Type.Oid)
	signed := []plan2.PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}}
	stmt.rememberIntegerArithmeticBindings(p, signed, values)
	require.NotEqual(t, unsignedKey, preparedExecutionBindingKey(signed, values), "packet cache must distinguish effective signs")
	decimal := []plan2.PreparedSourceBinding{{Position: 0, Type: types.New(types.T_decimal128, 10, 3)}}
	stmt.rememberIntegerArithmeticBindings(p, decimal, values)
	stmt.rememberIntegerArithmeticBindings(p, next, null)
	require.Equal(t, decimal[0], next[0], "NULL must not restore stale integer metadata after a decimal transition")
}

func TestIntegerArithmeticDomainMalformedResetAndClose(t *testing.T) {
	protocol, proc, stmt := newBinaryPrepareProtocolTestCase(t, "select ?*2")
	defer func() { proc.SetPrepareParams(nil); stmt.clearBinaryParamState(proc) }()
	stmt.integerArithmeticBindings = []plan2.PreparedSourceBinding{{Position: 0, Type: types.T_uint64.ToType()}}
	before := append([]plan2.PreparedSourceBinding(nil), stmt.integerArithmeticBindings...)
	require.Error(t, protocol.ParseExecuteData(context.Background(), proc, stmt, []byte{0}, 0))
	require.Equal(t, before, stmt.integerArithmeticBindings, "malformed payload cannot publish a domain")
	stmt.clearBinaryParamState(proc)
	require.Equal(t, before, stmt.integerArithmeticBindings, "statement RESET retains inferred domain")
	stmt.Close()
	require.Nil(t, stmt.integerArithmeticBindings, "CLOSE owns release")
}

func TestIntegerArithmeticMixedCommonUsesDecimalOwner(t *testing.T) {
	protocol, proc, stmt := newBinaryPrepareProtocolTestCase(t, "select case when rand()>0.5 then cast(1 as signed)+1 else cast(1 as unsigned)+1 end")
	defer func() { proc.SetPrepareParams(nil); stmt.clearBinaryParamState(proc) }()
	cols := plan2.GetResultColumnsFromPlan(stmt.PreparePlan.GetDcl().GetPrepare().Plan)
	require.Len(t, cols, 1)
	require.True(t, types.T(cols[0].Typ.Id).IsDecimal())
	require.Nil(t, cols[0].IntegerResultPresentation)
	_, err := protocol.MakeColumnDefData(t.Context(), cols)
	require.NoError(t, err)
}
