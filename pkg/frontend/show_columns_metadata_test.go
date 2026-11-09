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

package frontend

import (
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestShowColumnsWireMetadata(t *testing.T) {
	// Independent MySQL 8.4 ColumnDefinition41 observations; MO retains its
	// ordinary Comment column and advertises supported utf8mb4_general_ci.
	want := []struct {
		name   string
		typ    byte
		length uint32
		flags  uint16
	}{
		{"Field", 253, 256, 0}, {"Type", 252, 67108860, 4241},
		{"Collation", 253, 256, 0}, {"Null", 253, 12, 1},
		{"Key", 254, 12, 4481}, {"Default", 252, 262140, 144},
		{"Extra", 253, 1024, 0}, {"Privileges", 253, 616, 0},
		{"Comment", 252, 24576, 145},
	}
	for _, count := range []int{7, 9} {
		cw := &TxnComputationWrapper{stmt: &tree.ShowColumns{}, plan: newResultColumnTestPlan(count)}
		columns, err := cw.GetColumns(t.Context())
		require.NoError(t, err)
		require.Len(t, columns, count)
		proto := &MysqlProtocolImpl{io: NewIOPackage(true)}
		conn := &prepareResponseCaptureConn{}
		wire, _, prepared := newBinaryPrepareProtocolTestCaseWithConn(t, "select 1", conn)
		wire.capability &^= CLIENT_DEPRECATE_EOF
		prepared.PrepareStmt = cw.stmt
		prepared.PreparePlan.GetDcl().GetPrepare().Plan = cw.plan
		defs := plan2.GetResultColumnsFromPlan(cw.plan)
		cached, err := makeStatementColumnDefData(t.Context(), wire, cw.stmt, defs)
		require.NoError(t, err)
		var responses [][][]byte
		for _, cache := range [][][]byte{nil, cached} {
			prepared.ColDefData = cache
			conn.writes = nil
			require.NoError(t, wire.SendPrepareResponse(t.Context(), prepared))
			packets := splitProtocolPackets(t, conn.writes)
			require.Len(t, packets, count+2)
			require.Equal(t, uint16(count), binary.LittleEndian.Uint16(packets[0][5:]))
			responses = append(responses, packets[1:1+count])
		}
		i := 0
		for _, expected := range want {
			if count == 7 && (expected.name == "Collation" || expected.name == "Privileges") {
				continue
			}
			col := columns[i].(*MysqlColumn)
			for _, command := range []int{int(COM_QUERY), int(COM_STMT_PREPARE)} {
				packet := proto.makeColumnDefinition41Payload(col, command)
				got := parsePrepareColumnDefinition(t, packet[HeaderOffset:])
				require.Equal(t, expected.name, got.name)
				require.Equal(t, defines.MysqlType(expected.typ), got.typ)
				require.Equal(t, expected.length, got.length)
				require.Equal(t, expected.flags, got.flags)
				require.Equal(t, uint16(45), got.charset)
				require.Zero(t, got.decimals)
			}
			for _, response := range responses {
				got := parsePrepareColumnDefinition(t, response[i])
				require.Equal(t, expected.name, got.name)
				require.Equal(t, defines.MysqlType(expected.typ), got.typ)
				require.Equal(t, expected.length, got.length)
				require.Equal(t, expected.flags, got.flags)
				require.Equal(t, uint16(45), got.charset)
				require.Zero(t, got.decimals)
				require.Equal(t, cached[i][HeaderOffset:], response[i])
			}
			i++
		}
		prepared.PreparePlan.GetDcl().GetPrepare().Plan = newResultColumnTestPlan(1)
		conn.writes = nil
		require.ErrorContains(t, wire.SendPrepareResponse(t.Context(), prepared), "unexpected SHOW COLUMNS result width")
		require.Empty(t, conn.writes, "invalid metadata must fail before partial protocol output")
		// Reuse must not mutate or borrow protocol columns across calls.
		again, err := cw.GetColumns(t.Context())
		require.NoError(t, err)
		require.Equal(t, columns, again)
		require.NotSame(t, columns[0], again[0])
	}
	for _, count := range []int{0, 6, 8, 10} {
		_, err := showColumnsWireMetadata(t.Context(), count)
		require.ErrorContains(t, err, "unexpected SHOW COLUMNS result width")
	}
}

func TestStatementColumnDefDataPreservesSelectMetadata(t *testing.T) {
	proto := &MysqlProtocolImpl{io: NewIOPackage(true)}
	cols := plan2.GetResultColumnsFromPlan(newResultColumnTestPlan(2))
	want, err := proto.MakeColumnDefData(t.Context(), cols, 42, 64)
	require.NoError(t, err)
	got, err := makeStatementColumnDefData(t.Context(), proto, &tree.Select{}, cols, 42, 64)
	require.NoError(t, err)
	require.Equal(t, want, got)
}
