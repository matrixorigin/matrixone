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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/defines"
)

func TestParseExecuteDataRequiresCompleteFixedHeader(t *testing.T) {
	for _, offset := range []int{0, 4} {
		for bodyLen := 0; bodyLen < 5; bodyLen++ {
			proto, proc, stmt := newBinaryPrepareProtocolTestCase(t, "select 1")
			data := make([]byte, offset+bodyLen)
			stmt.cursorRequested = true
			require.Error(t, proto.ParseExecuteData(context.Background(), proc, stmt, data, offset),
				"offset %d, fixed body length %d", offset, bodyLen)
			require.True(t, stmt.cursorRequested, "a short packet must not change cursor state")
			stmt.Close()
		}

		proto, proc, stmt := newBinaryPrepareProtocolTestCase(t, "select 1")
		data := make([]byte, offset+5)
		require.NoError(t, proto.ParseExecuteData(context.Background(), proc, stmt, data, offset))
		stmt.Close()
	}

	proto, proc, stmt := newBinaryPrepareProtocolTestCase(t, "select 1")
	defer stmt.Close()
	require.Error(t, proto.ParseExecuteData(context.Background(), proc, stmt, nil, -1))
	require.Error(t, proto.ParseExecuteData(context.Background(), proc, stmt, nil, 1))
}

func TestParseExecuteDataNonzeroNewTypesAndMalformedRecovery(t *testing.T) {
	ctx := context.Background()
	proto, proc, stmt := newBinaryPrepareProtocolTestCase(t, "select ?")
	defer stmt.Close()
	packet := func(flag byte, typ defines.MysqlType, values ...byte) []byte {
		data := []byte{0, 1, 0, 0, 0, 0, flag}
		if flag != 0 {
			data = append(data, byte(typ), 0)
		}
		return append(data, values...)
	}

	require.NoError(t, proto.ParseExecuteData(ctx, proc, stmt,
		packet(1, defines.MYSQL_TYPE_TINY, 10), 0))
	require.Equal(t, "10", stmt.params.GetStringAt(0))
	require.Equal(t, []byte{byte(defines.MYSQL_TYPE_TINY), 0}, stmt.ParamTypes)

	// MySQL interprets every nonzero bind flag as "new types", including 2
	// and 255. A following zero flag must reuse the most recent type.
	require.NoError(t, proto.ParseExecuteData(ctx, proc, stmt,
		packet(2, defines.MYSQL_TYPE_SHORT, 0x34, 0x12), 0))
	require.Equal(t, "4660", stmt.params.GetStringAt(0))
	require.Equal(t, []byte{byte(defines.MYSQL_TYPE_SHORT), 0}, stmt.ParamTypes)
	require.NoError(t, proto.ParseExecuteData(ctx, proc, stmt,
		packet(0, 0, 42, 0), 0))
	require.Equal(t, "42", stmt.params.GetStringAt(0))
	require.NoError(t, proto.ParseExecuteData(ctx, proc, stmt,
		packet(255, defines.MYSQL_TYPE_TINY, 7), 0))
	require.Equal(t, "7", stmt.params.GetStringAt(0))
	require.Equal(t, []byte{byte(defines.MYSQL_TYPE_TINY), 0}, stmt.ParamTypes)

	// Neither a missing type array nor a complete type array followed by a
	// truncated value may replace the last successfully bound type.
	require.Error(t, proto.ParseExecuteData(ctx, proc, stmt,
		[]byte{0, 1, 0, 0, 0, 0, 2}, 0))
	require.Equal(t, []byte{byte(defines.MYSQL_TYPE_TINY), 0}, stmt.ParamTypes)
	require.Error(t, proto.ParseExecuteData(ctx, proc, stmt,
		packet(2, defines.MYSQL_TYPE_SHORT, 0x34), 0))
	require.Equal(t, []byte{byte(defines.MYSQL_TYPE_TINY), 0}, stmt.ParamTypes)
	require.NoError(t, proto.ParseExecuteData(ctx, proc, stmt,
		packet(0, 0, 9), 0))
	require.Equal(t, "9", stmt.params.GetStringAt(0))
}

func TestParseExecuteDataNonzeroNewTypesWithNullParam(t *testing.T) {
	proto, proc, stmt := newBinaryPrepareProtocolTestCase(t, "select ?")
	defer stmt.Close()
	data := []byte{0, 1, 0, 0, 0, 1, 2, byte(defines.MYSQL_TYPE_TINY), 0}
	require.NoError(t, proto.ParseExecuteData(context.Background(), proc, stmt, data, 0))
	require.Equal(t, []byte{byte(defines.MYSQL_TYPE_TINY), 0}, stmt.ParamTypes)
	require.True(t, stmt.params.GetNulls().Contains(0))
}
