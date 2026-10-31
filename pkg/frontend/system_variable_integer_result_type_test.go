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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// issue #31717: the ten integer system variables must expose MySQL integer
// result-set column metadata. The class-signedness policy follows the registry
// declaration domain, and every one of the ten is declared in an unsigned
// family: the pinned MySQL primary source (mysql/mysql-server@9fc8d600,
// sql/sys_vars.cc) defines Sys_max_connections :2970 and
// Sys_net_wait_timeout :5327 (also Sys_max_allowed_packet :2909,
// Sys_interactive_timeout :2332, Sys_connect_timeout :2056,
// Sys_auto_increment_increment :990, Sys_group_concat_max_len :5797,
// Sys_net_read_timeout :3214) as Sys_var_ulong, Sys_cte_max_recursion_depth
// :1012 as Sys_var_uint, and Sys_select_limit :5616 as Sys_var_harows, whose
// SHOW_LONG / SHOW_INT / SHOW_HA_ROWS / SHOW_LONGLONG families resolve to an
// unsigned LONGLONG column in Item_func_get_system_var::resolve_type
// (sql/item_func.cc:6992-6997; only the SHOW_SIGNED_* families stay signed).
var issue31717IntegerVariables = []struct{ name string }{
	{"max_connections"},
	{"max_allowed_packet"},
	{"wait_timeout"},
	{"interactive_timeout"},
	{"connect_timeout"},
	{"auto_increment_increment"},
	{"cte_max_recursion_depth"},
	{"group_concat_max_len"},
	{"sql_select_limit"},
	{"net_read_timeout"},
}

// newSystemVariablePlanContext returns a real TxnCompilerContext minimally able
// to build a variable-only SELECT plan. The system-variable type resolution of
// this batch never touches session state.
func newSystemVariablePlanContext(t *testing.T) *TxnCompilerContext {
	t.Helper()
	proc := testutil.NewProcess(t)
	ses, _ := newObservedProtocolSession()
	var planningSession FeSession = ses
	return &TxnCompilerContext{execCtx: &ExecCtx{reqCtx: proc.Ctx, ses: planningSession, proc: proc}}
}

// TestResolveVariableTypeDeclaresIntegerSystemVariables pins the first owner
// of the repair plan: the compile context must return the registry-declared
// engine type for integer system variables and nothing for every other class.
// Signedness follows the declared domain: the ten unsigned-family
// declarations resolve to T_uint64 while the one negative-domain declaration
// (optimizer_trace_offset) stays T_int64.
func TestResolveVariableTypeDeclaresIntegerSystemVariables(t *testing.T) {
	tcc := &TxnCompilerContext{}
	for _, tc := range issue31717IntegerVariables {
		typ, err := tcc.ResolveVariableType(tc.name, true, false)
		require.NoError(t, err, tc.name)
		expected := types.T_uint64
		require.Equal(t, plan2.Type{Id: int32(expected), Charset: uint32(types.CharsetType(expected))},
			typ, tc.name)
	}
	// A declaration whose domain admits negative values renders signed,
	// mirroring MySQL's SHOW_SIGNED_* families; its registry default is -1.
	signed, err := tcc.ResolveVariableType("optimizer_trace_offset", true, false)
	require.NoError(t, err)
	require.Equal(t, plan2.Type{Id: int32(types.T_int64), Charset: uint32(types.CharsetType(types.T_int64))},
		signed)
	for _, tc := range []struct {
		name string
		why  string
	}{
		{"version", "string class keeps the text envelope"},
		{"autocommit", "bool class keeps the text envelope"},
		{"sql_mode", "set class keeps the text envelope"},
		{"tx_isolation", "enum class keeps the text envelope"},
		{"long_query_time", "double class keeps the text envelope"},
		{"nosuchvariable31717", "unknown system variable must stay unresolved"},
	} {
		typ, err := tcc.ResolveVariableType(tc.name, true, false)
		require.NoError(t, err, tc.name)
		require.Equal(t, plan2.Type{}, typ, "%s: %s", tc.name, tc.why)
	}
	// Case-insensitive spelling resolves to the same declared type.
	upper, err := tcc.ResolveVariableType("MAX_CONNECTIONS", true, false)
	require.NoError(t, err)
	lower, err := tcc.ResolveVariableType("max_connections", true, false)
	require.NoError(t, err)
	require.Equal(t, lower, upper)
}

// prepareColumnForQuery compiles sql through the real TxnCompilerContext and
// returns the first result column definition parsed from the actual binary
// prepare-response wire bytes.
func prepareColumnForQuery(t *testing.T, sql string) prepareColumnDefinition {
	t.Helper()
	conn := &prepareResponseCaptureConn{}
	tcc := newSystemVariablePlanContext(t)
	proto, proc, prepareStmt := newBinaryPrepareProtocolTestCaseWithConnAndContext(t, sql, conn, tcc)
	proto.capability &^= CLIENT_DEPRECATE_EOF
	defer func() {
		proc.SetPrepareParams(nil)
		prepareStmt.clearBinaryParamState(proc)
	}()
	require.NoError(t, proto.SendPrepareResponse(context.TODO(), prepareStmt))
	packets := splitProtocolPackets(t, conn.writes)
	require.Len(t, packets, 3)
	return parsePrepareColumnDefinition(t, packets[1])
}

// TestIntegerSystemVariablePreparedWireMetadata walks the full forward wire
// chain (binder -> plan result columns -> prepare response) for the ten
// reported variables plus legal scope spellings, asserting the column type
// and signedness flags read from the wire bytes.
func TestIntegerSystemVariablePreparedWireMetadata(t *testing.T) {
	for _, tc := range issue31717IntegerVariables {
		t.Run(tc.name, func(t *testing.T) {
			column := prepareColumnForQuery(t, "select @@"+tc.name+" as v")
			require.Equal(t, "v", column.name)
			require.Equal(t, defines.MYSQL_TYPE_LONGLONG, column.typ)
			require.NotZero(t, column.flags&uint16(defines.UNSIGNED_FLAG), tc.name)
		})
	}
	t.Run("global-scope unsigned", func(t *testing.T) {
		column := prepareColumnForQuery(t, "select @@global.sql_select_limit as v")
		require.Equal(t, defines.MYSQL_TYPE_LONGLONG, column.typ)
		require.NotZero(t, column.flags&uint16(defines.UNSIGNED_FLAG))
	})
	t.Run("session-scope unsigned", func(t *testing.T) {
		column := prepareColumnForQuery(t, "select @@session.wait_timeout as v")
		require.Equal(t, defines.MYSQL_TYPE_LONGLONG, column.typ)
		require.NotZero(t, column.flags&uint16(defines.UNSIGNED_FLAG))
	})
	t.Run("signed-declaration control", func(t *testing.T) {
		column := prepareColumnForQuery(t, "select @@optimizer_trace_offset as v")
		require.Equal(t, "v", column.name)
		require.Equal(t, defines.MYSQL_TYPE_LONGLONG, column.typ)
		require.Zero(t, column.flags&uint16(defines.UNSIGNED_FLAG))
	})
}

// TestNonIntegerSystemVariablePreparedWireMetadataUnchanged proves the
// metadata adjacency policy: bool, string, enum, set and double system
// variables, an unknown system variable, and an untyped user variable all
// keep the existing text envelope (“select cast('a' as text)”) instead of
// being retyped by the integer-declaration repair.
func TestNonIntegerSystemVariablePreparedWireMetadataUnchanged(t *testing.T) {
	textEnvelope := prepareColumnForQuery(t, "select cast('a' as text) as v").typ
	for _, name := range []string{
		"version",     // string class
		"autocommit",   // bool class
		"sql_mode",     // set class
		"tx_isolation", // enum class
		"long_query_time",
	} {
		column := prepareColumnForQuery(t, "select @@"+name+" as v")
		require.Equal(t, textEnvelope, column.typ, name)
	}
	column := prepareColumnForQuery(t, "select @@nosuchvariable31717 as v")
	require.Equal(t, textEnvelope, column.typ)
	column = prepareColumnForQuery(t, "select @unknownuservariable31717 as v")
	require.Equal(t, textEnvelope, column.typ)
}
