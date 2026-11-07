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

package plan

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

// TestShowCreateReplayKeepsAssignmentPaddedBinaryDefault pins the restore
// regression the #31066 review found for the superseded re-quote rendering:
// the binder assignment-pads a fixed binary literal's bound value (Sval), so
// rendering a default from that bound literal instead of the stored default
// SQL text emits the padding and replays a different stored default. SHOW
// CREATE TABLE must print the stored text, so replaying the printed DDL
// recreates the source default and the persisted DUMP schema fingerprint of
// the replayed table stays identical to the source archive's.
func TestShowCreateReplayKeepsAssignmentPaddedBinaryDefault(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	source, err := buildTestCreateTableStmt(mock,
		"create table padded_binary_defaults("+
			"a binary(8) default 'a', "+
			"b varbinary(16) default 'a', "+
			"c varchar(8) default 'it''s')")
	require.NoError(t, err)

	// Assignment padding applies to the fixed binary literal only; every
	// column keeps the user spelling as its stored default SQL text.
	padded := "a\x00\x00\x00\x00\x00\x00\x00"
	require.Equal(t, padded, FindColumn(source.Cols, "a").Default.Expr.GetLit().GetSval())
	for _, col := range []struct{ name, origin string }{
		{"a", "'a'"},
		{"b", "'a'"},
		{"c", "'it''s'"},
	} {
		require.Equal(t, col.origin, FindColumn(source.Cols, col.name).Default.OriginString, col.name)
	}

	showSQL, parsed, err := ConstructCreateTableSQL(&mock.ctxt, source, nil, false, nil)
	require.NoError(t, err)
	if parsed != nil {
		parsed.Free()
	}
	require.Contains(t, showSQL, "`a` binary(8) DEFAULT 'a'")
	require.Contains(t, showSQL, "`b` varbinary(16) DEFAULT 'a'")
	require.NotContains(t, showSQL, `\0`, "the padded bound literal must not leak into replayable DDL")

	// Replaying the printed statement re-binds the same stored defaults:
	// the same SQL text, the same padded Sval, and a stable second cycle.
	replayed, err := buildTestCreateTableStmt(mock, showSQL)
	require.NoError(t, err)
	for _, name := range []string{"a", "b", "c"} {
		require.Equal(t,
			FindColumn(source.Cols, name).Default.OriginString,
			FindColumn(replayed.Cols, name).Default.OriginString, name)
		require.True(t, proto.Equal(
			FindColumn(source.Cols, name).Default.Expr,
			FindColumn(replayed.Cols, name).Default.Expr), name)
	}

	replaySQL, reparsed, err := ConstructCreateTableSQL(&mock.ctxt, replayed, nil, false, nil)
	require.NoError(t, err)
	if reparsed != nil {
		reparsed.Free()
	}
	require.Equal(t, showSQL, replaySQL)

	// The fingerprint input of the replayed table is therefore unchanged,
	// so a target rebuilt from SHOW CREATE output still matches the
	// archived manifest hash.
	sourceHash, err := ConstructCreateTableSQLForSchemaHash(source)
	require.NoError(t, err)
	replayHash, err := ConstructCreateTableSQLForSchemaHash(replayed)
	require.NoError(t, err)
	require.Equal(t, sourceHash, replayHash)
}
