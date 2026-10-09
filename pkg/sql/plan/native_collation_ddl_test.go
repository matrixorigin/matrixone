package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

func TestExplicitTableCollationUsesRequestedSemanticIdentity(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version uint8
	}{
		{"utf8mb4_general_ci", types.CollationVersionLegacy},
		{"utf8mb4_bin", types.CollationVersionLegacy},
		{"utf8mb4_0900_ai_ci", types.CollationVersionV1},
		{"utf8mb4_0900_bin", types.CollationVersionV1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(true)
			stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL,
				"create table t (name varchar(32)) collate "+tc.name, 1)
			require.NoError(t, err)
			p, err := BuildPlan(ctx, stmt, false)
			stmt.Free()
			require.NoError(t, err)
			def := p.GetDdl().GetCreateTable().GetTableDef()
			require.NotNil(t, def)
			require.Equal(t, uint32(tc.version), def.Cols[0].Typ.CollationVersion)
		})
	}
}

func TestExplicitColumnCollationUsesRequestedSemanticIdentity(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version uint8
	}{
		{"utf8mb4_general_ci", types.CollationVersionLegacy},
		{"utf8mb4_bin", types.CollationVersionLegacy},
		{"utf8mb4_0900_ai_ci", types.CollationVersionV1},
		{"utf8mb4_0900_bin", types.CollationVersionV1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(true)
			stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL,
				"create table t (name varchar(32) collate "+tc.name+")", 1)
			require.NoError(t, err)
			p, err := BuildPlan(ctx, stmt, false)
			stmt.Free()
			require.NoError(t, err)
			def := p.GetDdl().GetCreateTable().GetTableDef()
			require.NotNil(t, def)
			require.Equal(t, uint32(tc.version), def.Cols[0].Typ.CollationVersion)
		})
	}
}
