package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

func TestExplicitTableCollationUsesVersionedSemanticIdentity(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL,
		"create table t (name varchar(32)) collate utf8mb4_general_ci", 1)
	require.NoError(t, err)
	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	def := p.GetDdl().GetCreateTable().GetTableDef()
	require.NotNil(t, def)
	require.Equal(t, uint32(types.CollationVersionV1), def.Cols[0].Typ.CollationVersion)
}

func TestExplicitColumnCollationUsesVersionedSemanticIdentity(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL,
		"create table t (name varchar(32) collate utf8mb4_general_ci)", 1)
	require.NoError(t, err)
	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	def := p.GetDdl().GetCreateTable().GetTableDef()
	require.NotNil(t, def)
	require.Equal(t, uint32(types.CollationVersionV1), def.Cols[0].Typ.CollationVersion)
}
