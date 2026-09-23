package plan

import (
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBuildViewColumnsRejectsDirectUse(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
		"select * from nation cross apply mo_view_columns(n_nationkey) mc", 1)
	require.NoError(t, err)
	defer stmt.Free()
	_, err = BuildPlan(ctx, stmt, false)
	require.ErrorContains(t, err, "private to information_schema metadata views")
}
