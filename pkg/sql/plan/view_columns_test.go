package plan

import (
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBuildViewColumns(t *testing.T) {
	for _, sql := range []string{
		"select mc.attname from nation cross apply mo_view_columns(n_nationkey) mc where mc.att_is_hidden=0",
		"with v as (select * from nation) select mc.attname from v cross apply mo_view_columns(n_nationkey) mc where mc.att_is_hidden=0 union all select n_name from v",
	} {
		ctx := NewMockCompilerContext(false)
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		_, err = BuildPlan(ctx, stmt, false)
		stmt.Free()
		require.NoError(t, err, sql)
	}
}
