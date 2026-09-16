package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

func TestBuildPlanRejectsNativeCollationByDefault(t *testing.T) {
	native0900AdmissionDisabled(t)
	ctx := NewMockCompilerContext(true)
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL,
		"select 'Alpha' collate utf8mb4_0900_ai_ci", 1)
	require.NoError(t, err)
	_, err = BuildPlan(ctx, stmt, false)
	require.ErrorContains(t, err, native0900AdmissionError)
}

func TestBuildPlanRejectsFoldedNativeCollationByDefault(t *testing.T) {
	native0900AdmissionDisabled(t)
	ctx := NewMockCompilerContext(true)
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL,
		"select ('Alpha' collate utf8mb4_0900_ai_ci) = 'alpha'", 1)
	require.NoError(t, err)
	_, err = BuildPlan(ctx, stmt, false)
	require.ErrorContains(t, err, native0900AdmissionError)
}
