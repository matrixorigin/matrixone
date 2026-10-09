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

func TestBuildPlanKeepsLegacyCollationNoOpForNonStrings(t *testing.T) {
	native0900AdmissionDisabled(t)
	for _, sql := range []string{
		"select 1 collate utf8mb4_general_ci",
		"select null collate utf8mb4_bin",
	} {
		t.Run(sql, func(t *testing.T) {
			ctx := NewMockCompilerContext(true)
			stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			_, err = BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
		})
	}
}

func TestBuildPlanRejectsNativeCollationInsideDefaultBeforeFold(t *testing.T) {
	native0900AdmissionDisabled(t)
	for _, sql := range []string{
		"create table t (v int default (length('a' collate utf8mb4_0900_ai_ci)))",
		"create table t (v int default (('a' collate utf8mb4_0900_ai_ci) = 'a'))",
		"create table t (v varchar(8) default (ifnull('a' collate utf8mb4_0900_ai_ci, 'b')))",
	} {
		t.Run(sql, func(t *testing.T) {
			ctx := NewMockCompilerContext(true)
			stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			_, err = BuildPlan(ctx, stmt, false)
			require.ErrorContains(t, err, native0900AdmissionError)
		})
	}
}
