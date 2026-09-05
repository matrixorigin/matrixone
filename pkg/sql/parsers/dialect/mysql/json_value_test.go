package mysql

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestJSONValueClausesParseAndFormat(t *testing.T) {
	_, err := ParseOne(context.Background(), "select json_value(doc, '$')", 1)
	require.NoError(t, err)
	stmt, err := ParseOne(context.Background(),
		"select json_value(doc, ? returning char(12) character set utf8mb4 default 'x' on empty error on error)", 1)
	require.NoError(t, err)
	formatted := tree.String(stmt, dialect.MYSQL)
	require.Contains(t, formatted, "json_value(doc, ? returning char(12) character set utf8mb4 default 'x' on empty error on error)")

	_, err = ParseOne(context.Background(), "select json_value(doc, '$' null on error null on empty)", 1)
	require.Error(t, err)
	_, err = ParseOne(context.Background(), "select json_value(doc, '$' default null on error)", 1)
	require.Error(t, err)
	_, err = ParseOne(context.Background(), "select json_value(doc, '$' default -null on error)", 1)
	require.Error(t, err)
	view, err := ParseOne(context.Background(),
		"create view json_value_view as select json_value(doc, '$') as extracted", 1)
	require.NoError(t, err)
	require.Contains(t, tree.String(view, dialect.MYSQL),
		"json_value(doc, $ returning char(512))")
}

func TestJSONValueReturningTypesParse(t *testing.T) {
	for _, returning := range []string{
		"signed", "unsigned", "decimal(10,2)", "float", "double", "date",
		"time(3)", "datetime(6)", "year", "char(12)",
		"char(12) character set utf8mb4", "binary(12)", "json",
	} {
		t.Run(returning, func(t *testing.T) {
			_, err := ParseOne(context.Background(),
				"select json_value(doc, '$' returning "+returning+")", 1)
			require.NoError(t, err)
		})
	}
}

func TestJSONValueDefaultLiteralAndClauseRejections(t *testing.T) {
	for _, sql := range []string{
		"select json_value(doc, '$' null on error null on empty)",
		"select json_value(doc, '$' null on empty null on empty)",
		"select json_value(doc, '$' default null on error)",
		"select json_value(doc, '$' default -null on error)",
		"select json_value(doc, '$' default doc on error)",
		"select json_value(doc, '$' default length('x') on error)",
		"select json_value(doc, '$' default ? on error)",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := ParseOne(context.Background(), sql, 1)
			require.Error(t, err)
		})
	}
}
