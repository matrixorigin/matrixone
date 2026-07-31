// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func TestOperationalSQLTemplatesParse(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	require.True(t, ok)
	root := filepath.Dir(source)
	for _, name := range []string{"incremental_ingest.sql", "bounded_backfill.sql", "archive_reset_gate.sql"} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(root, "sql", name))
			require.NoError(t, err)
			require.NoError(t, parseSQLClientTemplate(context.Background(), string(data)))
		})
	}
}

func TestIncrementalTemplateAlignsReplayBoundsToResultGrain(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	require.True(t, ok)
	data, err := os.ReadFile(filepath.Join(filepath.Dir(source), "sql", "incremental_ingest.sql"))
	require.NoError(t, err)
	template := string(data)
	require.Equal(t, 2, strings.Count(template, "''%Y-%m-%d %H:%i:00.000''"),
		"both replay bounds must be minute-aligned before REPLACE recomputes a bucket")
	require.Contains(t, template, "temperature_celsius float")
	require.Contains(t, template, "''mongodb-aggregate-v1-exact|''")
	require.NotContains(t, strings.ToLower(template), "concat_ws",
		"a separator-only encoding aliases NULL/empty values and delimiter-bearing strings")
}

func parseSQLClientTemplate(ctx context.Context, sql string) error {
	prefix, delimited, found := strings.Cut(sql, "delimiter //")
	if !found {
		_, err := mysql.Parse(ctx, sql, 1)
		return err
	}
	if statements, err := mysql.Parse(ctx, prefix, 1); err != nil || len(statements) == 0 {
		return err
	}
	compound, suffix, found := strings.Cut(delimited, "delimiter ;")
	if !found {
		return errors.New("unterminated delimiter block")
	}
	compound = strings.TrimSpace(compound)
	compound = strings.TrimSuffix(compound, "//")
	if _, err := mysql.ParseOne(ctx, compound, 1); err != nil {
		return err
	}
	_, err := mysql.Parse(ctx, suffix, 1)
	return err
}
