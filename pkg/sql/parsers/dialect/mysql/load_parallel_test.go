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

package mysql

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestLoadParallelOptionPreservesExplicitFalse(t *testing.T) {
	for _, test := range []struct {
		name      string
		clause    string
		parallel  bool
		specified bool
	}{
		{name: "omitted", clause: "", parallel: false, specified: false},
		{name: "true", clause: " parallel 'true'", parallel: true, specified: true},
		{name: "false", clause: " parallel 'false'", parallel: false, specified: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(),
				"load data infile {'filepath'='input.parquet', 'compression'='auto'} into table t"+test.clause, 1)
			require.NoError(t, err)
			load, ok := stmt.(*tree.Load)
			require.True(t, ok)
			require.Equal(t, test.parallel, load.Param.Parallel)
			require.Equal(t, test.specified, load.Param.ParallelSpecified)

			formatted := tree.String(load, dialect.MYSQL)
			if test.specified {
				require.Contains(t, formatted, "parallel '"+test.name+"'")
				roundTrip, err := ParseOne(context.Background(), formatted, 1)
				require.NoError(t, err)
				roundTripLoad, ok := roundTrip.(*tree.Load)
				require.True(t, ok)
				require.Equal(t, test.parallel, roundTripLoad.Param.Parallel)
				require.Equal(t, test.specified, roundTripLoad.Param.ParallelSpecified)
			}

			encoded, err := json.Marshal(load.Param)
			require.NoError(t, err)
			var decoded tree.ExternParam
			require.NoError(t, json.Unmarshal(encoded, &decoded))
			require.Equal(t, test.parallel, decoded.Parallel)
			require.Equal(t, test.specified, decoded.ParallelSpecified)
		})
	}
}

func TestLoadFormatRoundTripsParallelAndStrictOptions(t *testing.T) {
	for _, test := range []struct {
		name     string
		parallel string
		strict   string
	}{
		{name: "parallel_true_strict_true", parallel: "true", strict: "true"},
		{name: "parallel_true_strict_false", parallel: "true", strict: "false"},
		{name: "parallel_false_strict_true", parallel: "false", strict: "true"},
		{name: "parallel_false_strict_false", parallel: "false", strict: "false"},
	} {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(),
				"load data infile 'input.parquet' into table t parallel '"+test.parallel+"' strict '"+test.strict+"'", 1)
			require.NoError(t, err)
			load, ok := stmt.(*tree.Load)
			require.True(t, ok)

			formatted := tree.String(load, dialect.MYSQL)
			roundTrip, err := ParseOne(context.Background(), formatted, 1)
			require.NoError(t, err)
			roundTripLoad, ok := roundTrip.(*tree.Load)
			require.True(t, ok)
			require.Equal(t, load.Param.Parallel, roundTripLoad.Param.Parallel)
			require.Equal(t, load.Param.ParallelSpecified, roundTripLoad.Param.ParallelSpecified)
			require.Equal(t, load.Param.Strict, roundTripLoad.Param.Strict)
		})
	}
}

func TestLoadFormatRoundTripsEscapedFilepath(t *testing.T) {
	const filepath = "C:\\tmp\\it's.parquet"
	stmt, err := ParseOne(context.Background(),
		"load data infile 'C:\\\\tmp\\\\it''s.parquet' into table t parallel 'false'", 1)
	require.NoError(t, err)
	load, ok := stmt.(*tree.Load)
	require.True(t, ok)
	require.Equal(t, filepath, load.Param.Filepath)

	formatted := tree.String(load, dialect.MYSQL)
	require.Contains(t, formatted, "infile 'C:\\\\tmp\\\\it''s.parquet'")
	roundTrip, err := ParseOne(context.Background(), formatted, 1)
	require.NoError(t, err)
	roundTripLoad, ok := roundTrip.(*tree.Load)
	require.True(t, ok)
	require.Equal(t, filepath, roundTripLoad.Param.Filepath)
}

func TestLoadFormatFilepathNoBackslashEscape(t *testing.T) {
	stmt, err := ParseOne(context.Background(), "load data infile 'input.parquet' into table t", 1)
	require.NoError(t, err)
	load, ok := stmt.(*tree.Load)
	require.True(t, ok)
	load.Param.Filepath = "C:\\tmp\\it's.parquet"
	ctx := tree.NewFmtCtx(dialect.MYSQL, tree.WithNoBackslashEscape())
	load.Format(ctx)
	require.Contains(t, ctx.String(), "infile 'C:\\tmp\\it''s.parquet'")
}

func TestLoadOptionDiagnostics(t *testing.T) {
	for _, option := range []struct{ name, other string }{
		{name: "parallel", other: "strict"},
		{name: "strict", other: "parallel"},
	} {
		for _, value := range []string{"invalid", ""} {
			t.Run(option.name+"/"+value, func(t *testing.T) {
				stmt, err := ParseOne(context.Background(),
					"load data infile 'input.csv' into table t "+option.name+" '"+value+"'", 1)
				require.Error(t, err)
				require.Nil(t, stmt)
				require.ErrorContains(t, err, "error "+option.name+" flag")
				require.NotContains(t, err.Error(), "error "+option.other+" flag")
			})
		}
	}
}

func TestLoadOptionValues(t *testing.T) {
	for _, test := range []struct {
		name, clause                string
		parallel, specified, strict bool
	}{
		{"omitted", "", false, false, true},
		{"parallel_true", " parallel 'true'", true, true, true},
		{"parallel_false", " parallel 'false'", false, true, true},
		{"parallel_mixed_true", " parallel 'TrUe'", true, true, true},
		{"parallel_mixed_false", " parallel 'FaLsE'", false, true, true},
		{"strict_true", " strict 'true'", false, false, true},
		{"strict_false", " strict 'false'", false, false, false},
		{"strict_mixed_true", " strict 'TrUe'", false, false, true},
		{"strict_mixed_false", " strict 'FaLsE'", false, false, false},
		{"both_false", " parallel 'false' strict 'false'", false, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), "load data infile 'input.csv' into table t"+test.clause, 1)
			require.NoError(t, err)
			require.NotNil(t, stmt)
			defer stmt.Free()
			load, ok := stmt.(*tree.Load)
			require.True(t, ok)
			require.Equal(t, test.parallel, load.Param.Parallel)
			require.Equal(t, test.specified, load.Param.ParallelSpecified)
			require.Equal(t, test.strict, load.Param.Strict)
		})
	}
}

func TestLoadOptionDiagnosticsWithAdjacentOptions(t *testing.T) {
	for _, test := range []struct {
		name, clause, expected, other string
	}{
		{"invalid_parallel_valid_strict", "parallel ' true ' strict 'false'", "parallel", "strict"},
		{"valid_parallel_invalid_strict", "parallel 'true' strict 'falsex'", "strict", "parallel"},
		{"both_invalid", "parallel 'falsex' strict 'truex'", "parallel", "strict"},
	} {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), "load data infile 'input.csv' into table t "+test.clause, 1)
			require.Error(t, err)
			require.Nil(t, stmt)
			require.ErrorContains(t, err, "error "+test.expected+" flag")
			require.NotContains(t, err.Error(), "error "+test.other+" flag")

			// A failed parse must not poison the next parse in the same process.
			stmt, err = ParseOne(context.Background(), "load data infile 'input.csv' into table t parallel 'false' strict 'true'", 1)
			require.NoError(t, err)
			require.NotNil(t, stmt)
			defer stmt.Free()
			load, ok := stmt.(*tree.Load)
			require.True(t, ok)
			require.False(t, load.Param.Parallel)
			require.True(t, load.Param.ParallelSpecified)
			require.True(t, load.Param.Strict)
		})
	}
}
