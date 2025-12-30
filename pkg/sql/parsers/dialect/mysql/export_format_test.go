// Copyright 2021 Matrix Origin
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

package mysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// TestExportFormat tests the FORMAT and SPLITSIZE syntax for SELECT INTO OUTFILE
func TestExportFormat(t *testing.T) {
	ctx := context.TODO()

	testCases := []struct {
		name           string
		input          string
		output         string
		expectedFormat string
		expectedSplit  uint64
		shouldFail     bool
	}{
		// Basic cases - no FORMAT (default CSV)
		{
			name:           "basic outfile without format",
			input:          "select * from t1 into outfile 'output.csv'",
			output:         "select * from t1 into outfile output.csv fields terminated by , enclosed by \" lines terminated by \n header true",
			expectedFormat: "",
			expectedSplit:  0,
		},

		// FORMAT 'csv'
		{
			name:           "outfile with format csv",
			input:          "select * from t1 into outfile 'output.csv' format 'csv'",
			output:         "select * from t1 into outfile output.csv format csv fields terminated by , enclosed by \" lines terminated by \n header true",
			expectedFormat: "csv",
			expectedSplit:  0,
		},

		// FORMAT 'jsonline'
		{
			name:           "outfile with format jsonline",
			input:          "select * from t1 into outfile 'output.jsonl' format 'jsonline'",
			output:         "select * from t1 into outfile output.jsonl format jsonline header true",
			expectedFormat: "jsonline",
			expectedSplit:  0,
		},

		// FORMAT 'parquet'
		{
			name:           "outfile with format parquet",
			input:          "select * from t1 into outfile 'output.parquet' format 'parquet'",
			output:         "select * from t1 into outfile output.parquet format parquet header true",
			expectedFormat: "parquet",
			expectedSplit:  0,
		},

		// FORMAT case insensitive
		{
			name:           "outfile with format CSV uppercase",
			input:          "select * from t1 into outfile 'output.csv' format 'CSV'",
			output:         "select * from t1 into outfile output.csv format csv fields terminated by , enclosed by \" lines terminated by \n header true",
			expectedFormat: "csv",
			expectedSplit:  0,
		},
		{
			name:           "outfile with format JSONLINE uppercase",
			input:          "select * from t1 into outfile 'output.jsonl' format 'JSONLINE'",
			output:         "select * from t1 into outfile output.jsonl format jsonline header true",
			expectedFormat: "jsonline",
			expectedSplit:  0,
		},
		{
			name:           "outfile with format Parquet mixed case",
			input:          "select * from t1 into outfile 'output.parquet' format 'Parquet'",
			output:         "select * from t1 into outfile output.parquet format parquet header true",
			expectedFormat: "parquet",
			expectedSplit:  0,
		},

		// SPLITSIZE cases
		{
			name:           "outfile with splitsize in bytes",
			input:          "select * from t1 into outfile 'data_%05d.csv' format 'csv' splitsize '1024'",
			output:         "select * from t1 into outfile data_%05d.csv format csv splitsize 1024 fields terminated by , enclosed by \" lines terminated by \n header true",
			expectedFormat: "csv",
			expectedSplit:  1024,
		},
		{
			name:           "outfile with splitsize K",
			input:          "select * from t1 into outfile 'data_%05d.csv' format 'csv' splitsize '100K'",
			output:         "select * from t1 into outfile data_%05d.csv format csv splitsize 102400 fields terminated by , enclosed by \" lines terminated by \n header true",
			expectedFormat: "csv",
			expectedSplit:  100 * 1024,
		},
		{
			name:           "outfile with splitsize M",
			input:          "select * from t1 into outfile 'data_%05d.parquet' format 'parquet' splitsize '512M'",
			output:         "select * from t1 into outfile data_%05d.parquet format parquet splitsize 536870912 header true",
			expectedFormat: "parquet",
			expectedSplit:  512 * 1024 * 1024,
		},
		{
			name:           "outfile with splitsize G",
			input:          "select * from t1 into outfile 'data_%05d.parquet' format 'parquet' splitsize '10G'",
			output:         "select * from t1 into outfile data_%05d.parquet format parquet splitsize 10737418240 header true",
			expectedFormat: "parquet",
			expectedSplit:  10 * 1024 * 1024 * 1024,
		},
		{
			name:           "outfile with splitsize lowercase",
			input:          "select * from t1 into outfile 'data_%05d.csv' format 'csv' splitsize '1g'",
			output:         "select * from t1 into outfile data_%05d.csv format csv splitsize 1073741824 fields terminated by , enclosed by \" lines terminated by \n header true",
			expectedFormat: "csv",
			expectedSplit:  1 * 1024 * 1024 * 1024,
		},

		// Combined with existing options
		{
			name:           "outfile with format and fields options",
			input:          "select * from t1 into outfile 'output.csv' format 'csv' fields terminated by '\\t' enclosed by '\\''",
			output:         "select * from t1 into outfile output.csv format csv fields terminated by \t enclosed by ' lines terminated by \n header true",
			expectedFormat: "csv",
			expectedSplit:  0,
		},
		{
			name:           "outfile with format splitsize and header",
			input:          "select * from t1 into outfile 'data_%05d.csv' format 'csv' splitsize '1G' fields terminated by ',' header 'false'",
			output:         "select * from t1 into outfile data_%05d.csv format csv splitsize 1073741824 fields terminated by , enclosed by \" lines terminated by \n header false",
			expectedFormat: "csv",
			expectedSplit:  1 * 1024 * 1024 * 1024,
		},

		// Stage URL with format
		{
			name:           "stage url with format parquet",
			input:          "select * from t1 into outfile 'stage://bucket/data_%05d.parquet' format 'parquet' splitsize '10G'",
			output:         "select * from t1 into outfile stage://bucket/data_%05d.parquet format parquet splitsize 10737418240 header true",
			expectedFormat: "parquet",
			expectedSplit:  10 * 1024 * 1024 * 1024,
		},

		// Invalid format should fail
		{
			name:       "invalid format",
			input:      "select * from t1 into outfile 'output.txt' format 'xml'",
			shouldFail: true,
		},

		// jsonline should not have fields options (they should be ignored or error)
		{
			name:           "jsonline format only",
			input:          "select * from t1 into outfile 'output.jsonl' format 'jsonline'",
			output:         "select * from t1 into outfile output.jsonl format jsonline header true",
			expectedFormat: "jsonline",
			expectedSplit:  0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ast, err := ParseOne(ctx, tc.input, 1)

			if tc.shouldFail {
				require.Error(t, err, "expected parse error for: %s", tc.input)
				return
			}

			require.NoError(t, err, "parse error for: %s", tc.input)

			selectStmt, ok := ast.(*tree.Select)
			require.True(t, ok, "expected Select statement")
			require.NotNil(t, selectStmt.Ep, "expected ExportParam")

			// Check format
			require.Equal(t, tc.expectedFormat, selectStmt.Ep.ExportFormat,
				"format mismatch for: %s", tc.input)

			// Check splitsize
			require.Equal(t, tc.expectedSplit, selectStmt.Ep.SplitSize,
				"splitsize mismatch for: %s", tc.input)

			// Check output string representation
			out := tree.String(ast, dialect.MYSQL)
			require.Equal(t, tc.output, out, "output mismatch for: %s", tc.input)
		})
	}
}

// TestExportFormatInvalidCases tests invalid FORMAT/SPLITSIZE syntax
func TestExportFormatInvalidCases(t *testing.T) {
	ctx := context.TODO()

	invalidCases := []struct {
		name  string
		input string
	}{
		{
			name:  "invalid format value",
			input: "select * from t1 into outfile 'output.txt' format 'xml'",
		},
		{
			name:  "invalid format value avro",
			input: "select * from t1 into outfile 'output.avro' format 'avro'",
		},
		{
			name:  "invalid splitsize format",
			input: "select * from t1 into outfile 'output.csv' format 'csv' splitsize 'abc'",
		},
		{
			name:  "invalid splitsize unit",
			input: "select * from t1 into outfile 'output.csv' format 'csv' splitsize '10X'",
		},
	}

	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseOne(ctx, tc.input, 1)
			require.Error(t, err, "expected parse error for: %s", tc.input)
		})
	}
}
