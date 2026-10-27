// Copyright 2021-2026 Matrix Origin
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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// TestFormatInfileExternalOptionsPreservesFilePathForPlainInfile covers issue
// #31261: an ordinary read-only local infile external table must keep its
// FILEPATH in SHOW CREATE TABLE output, otherwise the replayed DDL builds a
// table that cannot read the source file.
func TestFormatInfileExternalOptionsPreservesFilePathForPlainInfile(t *testing.T) {
	param := &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format:   tree.CSV,
		Filepath: "/data/ext rows/rows.csv",
	}}
	out := formatInfileExternalOptionsForShowCreate(param, "")
	require.Contains(t, out, "'FILEPATH'='/data/ext rows/rows.csv'")
	require.Contains(t, out, "'FORMAT'='csv'")
	// Empty optional keys must be omitted: the read-side option validator
	// rejects e.g. 'JSONDATA'='', so emitting them breaks replayed tables.
	require.NotContains(t, out, "JSONDATA")
	require.NotContains(t, out, "COMPRESSION")
}

// TestFormatInfileExternalOptionsEscapesInfilePath: the emitted FILEPATH is a
// SQL literal that reparses to the original path in the default SQL mode and
// under NO_BACKSLASH_ESCAPES.
func TestFormatInfileExternalOptionsEscapesInfilePath(t *testing.T) {
	param := &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format:   tree.CSV,
		Filepath: `/data/o'brien\dir rows.csv`,
	}}
	out := formatInfileExternalOptionsForShowCreate(param, "")
	require.Contains(t, out, `'FILEPATH'='/data/o''brien\\dir rows.csv'`)

	outNoEsc := formatInfileExternalOptionsForShowCreate(param, "NO_BACKSLASH_ESCAPES")
	require.Contains(t, outNoEsc, `'FILEPATH'='/data/o''brien\dir rows.csv'`)
}

// TestFormatInfileExternalOptionsKeepsHiveAndNonEmptyOptionals keeps the
// #24372 shape: Hive-partitioned external tables retain FILEPATH plus the
// HIVE_PARTITION* keys, and non-empty optional keys still round-trip.
func TestFormatInfileExternalOptionsKeepsHiveAndNonEmptyOptionals(t *testing.T) {
	param := &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format:            tree.CSV,
		Filepath:          "/data/test/",
		CompressType:      "auto",
		HivePartitioning:  true,
		HivePartitionCols: []string{"part_id"},
	}}
	out := formatInfileExternalOptionsForShowCreate(param, "")
	require.Contains(t, out, "'FILEPATH'='/data/test/'")
	require.Contains(t, out, "'COMPRESSION'='auto'")
	require.Contains(t, out, "'HIVE_PARTITIONING'='true'")
	require.Contains(t, out, "'HIVE_PARTITION_COLUMNS'='part_id'")

	// A JSON-format infile table must retain its non-empty JSONDATA option.
	jsonParam := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Format:   "jsonline",
			Filepath: "/data/x.jsonl",
		},
		ExParam: tree.ExParam{JsonData: "object"},
	}
	out = formatInfileExternalOptionsForShowCreate(jsonParam, "")
	require.Contains(t, out, "'FILEPATH'='/data/x.jsonl'")
	require.Contains(t, out, "'FORMAT'='jsonline'")
	require.Contains(t, out, "'JSONDATA'='object'")
}
