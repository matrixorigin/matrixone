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

package sqlexec

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/stretchr/testify/require"
)

// A rolling upgrade puts a new CN in front of both table shapes: tables it created carry
// nrow/build_ts, tables created before the columns existed do not until their tenant's v4_0_7
// migration runs. One writer has to be right on both, which is why it names its columns.
func TestMetadataInsertMatchesEitherTableShape(t *testing.T) {
	rows := []string{MetadataRow(true, "idx-1", "abc", 7, 4096, 1000, 12345)}

	wide := MetadataInsertSql("db", "__mo_index_secondary_meta", true, rows)
	require.Contains(t, wide, "`nrow`")
	require.Contains(t, wide, "`build_ts`")
	require.Contains(t, wide, "1000, 12345)")

	legacy := MetadataInsertSql("db", "__mo_index_secondary_meta", false,
		[]string{MetadataRow(false, "idx-1", "abc", 7, 4096, 1000, 12345)})
	require.NotContains(t, legacy, "`nrow`")
	require.NotContains(t, legacy, "`build_ts`")
	require.NotContains(t, legacy, "12345", "the values go with the columns, or the counts diverge")

	for _, sql := range []string{wide, legacy} {
		// NAMED, never positional: a positional INSERT is correct on exactly one of the two
		// shapes, and which one it is depends on a migration this writer cannot see.
		require.Contains(t, sql, "INSERT INTO `db`.`__mo_index_secondary_meta` (`index_id`")
		require.Equal(t, strings.Count(sql, ","), columnsIn(sql)+valuesIn(sql),
			"every named column has exactly one value: %s", sql)
	}
}

// columnsIn/valuesIn count the commas in the column list and in the single VALUES tuple, so the
// test can assert the two lists are the same length without reimplementing the formatter.
func columnsIn(sql string) int {
	head := sql[strings.Index(sql, "(")+1 : strings.Index(sql, ") VALUES")]
	return strings.Count(head, ",")
}

func valuesIn(sql string) int {
	tail := sql[strings.Index(sql, ") VALUES")+len(") VALUES"):]
	return strings.Count(tail, ",")
}

// The provenance columns are appended last and default to 0, so omitting them is a loss of
// provenance and nothing else -- 0 is already the documented "unknown" sentinel.
func TestMetadataRowOmitsProvenanceWithoutDroppingColumns(t *testing.T) {
	require.Equal(t, "('idx-1', 'abc', 7, 4096)", MetadataRow(false, "idx-1", "abc", 7, 4096, 1000, 12345))
	require.Equal(t, "('idx-1', 'abc', 7, 4096, 1000, 12345)", MetadataRow(true, "idx-1", "abc", 7, 4096, 1000, 12345))
}

// The shape question is asked of the table, and a table that has the column keeps the answer.
func TestHasProvenanceColumnsMemoizesOnlyThePositive(t *testing.T) {
	const db, tbl = "shapedb", "shapetbl"
	t.Cleanup(func() { ForgetProvenanceShape(db, tbl) })

	// No session: nothing can be asked, so the writer takes the shape that works on both.
	require.False(t, HasProvenanceColumns(nil, db, tbl, catalog.Hnsw_TblCol_Metadata_Build_Ts))
	require.False(t, HasProvenanceColumns(nil, "", "", ""))

	provenanceShape.Store(db+"."+tbl, struct{}{})
	require.True(t, HasProvenanceColumns(nil, db, tbl, catalog.Hnsw_TblCol_Metadata_Build_Ts),
		"a table never loses the columns, so the positive answer needs no re-read")

	ForgetProvenanceShape(db, tbl)
	require.False(t, HasProvenanceColumns(nil, db, tbl, catalog.Hnsw_TblCol_Metadata_Build_Ts))
}
