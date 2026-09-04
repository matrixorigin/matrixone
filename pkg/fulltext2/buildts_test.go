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

package fulltext2

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

// metadataInsert returns the metadata INSERT out of a ToInsertSqls result.
func metadataInsert(t *testing.T, sqls []string, metaTbl string) string {
	t.Helper()
	for _, sql := range sqls {
		if strings.Contains(sql, metaTbl) && strings.Contains(sql, catalog.FullText2Index_TblCol_Metadata_Build_Ts) {
			return sql
		}
	}
	t.Fatalf("no metadata insert naming %s in %v", catalog.FullText2Index_TblCol_Metadata_Build_Ts, sqls)
	return ""
}

// The caller supplies build_ts, because the two writers know different things and the tag cannot
// tell them apart -- both persist a tag=0 base.
//
// A CREATE reads the source table inside its transaction, so that SnapshotTS is exactly the
// version captured. A MERGE folds existing index segments together and never reads the source
// table, so stamping the compaction transaction's SnapshotTS would claim coverage the content
// does not have; its inputs include unversioned CDC tails, so it has no version to name.
func TestToInsertSqlsRecordsTheCallersBuildTS(t *testing.T) {
	cfg := TableConfig{DbName: "db", IndexTable: "idx", MetadataTable: "meta"}

	for _, c := range []struct {
		name    string
		buildTS int64
		want    string
	}{
		{name: "create records the source-table version", buildTS: 987654321, want: "987654321"},
		{name: "merge records unknown", buildTS: 0, want: "0"},
	} {
		t.Run(c.name, func(t *testing.T) {
			seg := &Segment{Id: "s1", N: 7}
			sqls, cleanup, err := seg.ToInsertSqls(nil, cfg, 111, int(vectorindex.Tag_ModelChunk), c.buildTS)
			require.NoError(t, err)
			if cleanup != nil {
				defer cleanup()
			}
			ins := metadataInsert(t, sqls, cfg.MetadataTable)
			require.Contains(t, ins, catalog.FullText2Index_TblCol_Metadata_Build_Ts,
				"build_ts must be named explicitly, so the insert survives a column being appended")
			require.Contains(t, ins, c.want)
			require.Contains(t, ins, catalog.FullText2Index_TblCol_Metadata_Nrow)
		})
	}
}

// The insert names its columns rather than relying on positions, which is what lets it keep
// working against a metadata table whose column order or width changes.
func TestToInsertSqlsUsesNamedColumns(t *testing.T) {
	cfg := TableConfig{DbName: "db", IndexTable: "idx", MetadataTable: "meta"}
	seg := &Segment{Id: "s1", N: 3}
	sqls, cleanup, err := seg.ToInsertSqls(nil, cfg, 1, int(vectorindex.Tag_ModelChunk), 42)
	require.NoError(t, err)
	if cleanup != nil {
		defer cleanup()
	}
	ins := metadataInsert(t, sqls, cfg.MetadataTable)
	for _, col := range []string{
		catalog.FullText2Index_TblCol_Metadata_Index_Id,
		catalog.FullText2Index_TblCol_Metadata_Nrow,
		catalog.FullText2Index_TblCol_Metadata_Build_Ts,
	} {
		require.Contains(t, ins, col)
	}
}
