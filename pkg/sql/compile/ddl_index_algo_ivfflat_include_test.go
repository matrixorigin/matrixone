// Copyright 2022 Matrix Origin
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

package compile

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

func TestBuildIvfEntriesInsertSQL_IncludesIncludeColumns(t *testing.T) {
	indexDef := &plan.IndexDef{
		IndexTableName:  "idx_entries_tbl",
		Parts:           []string{"embedding"},
		IndexAlgoParams: `{"lists":"2","op_type":"vector_l2_ops"}`,
		IncludedColumns: []string{"title", "category"},
	}
	originalTableDef := &plan.TableDef{
		Name: "src_tbl",
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"id"},
			PkeyColName: "id",
		},
	}

	sql, err := buildIvfEntriesInsertSQL(indexDef, "test_db", originalTableDef, "meta_tbl", "centroids_tbl")
	require.NoError(t, err)

	require.Contains(t, sql, "INSERT INTO `test_db`.`idx_entries_tbl` (`"+
		catalog.SystemSI_IVFFLAT_TblCol_Entries_version+"`, `"+
		catalog.SystemSI_IVFFLAT_TblCol_Entries_id+"`, `"+
		catalog.SystemSI_IVFFLAT_TblCol_Entries_pk+"`, `"+
		catalog.SystemSI_IVFFLAT_TblCol_Entries_entry+"`, `"+
		catalog.SystemSI_IVFFLAT_IncludeColPrefix+"title`, `"+
		catalog.SystemSI_IVFFLAT_IncludeColPrefix+"category`)")
	require.Contains(t, sql, "FROM `test_db`.`src_tbl` AS `src` CENTROIDX ('vector_l2_ops')")
	require.Contains(t, sql, "SELECT `centroids_cur`.`"+catalog.SystemSI_IVFFLAT_TblCol_Centroids_version+"`, `centroids_cur`.`"+catalog.SystemSI_IVFFLAT_TblCol_Centroids_id+"`, `src`.`id`, `src`.`embedding`, `src`.`title`, `src`.`category`")
}

func TestBuildIvfEntriesInsertSQL_UsesSerialForCompositePrimaryKey(t *testing.T) {
	indexDef := &plan.IndexDef{
		IndexTableName:  "idx_entries_tbl",
		Parts:           []string{"embedding"},
		IndexAlgoParams: `{"lists":"2","op_type":"vector_l2_ops"}`,
	}
	originalTableDef := &plan.TableDef{
		Name: "src_tbl",
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"tenant_id", "doc_id"},
			PkeyColName: catalog.CPrimaryKeyColName,
		},
	}

	sql, err := buildIvfEntriesInsertSQL(indexDef, "test_db", originalTableDef, "meta_tbl", "centroids_tbl")
	require.NoError(t, err)

	require.Contains(t, sql, "serial(`src`.`tenant_id`,`src`.`doc_id`)")
}

func TestBuildIvfCreateSQL_UsesAlgorithmParamsOnly(t *testing.T) {
	cfg := vectorindex.IndexTableConfig{
		DbName:        "test_db",
		SrcTable:      "src_tbl",
		MetadataTable: "meta_tbl",
		IndexTable:    "idx_tbl",
		PKey:          "src.id",
		KeyPart:       "embedding",
	}

	sql, err := buildIvfCreateSQL(
		`{"lists":"2","op_type":"vector_l2_ops"}`,
		cfg,
	)
	require.NoError(t, err)

	require.Contains(t, sql, `ivf_create('{"lists":"2","op_type":"vector_l2_ops"}',`)
	require.Contains(t, sql, `{"db":"test_db","src":"src_tbl","metadata":"meta_tbl"`)
	require.Contains(t, sql, `"index":"idx_tbl","pkey":"src.id","part":"embedding"`)
}
