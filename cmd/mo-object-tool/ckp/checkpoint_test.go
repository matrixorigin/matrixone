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

package ckp

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/tools/checkpointtool"
	"github.com/matrixorigin/matrixone/pkg/tools/toolfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeCreateTableDDLName(t *testing.T) {
	table := checkpointtool.TableCatalogEntry{
		DatabaseName: "compat_ckp",
		TableName:    "employees",
	}

	tests := []struct {
		name string
		ddl  string
		want string
	}{
		{
			name: "plain table name",
			ddl:  "CREATE TABLE employees_copy_123 (id INT)",
			want: "CREATE TABLE `compat_ckp`.`employees` (id INT)",
		},
		{
			name: "qualified table name",
			ddl:  "CREATE TABLE `compat_ckp`.`employees_copy_123` (id INT)",
			want: "CREATE TABLE `compat_ckp`.`employees` (id INT)",
		},
		{
			name: "if not exists",
			ddl:  "CREATE TABLE IF NOT EXISTS old_name (id INT)",
			want: "CREATE TABLE IF NOT EXISTS `compat_ckp`.`employees` (id INT)",
		},
		{
			name: "external table",
			ddl:  "CREATE EXTERNAL TABLE old_name (id INT) INFILE {'filepath'='/tmp/data.csv','format'='csv'}",
			want: "CREATE EXTERNAL TABLE `compat_ckp`.`employees` (id INT) INFILE {'filepath'='/tmp/data.csv','format'='csv'}",
		},
		{
			name: "escaped target name",
			ddl:  "CREATE TABLE old_name (id INT)",
			want: "CREATE TABLE `compat``ckp`.`employees` (id INT)",
		},
	}

	tests[4].want = "CREATE TABLE `compat``ckp`.`employees` (id INT)"
	tests[4].ddl = "CREATE TABLE old_name (id INT)"
	tests[4].name = "escaped database name"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "escaped database name" {
				table.DatabaseName = "compat`ckp"
			} else {
				table.DatabaseName = "compat_ckp"
			}
			assert.Equal(t, tt.want, normalizeCreateTableDDLName(tt.ddl, table))
		})
	}
}

func TestRestoreCreateTableDDLUsesExternalCreateSQL(t *testing.T) {
	table := checkpointtool.TableCatalogEntry{
		DatabaseName: "ckp_external",
		TableName:    "ext_csv_local",
		RelKind:      "e",
	}
	dumpData := &checkpointtool.TableDumpData{
		Schema: &checkpointtool.TableSchema{
			TableName: "stale_name",
			CreateSQL: "CREATE EXTERNAL TABLE stale_name (id INT) INFILE {'filepath'='/tmp/ext.csv','format'='csv'}",
			Columns:   []checkpointtool.TableColumn{{Name: "id", SQLType: "INT", Position: 1}},
		},
	}

	ddl, err := restoreCreateTableDDL(context.Background(), nil, table, dumpData, types.TS{})
	require.NoError(t, err)
	assert.Equal(t, "CREATE EXTERNAL TABLE `ckp_external`.`ext_csv_local` (id INT) INFILE {'filepath'='/tmp/ext.csv','format'='csv'}", ddl)
}

func TestShouldWriteLoadDataSkipsExternalRelations(t *testing.T) {
	assert.False(t, shouldWriteLoadData(true, checkpointtool.TableCatalogEntry{RelKind: "e"}))
	assert.False(t, shouldWriteLoadData(true, checkpointtool.TableCatalogEntry{RelKind: "external"}))
	assert.True(t, shouldWriteLoadData(true, checkpointtool.TableCatalogEntry{RelKind: "r"}))
	assert.False(t, shouldWriteLoadData(false, checkpointtool.TableCatalogEntry{RelKind: "r"}))
}

func TestFilterExistingIndexDDLs(t *testing.T) {
	createDDL := "CREATE TABLE `items_gist` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `embedding` vecf32(960) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`),\n" +
		"  KEY `ivf_2000` USING ivfflat(`embedding`) lists=2000 op_type 'vector_l2_ops'\n" +
		")"
	indexDDLs := []string{
		"ALTER TABLE `items_gist` ADD KEY `ivf_2000` USING ivfflat(`embedding`) lists = 2000  op_type 'vector_l2_ops' ;",
		"ALTER TABLE `items_gist` ADD KEY `new_idx`(`id`);",
	}

	assert.Equal(t, []string{"ALTER TABLE `items_gist` ADD KEY `new_idx`(`id`);"}, filterExistingIndexDDLs(createDDL, indexDDLs))
}

func TestMergeCreateTableIndexDDLs(t *testing.T) {
	createDDL := "CREATE TABLE `ann`.`items_gist` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `embedding` vecf32(960) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		")"
	indexDDLs := []string{
		"ALTER TABLE `items_gist` ADD KEY `ivf_2000` USING ivfflat(`embedding`) lists = 2000  op_type 'vector_l2_ops' ;",
	}
	want := "CREATE TABLE `ann`.`items_gist` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `embedding` vecf32(960) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`),\n" +
		"  KEY `ivf_2000` USING ivfflat(`embedding`) lists = 2000  op_type 'vector_l2_ops'\n" +
		")"

	got, err := mergeCreateTableIndexDDLs(createDDL, indexDDLs)
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestMergeCreateTableIndexDDLsSingleLine(t *testing.T) {
	createDDL := "CREATE TABLE `ann`.`items_sift` (id int primary key, embedding vecf32(128))"
	indexDDLs := []string{
		"ALTER TABLE `items_sift` ADD KEY `ivf_500` USING ivfflat(`embedding`) lists = 500  op_type 'vector_l2_ops' ;",
	}
	want := "CREATE TABLE `ann`.`items_sift` (id int primary key, embedding vecf32(128), KEY `ivf_500` USING ivfflat(`embedding`) lists = 500  op_type 'vector_l2_ops')"

	got, err := mergeCreateTableIndexDDLs(createDDL, indexDDLs)
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestMergeCreateTableIndexDDLsSkipsExistingIndex(t *testing.T) {
	createDDL := "CREATE TABLE `items_gist` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `embedding` vecf32(960) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`),\n" +
		"  KEY `ivf_2000` USING ivfflat(`embedding`) lists=2000 op_type 'vector_l2_ops'\n" +
		")"
	indexDDLs := []string{
		"ALTER TABLE `items_gist` ADD KEY `ivf_2000` USING ivfflat(`embedding`) lists = 2000  op_type 'vector_l2_ops' ;",
	}

	got, err := mergeCreateTableIndexDDLs(createDDL, indexDDLs)
	assert.NoError(t, err)
	assert.Equal(t, createDDL, got)
}

func TestMergeCreateTableIndexDDLsWithConstraintsAndComments(t *testing.T) {
	createDDL := "CREATE TABLE `ckp_constraints`.`parent` (\n" +
		"  `id` INT NOT NULL PRIMARY KEY,\n" +
		"  `code` VARCHAR(20) NOT NULL UNIQUE,\n" +
		"  `note` VARCHAR(100) DEFAULT 'parent-default' COMMENT 'parent note'\n" +
		") COMMENT='parent table comment'"
	indexDDLs := []string{
		"ALTER TABLE `parent` ADD KEY `idx_parent_note`(`note`);",
	}

	got, err := mergeCreateTableIndexDDLs(createDDL, indexDDLs)
	require.NoError(t, err)
	assert.Contains(t, got, "KEY `idx_parent_note`(`note`)")
	assert.Contains(t, got, ") COMMENT='parent table comment'")
}

func TestMergeCreateTableIndexDDLsWithAutoIncrementAndClusterTable(t *testing.T) {
	createDDL := "CREATE CLUSTER TABLE `ckp_constraints`.`t_auto_inc` (\n" +
		"  `id` BIGINT NOT NULL AUTO_INCREMENT,\n" +
		"  `note` VARCHAR(64) COMMENT 'note (with parens)',\n" +
		"  PRIMARY KEY (`id`)\n" +
		") COMMENT='auto increment table'"
	indexDDLs := []string{
		"ALTER TABLE `t_auto_inc` ADD KEY `idx_auto_inc_note`(`note`);",
	}

	got, err := mergeCreateTableIndexDDLs(createDDL, indexDDLs)
	require.NoError(t, err)
	assert.Contains(t, got, "KEY `idx_auto_inc_note`(`note`)")
	assert.Contains(t, got, "COMMENT='auto increment table'")
}

func TestMergeCreateTableIndexDDLsFallsBackToSeparateAlter(t *testing.T) {
	createDDL := "CREATE TABLE `parent` LIKE `parent_template`"
	indexDDLs := []string{
		"ALTER TABLE `parent` ADD KEY `idx_parent_note`(`note`);",
	}

	got, err := mergeCreateTableIndexDDLs(createDDL, indexDDLs)
	require.NoError(t, err)
	assert.Equal(t, "CREATE TABLE `parent` LIKE `parent_template`;\nALTER TABLE `parent` ADD KEY `idx_parent_note`(`note`)", got)
}

func TestNormalizeCreateTableDDLNameWithCreateTableModifiers(t *testing.T) {
	table := checkpointtool.TableCatalogEntry{
		DatabaseName: "ckp_constraints",
		TableName:    "parent",
	}

	got := normalizeCreateTableDDLName("CREATE CLUSTER TABLE old_parent (id INT)", table)
	assert.Equal(t, "CREATE CLUSTER TABLE `ckp_constraints`.`parent` (id INT)", got)
}

func TestCleanObjectPath(t *testing.T) {
	assert.Equal(t, "dump/account_1/t.csv", cleanObjectPath("dump/account_1/t.csv"))
	assert.Equal(t, "tmp/dump/t.csv", cleanObjectPath("/tmp/dump/t.csv"))
	assert.Equal(t, "dump/t.csv", cleanObjectPath("dump//nested/../t.csv"))
}

func TestLoadDataPathResolverLocal(t *testing.T) {
	table := checkpointtool.TableCatalogEntry{
		AccountID:  0,
		DatabaseID: 272577,
		TableID:    272578,
		TableName:  "bmsql_config",
	}

	resolver, err := newLoadDataPathResolver(toolfs.StorageOptions{})
	require.NoError(t, err)
	loadSource := resolver.loadDataSource("bmsql_config", table)
	assert.True(t, strings.HasPrefix(loadSource, "LOAD DATA INFILE '"))
	assert.Contains(t, loadSource, "/bmsql_config/account_0/db_272577/bmsql_config_272578.csv'")
	assert.True(t, filepath.IsAbs(strings.TrimSuffix(strings.TrimPrefix(loadSource, "LOAD DATA INFILE '"), "'")))
}

func TestLoadDataPathResolverS3(t *testing.T) {
	table := checkpointtool.TableCatalogEntry{
		AccountID:  0,
		DatabaseID: 272577,
		TableID:    272578,
		TableName:  "bmsql_config",
	}

	resolver, err := newLoadDataPathResolver(toolfs.StorageOptions{
		S3: "bucket=mo-nightly-gz-1308875761,endpoint=https://cos.ap-guangzhou.myqcloud.com,region=ap-guangzhou," +
			"key-prefix=/ckp-dump-test/tpcc_100_20260612_174916/bmsql_config/,key-id=xxx,key-secret=yyy",
		Backend: "S3",
	})
	require.NoError(t, err)
	assert.Equal(t,
		"LOAD DATA URL s3option{'bucket'='mo-nightly-gz-1308875761', 'filepath'='ckp-dump-test/tpcc_100_20260612_174916/bmsql_config/bmsql_config/account_0/db_272577/bmsql_config_272578.csv', 'endpoint'='https://cos.ap-guangzhou.myqcloud.com', 'region'='ap-guangzhou', 'access_key_id'='xxx', 'secret_access_key'='yyy'}",
		resolver.loadDataSource("bmsql_config", table),
	)
}
