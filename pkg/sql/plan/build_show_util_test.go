// Copyright 2024 Matrix Origin
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
	"encoding/json"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func Test_buildTestShowCreateTable(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "test1",
			sql: `CREATE TABLE employees (
				id INT NOT NULL,
				fname VARCHAR(30),
				lname VARCHAR(30),
				hired DATE NOT NULL DEFAULT '1970-01-01',
				separated DATE NOT NULL DEFAULT '9999-12-31',
				job_code INT NOT NULL,
				store_id INT NOT NULL
			)`,
			want: "CREATE TABLE `employees` (\n  `id` int NOT NULL,\n  `fname` varchar(30) DEFAULT NULL,\n  `lname` varchar(30) DEFAULT NULL,\n  `hired` date NOT NULL DEFAULT '1970-01-01',\n  `separated` date NOT NULL DEFAULT '9999-12-31',\n  `job_code` int NOT NULL,\n  `store_id` int NOT NULL\n)",
		},
		{
			name: "test2",
			sql: `CREATE TABLE t_time (
				    id INT AUTO_INCREMENT PRIMARY KEY,
				    date_column DATE, 
				    time_column TIME(3),
				    datetime_column DATETIME(6), 
				    timestamp_column TIMESTAMP(6),
				    timestamp_column2 TIMESTAMP
				);`,
			want: "CREATE TABLE `t_time` (\n  `id` int NOT NULL AUTO_INCREMENT,\n  `date_column` date DEFAULT NULL,\n  `time_column` time(3) DEFAULT NULL,\n  `datetime_column` datetime(6) DEFAULT NULL,\n  `timestamp_column` timestamp(6) NULL DEFAULT NULL,\n  `timestamp_column2` timestamp NULL DEFAULT NULL,\n  PRIMARY KEY (`id`)\n)",
		},
		{
			name: "test3",
			sql: `CREATE TABLE t_time (
				id INT AUTO_INCREMENT PRIMARY KEY,
				date_column DATE,
				time_column TIME(3) DEFAULT NULL,
				datetime_column DATETIME(6)  DEFAULT NULL,
				timestamp_column TIMESTAMP(6)  NULL DEFAULT NULL,
				timestamp_column2 TIMESTAMP  NULL DEFAULT NULL
				)`,
			want: "CREATE TABLE `t_time` (\n  `id` int NOT NULL AUTO_INCREMENT,\n  `date_column` date DEFAULT NULL,\n  `time_column` time(3) DEFAULT NULL,\n  `datetime_column` datetime(6) DEFAULT NULL,\n  `timestamp_column` timestamp(6) NULL DEFAULT NULL,\n  `timestamp_column2` timestamp NULL DEFAULT NULL,\n  PRIMARY KEY (`id`)\n)",
		},
		{
			name: "test4",
			sql: `CREATE TABLE t_log (
				LOG_ID bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
				ROUND_ID bigint(20) UNSIGNED NOT NULL,
				USER_ID int(10) UNSIGNED NOT NULL,
				USER_IP int(10) UNSIGNED DEFAULT NULL,
				END_TIME datetime NOT NULL,
				USER_TYPE int(11) DEFAULT NULL,
				APP_ID int(11) DEFAULT NULL,
				PRIMARY KEY (LOG_ID,END_TIME),
				KEY IDX_EndTime (END_TIME),
				KEY IDX_RoundId (ROUND_ID),
				KEY IDX_UserId_EndTime (USER_ID,END_TIME)
				)`,
			want: "CREATE TABLE `t_log` (\n  `LOG_ID` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `ROUND_ID` bigint unsigned NOT NULL,\n  `USER_ID` int unsigned NOT NULL,\n  `USER_IP` int unsigned DEFAULT NULL,\n  `END_TIME` datetime NOT NULL,\n  `USER_TYPE` int DEFAULT NULL,\n  `APP_ID` int DEFAULT NULL,\n  PRIMARY KEY (`LOG_ID`,`END_TIME`),\n  KEY `idx_endtime` (`END_TIME`),\n  KEY `idx_roundid` (`ROUND_ID`),\n  KEY `idx_userid_endtime` (`USER_ID`,`END_TIME`)\n)",
		},
		{
			name: "test5",
			sql: `CREATE TABLE example_table (
				id INT NOT NULL,
				name VARCHAR(255) NOT NULL DEFAULT 'default_name',
				email VARCHAR(255),
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
				PRIMARY KEY (id),
				UNIQUE KEY uk1 (name),
				UNIQUE KEY uk2 (email)
				);`,
			want: "CREATE TABLE `example_table` (\n  `id` int NOT NULL,\n  `name` varchar(255) NOT NULL DEFAULT 'default_name',\n  `email` varchar(255) DEFAULT NULL,\n  `created_at` timestamp DEFAULT CURRENT_TIMESTAMP(),\n  `updated_at` timestamp DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP(),\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `uk1` (`name`),\n  UNIQUE KEY `uk2` (`email`)\n)",
		},
		{
			name: "test6",
			sql: `create table t (
				a timestamp not null default current_timestamp,
				b timestamp(3) default current_timestamp(3),
				c datetime default current_timestamp,
				d datetime(4) default current_timestamp(4),
				e varchar(20) default 'cUrrent_tImestamp',
				f datetime(2) default current_timestamp(2) on update current_timestamp(2),
				g timestamp(2) default current_timestamp(2) on update current_timestamp(2),
				h date default '2024-01-01'
				)`,
			want: "CREATE TABLE `t` (\n  `a` timestamp NOT NULL DEFAULT current_timestamp(),\n  `b` timestamp(3) DEFAULT current_timestamp(3),\n  `c` datetime DEFAULT current_timestamp(),\n  `d` datetime(4) DEFAULT current_timestamp(4),\n  `e` varchar(20) DEFAULT 'cUrrent_tImestamp',\n  `f` datetime(2) DEFAULT current_timestamp(2) ON UPDATE current_timestamp(2),\n  `g` timestamp(2) DEFAULT current_timestamp(2) ON UPDATE current_timestamp(2),\n  `h` date DEFAULT '2024-01-01'\n)",
		},
		{
			name: "test7",
			sql: `CREATE TABLE src (id bigint NOT NULL,
				json1 json DEFAULT NULL,
				json2 json DEFAULT NULL,
				PRIMARY KEY (id),
				FULLTEXT(json1) WITH PARSER json,
				FULLTEXT(json1,json2) WITH PARSER json)`,
			want: "CREATE TABLE `src` (\n  `id` bigint NOT NULL,\n  `json1` json DEFAULT NULL,\n  `json2` json DEFAULT NULL,\n  PRIMARY KEY (`id`),\n FULLTEXT (`json1`) WITH PARSER json,\n FULLTEXT (`json1`,`json2`) WITH PARSER json\n)",
		},
		{
			name: "test8",
			sql: `CREATE TABLE src (id bigint NOT NULL,
                                json1 json DEFAULT NULL,
                                json2 json DEFAULT NULL,
                                PRIMARY KEY (id),
                                FULLTEXT idx01(json1) WITH PARSER json ASYNC,
                                FULLTEXT idx02(json1,json2) WITH PARSER json)`,
			want: "CREATE TABLE `src` (\n  `id` bigint NOT NULL,\n  `json1` json DEFAULT NULL,\n  `json2` json DEFAULT NULL,\n  PRIMARY KEY (`id`),\n FULLTEXT `idx01`(`json1`) WITH PARSER json ASYNC,\n FULLTEXT `idx02`(`json1`,`json2`) WITH PARSER json\n)",
		},
		{
			name: "array column metadata",
			sql: `CREATE TABLE vec_json_case (
				doc_id BIGINT PRIMARY KEY,
				embedding VECF32(3),
				payload JSON,
				tags ARRAY(VARCHAR(20))
			)`,
			want: "CREATE TABLE `vec_json_case` (\n  `doc_id` bigint NOT NULL,\n  `embedding` vecf32(3) DEFAULT NULL,\n  `payload` json DEFAULT NULL,\n  `tags` array(varchar(20)) DEFAULT NULL,\n  PRIMARY KEY (`doc_id`)\n)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildTestShowCreateTable(tt.sql)
			if err != nil {
				t.Fatalf("test name:%v, err: %+v, sql=%v", tt.name, err, tt.sql)
			}

			if tt.want != got {
				t.Errorf("buildShow failed. \nExpected/Got:\n%s\n%s", tt.want, got)
			}
		})
	}
}

func Test_buildShowCreateTableSpatialIndex(t *testing.T) {
	mock := NewMockOptimizer(false)
	tableDef, err := buildTestCreateTableStmt(mock, `CREATE TABLE spatial_src (
		id INT NOT NULL,
		g POINT NOT NULL,
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err)

	tableDef.Indexes = append(tableDef.Indexes, &plan.IndexDef{
		IndexName: "idx_g",
		Parts:     []string{"g"},
		IndexAlgo: catalog.MoIndexRTreeAlgo.ToString(),
	})

	var snapshot *plan.Snapshot
	got, _, err := ConstructCreateTableSQL(&mock.ctxt, tableDef, snapshot, false, nil)
	require.NoError(t, err)
	require.Equal(t, "CREATE TABLE `spatial_src` (\n  `id` int NOT NULL,\n  `g` point NOT NULL,\n  PRIMARY KEY (`id`),\n  SPATIAL KEY `idx_g` (`g`)\n)", got)
}

func TestShowCreateTablePreservesIndexPrefixLengths(t *testing.T) {
	mock := NewMockOptimizer(false)
	tableDef, err := buildTestCreateTableStmt(mock, `CREATE TABLE prefix_show_src (
		id INT PRIMARY KEY,
		name VARCHAR(191),
		t TEXT,
		b BLOB,
		KEY idx_t(t(100)),
		UNIQUE KEY uq_b(b(20)),
		KEY idx_mix(name, t(30))
	)`)
	require.NoError(t, err)

	var snapshot *plan.Snapshot
	got, _, err := ConstructCreateTableSQL(&mock.ctxt, tableDef, snapshot, false, nil)
	require.NoError(t, err)
	require.Contains(t, got, "KEY `idx_t` (`t`(100))")
	require.Contains(t, got, "UNIQUE KEY `uq_b` (`b`(20))")
	require.Contains(t, got, "KEY `idx_mix` (`name`,`t`(30))")
}

func Test_ShowCreateTableUsesIncludedColumnsFromIndexDef(t *testing.T) {
	mock := NewMockOptimizer(false)
	tableDef, err := buildTestCreateTableStmt(mock, `CREATE TABLE vector_src (
		id INT NOT NULL,
		embedding VECF32(3),
		title VARCHAR(20),
		category INT,
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err)

	tableDef.Indexes = append(tableDef.Indexes, &plan.IndexDef{
		IndexName:       "idx_vec",
		Parts:           []string{"embedding"},
		IndexAlgo:       catalog.MoIndexIvfFlatAlgo.ToString(),
		IndexAlgoParams: `{"lists":"2","op_type":"vector_l2_ops"}`,
		IncludedColumns: []string{"title", "category"},
	})

	got, _, err := ConstructCreateTableSQL(&mock.ctxt, tableDef, nil, false, nil)
	require.NoError(t, err)
	require.Contains(t, got, "KEY `idx_vec` USING ivfflat (`embedding`) lists = 2  op_type 'vector_l2_ops'  INCLUDE (`title`, `category`)")
}

func Test_ShowCreateTableQuotesIncludedColumns(t *testing.T) {
	mock := NewMockOptimizer(false)
	tableDef, err := buildTestCreateTableStmt(mock, `CREATE TABLE vector_src_reserved (
		id INT NOT NULL,
		embedding VECF32(3),
		status INT,
		PRIMARY KEY (id)
	)`)
	require.NoError(t, err)

	tableDef.Indexes = append(tableDef.Indexes, &plan.IndexDef{
		IndexName:       "idx_vec_reserved",
		Parts:           []string{"embedding"},
		IndexAlgo:       catalog.MoIndexIvfFlatAlgo.ToString(),
		IndexAlgoParams: `{"lists":"2","op_type":"vector_l2_ops"}`,
		IncludedColumns: []string{"status"},
	})

	got, _, err := ConstructCreateTableSQL(&mock.ctxt, tableDef, nil, false, nil)
	require.NoError(t, err)
	require.Contains(t, got, "INCLUDE (`status`)")
}

func Test_ShowCreateTableRendersSingleIncludeWhenAlgoParamsAlsoCarryIncludeColumns(t *testing.T) {
	for _, tc := range []struct {
		name string
		algo string
	}{
		{name: "cagra", algo: catalog.MoIndexCagraAlgo.ToString()},
		{name: "ivfpq", algo: catalog.MoIndexIvfpqAlgo.ToString()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			tableDef, err := buildTestCreateTableStmt(mock, `CREATE TABLE vector_src_gpu (
				id INT NOT NULL,
				embedding VECF32(3),
				price INT,
				category BIGINT,
				PRIMARY KEY (id)
			)`)
			require.NoError(t, err)

			tableDef.Indexes = append(tableDef.Indexes, &plan.IndexDef{
				IndexName:       "idx_vec_gpu",
				Parts:           []string{"embedding"},
				IndexAlgo:       tc.algo,
				IndexAlgoParams: `{"op_type":"vector_l2_ops","included_columns":"price,category"}`,
				IncludedColumns: []string{"price", "category"},
			})

			got, _, err := ConstructCreateTableSQL(&mock.ctxt, tableDef, nil, false, nil)
			require.NoError(t, err)
			require.Equal(t, 1, strings.Count(got, "INCLUDE"))
			require.Contains(t, got, "INCLUDE (`price`, `category`)")
		})
	}
}

func Test_ShowCreateTableUsesStoredDDLForChecks(t *testing.T) {
	const sql = `CREATE TABLE t_numeric_types (
		id BIGINT NOT NULL AUTO_INCREMENT,
		c_age INT NULL,
		c_score DECIMAL(5,2) NULL,
		PRIMARY KEY (id),
		CONSTRAINT chk_age CHECK (c_age IS NULL OR (c_age >= 0 AND c_age <= 200)),
		CONSTRAINT chk_score CHECK (c_score IS NULL OR (c_score >= 0 AND c_score <= 100))
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

	mock := NewMockOptimizer(false)
	tableDef, err := buildTestCreateTableStmt(mock, sql)
	if err != nil {
		t.Fatalf("build create table failed: %+v", err)
	}
	tableDef.Createsql = sql

	showSQL, _, err := ConstructCreateTableSQL(&mock.ctxt, tableDef, nil, false, nil)
	if err != nil {
		t.Fatalf("construct show create failed: %+v", err)
	}
	if !strings.Contains(showSQL, "CONSTRAINT chk_age CHECK (c_age IS NULL OR (c_age >= 0 AND c_age <= 200))") {
		t.Fatalf("expected chk_age check constraint in show create output: %s", showSQL)
	}
	if !strings.Contains(showSQL, "CONSTRAINT chk_score CHECK (c_score IS NULL OR (c_score >= 0 AND c_score <= 100))") {
		t.Fatalf("expected chk_score check constraint in show create output: %s", showSQL)
	}
	if !strings.Contains(showSQL, "PRIMARY KEY (`id`)") {
		t.Fatalf("expected canonical primary key output to be preserved: %s", showSQL)
	}
}

func Test_extractTopLevelCheckDefs(t *testing.T) {
	tableDef := &plan.TableDef{
		TableType: catalog.SystemOrdinaryRel,
		Createsql: "CREATE TABLE t1 (id int, note varchar(20) comment 'constraint check', CONSTRAINT chk_id CHECK(id > 0), CHECK(score>0), CONSTRAINT fk1 FOREIGN KEY (id) REFERENCES t2(id)) ENGINE=InnoDB",
	}

	checks := extractTopLevelCheckDefs(tableDef)
	if len(checks) != 2 {
		t.Fatalf("expected 2 top-level check defs, got %d: %#v", len(checks), checks)
	}
	if checks[0] != "CONSTRAINT chk_id CHECK(id > 0)" {
		t.Fatalf("unexpected first check def: %s", checks[0])
	}
	if checks[1] != "CHECK(score>0)" {
		t.Fatalf("unexpected second check def: %s", checks[1])
	}
}

func Test_SingleShowCreateTable(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "test7",
			sql: `CREATE TABLE example_table (
				id INT NOT NULL,
				name VARCHAR(255) NOT NULL DEFAULT 'default_name',
				email VARCHAR(255),
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
				PRIMARY KEY (id),
				UNIQUE KEY uk1 (name),
				UNIQUE KEY uk2 (email)
				);`,
			want: "CREATE TABLE `example_table` (\n  `id` int NOT NULL,\n  `name` varchar(255) NOT NULL DEFAULT 'default_name',\n  `email` varchar(255) DEFAULT NULL,\n  `created_at` timestamp DEFAULT CURRENT_TIMESTAMP(),\n  `updated_at` timestamp DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP(),\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `uk1` (`name`),\n  UNIQUE KEY `uk2` (`email`)\n)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildTestShowCreateTable(tt.sql)
			if err != nil {
				t.Fatalf("test name:%v, err: %+v, sql=%v", tt.name, err, tt.sql)
			}

			if tt.want != got {
				t.Errorf("buildShow failed. \nExpected/Got:\n%s\n%s", tt.want, got)
			}
		})
	}
}

func buildTestCreateTableStmt(opt Optimizer, sql string) (*TableDef, error) {
	statements, err := mysql.Parse(opt.CurrentContext().GetContext(), sql, 1)
	if err != nil {
		return nil, err
	}
	// this sql always return single statement
	context := opt.CurrentContext()
	ddlPlan, err := BuildPlan(context, statements[0], false)
	//if ddlPlan != nil {
	//	testDeepCopy(ddlPlan)
	//}

	planDdl := ddlPlan.Plan.(*plan.Plan_Ddl)
	definition := planDdl.Ddl.Definition.(*plan.DataDefinition_CreateTable)
	definition.CreateTable.TableDef.TableType = catalog.SystemOrdinaryRel

	return definition.CreateTable.TableDef, err
}

func buildTestShowCreateTable(sql string) (string, error) {
	mock := NewMockOptimizer(false)
	tableDef, err := buildTestCreateTableStmt(mock, sql)
	if err != nil {
		return "", err
	}

	var snapshot *plan.Snapshot
	showSQL, _, err := ConstructCreateTableSQL(&mock.ctxt, tableDef, snapshot, false, nil)
	if err != nil {
		return "", err
	}
	return showSQL, nil
}

func buildTestShowCreateExternalTable(t *testing.T, tableName string, param *tree.ExternParam) string {
	t.Helper()
	mock := NewMockOptimizer(false)
	jsonBytes, err := json.Marshal(param)
	require.NoError(t, err)

	tableDef := &plan.TableDef{
		Name:      tableName,
		TableType: catalog.SystemExternalRel,
		Createsql: string(jsonBytes),
		Cols: []*plan.ColDef{
			{
				Name:    "id",
				Typ:     plan.Type{Id: int32(types.T_int32)},
				Default: &plan.Default{NullAbility: true},
			},
			{
				Name:    "part_id",
				Typ:     plan.Type{Id: int32(types.T_int32)},
				Default: &plan.Default{NullAbility: true},
			},
		},
	}

	showSQL, _, err := ConstructCreateTableSQL(&mock.ctxt, tableDef, nil, false, nil)
	require.NoError(t, err)
	return showSQL
}

func TestShowCreateHiveExternalTableKeepsFilepath(t *testing.T) {
	got := buildTestShowCreateExternalTable(t, "test_show_ddl", &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.INFILE,
			Option: []string{
				"filepath", "/data/test/",
				"format", "parquet",
				"hive_partitioning", "true",
				"hive_partition_columns", "part_id",
			},
		},
	})
	require.Contains(t, got, "CREATE EXTERNAL TABLE `test_show_ddl`")
	require.Contains(t, got, "'FILEPATH'='/data/test/'")
	require.Contains(t, got, "'FORMAT'='parquet'")
	require.Contains(t, got, "'HIVE_PARTITIONING'='true'")
	require.Contains(t, got, "'HIVE_PARTITION_COLUMNS'='part_id'")
}

func TestShowCreateS3HiveExternalTableUsesS3Options(t *testing.T) {
	got := buildTestShowCreateExternalTable(t, "test_show_ddl_s3", &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.S3,
			Option: []string{
				"endpoint", "http://minio.local",
				"access_key_id", "AK",
				"secret_access_key", "SK",
				"bucket", "my-bucket",
				"filepath", "data/test/",
				"region", "us-east-1",
				"provider", "minio",
				"format", "parquet",
				"hive_partitioning", "true",
				"hive_partition_columns", "part_id",
			},
		},
	})
	require.Contains(t, got, " URL s3option{")
	require.Contains(t, got, "'endpoint'='http://minio.local'")
	require.Contains(t, got, "'access_key_id'='******'")
	require.Contains(t, got, "'secret_access_key'='******'")
	require.Contains(t, got, "'bucket'='my-bucket'")
	require.Contains(t, got, "'filepath'='data/test/'")
	require.Contains(t, got, "'region'='us-east-1'")
	require.Contains(t, got, "'provider'='minio'")
	require.Contains(t, got, "'format'='parquet'")
	require.Contains(t, got, "'hive_partitioning'='true'")
	require.Contains(t, got, "'hive_partition_columns'='part_id'")
	require.NotContains(t, got, " INFILE{")
}

func TestFormatColTypeGeometrySubtype(t *testing.T) {
	// Subtype lives in Scale, SRID in Width (srid+1 when defined).
	require.Equal(t, "POINT", FormatColType(plan.Type{
		Id:    int32(types.T_geometry),
		Scale: int32(geometrySubtypeEnum("POINT")),
	}))

	require.Equal(t, "GEOMETRY", FormatColType(plan.Type{
		Id: int32(types.T_geometry),
	}))

	require.Equal(t, "POINT SRID 4326", FormatColType(plan.Type{
		Id:    int32(types.T_geometry),
		Scale: int32(geometrySubtypeEnum("POINT")),
		Width: encodeGeometrySRIDWidth(4326, true),
	}))

	require.Equal(t, "GEOMETRY SRID 0", FormatColType(plan.Type{
		Id:    int32(types.T_geometry),
		Width: encodeGeometrySRIDWidth(0, true),
	}))

	// GEOMETRY32 family renders with the "32" suffix.
	require.Equal(t, "GEOMETRY32", FormatColType(plan.Type{
		Id: int32(types.T_geometry32),
	}))
	require.Equal(t, "POINT32", FormatColType(plan.Type{
		Id:    int32(types.T_geometry32),
		Scale: int32(geometrySubtypeEnum("POINT")),
	}))
}

func TestFormatColTypeArrayMetadata(t *testing.T) {
	require.Equal(t, "ARRAY(varchar(20))", FormatColType(plan.Type{
		Id:         int32(types.T_json),
		Enumvalues: "array(varchar(20))",
	}))
}

// TestShowCreateExternalWriteFilePattern ensures SHOW CREATE TABLE formatting
// keeps WRITE_FILE_PATTERN for writable external tables, in both the INFILE
// and the URL s3option forms; without it the recreated table silently degrades
// to read-only.
func TestShowCreateExternalWriteFilePattern(t *testing.T) {
	pattern := "stage://s/part-%U.csv"

	// INFILE{...} form (ScanType != S3). Writable tables must be recreatable
	// from the emitted DDL: the read FILEPATH is preserved (not masked) and
	// empty optional keys are omitted (the read-side validator rejects
	// 'JSONDATA'='').
	p := &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format:   tree.CSV,
		Filepath: "stage://s/part-*.csv",
		Option:   []string{"format", "csv", "write_file_pattern", pattern},
	}}
	out := formatInfileExternalOptionsForShowCreate(p)
	require.Contains(t, out, "'WRITE_FILE_PATTERN'='"+pattern+"'")
	require.Contains(t, out, "'FILEPATH'='stage://s/part-*.csv'")
	require.NotContains(t, out, "JSONDATA")
	require.NotContains(t, out, "''")

	// Read-only table: legacy output unchanged — no WRITE_FILE_PATTERN key,
	// FILEPATH masked, empty keys present.
	ro := &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format:   tree.CSV,
		Filepath: "/local/path.csv",
		Option:   []string{"format", "csv"},
	}}
	roOut := formatInfileExternalOptionsForShowCreate(ro)
	require.NotContains(t, roOut, "WRITE_FILE_PATTERN")
	require.Contains(t, roOut, "'FILEPATH'=''")
	require.NotContains(t, roOut, "/local/path.csv")
	require.Contains(t, roOut, "'JSONDATA'=''")

	// URL s3option{...} form.
	s3 := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.S3,
			Format:   tree.CSV,
			Option:   []string{"format", "csv", "write_file_pattern", pattern},
		},
		ExParam: tree.ExParam{S3Param: &tree.S3Parameter{Bucket: "b"}},
	}
	out = formatS3ExternalOptionsForShowCreate(s3)
	require.Contains(t, out, "'write_file_pattern'='"+pattern+"'")
}

// TestShowCreateExternalCommentRoundTrip: the CSV reader skips lines whose raw
// prefix matches the COMMENT marker, so SHOW CREATE must round-trip it for
// read-only external tables (writable tables reject a non-empty marker, so they
// never carry one). Omitted entirely when unset.
func TestShowCreateExternalCommentRoundTrip(t *testing.T) {
	// INFILE read-only table with a comment marker
	ro := &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format:   tree.CSV,
		Filepath: "/local/path.csv",
		Option:   []string{"format", "csv", "comment", "#"},
	}}
	out := formatInfileExternalOptionsForShowCreate(ro)
	require.Contains(t, out, "'COMMENT'='#'")

	// no comment option => no COMMENT key
	noComment := &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format:   tree.CSV,
		Filepath: "/local/path.csv",
		Option:   []string{"format", "csv"},
	}}
	require.NotContains(t, formatInfileExternalOptionsForShowCreate(noComment), "COMMENT")

	// S3 read-only table with a comment marker
	s3 := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.S3,
			Format:   tree.CSV,
			Option:   []string{"format", "csv", "comment", "REM"},
		},
		ExParam: tree.ExParam{S3Param: &tree.S3Parameter{Bucket: "b"}},
	}
	require.Contains(t, formatS3ExternalOptionsForShowCreate(s3), "'comment'='REM'")
}

// TestFormatStrInSingleQuotes: FIELDS/LINES values emitted by SHOW CREATE must
// be valid inside single-quoted SQL literals (a custom LINES TERMINATED BY
// used to render as the Go struct '&{#EOL#}', and a single-quote enclosure as
// an unescaped single quote).
func TestFormatStrInSingleQuotes(t *testing.T) {
	require.Equal(t, "#EOL#", formatStrInSingleQuotes("#EOL#"))
	require.Equal(t, `a''b`, formatStrInSingleQuotes("a'b"))
	require.Equal(t, `a\\b`, formatStrInSingleQuotes(`a\b`))
	require.Equal(t, `\\''`, formatStrInSingleQuotes(`\'`))
}

// TestFormatLinesTerminatedBy: SHOW CREATE must keep \n and \r\n distinct so a
// CRLF writable external table recreates as CRLF (not silently downgraded to
// LF). Both render as doubled-backslash escape sequences; other values flow
// through formatStrInSingleQuotes.
func TestFormatLinesTerminatedBy(t *testing.T) {
	require.Equal(t, `\\n`, formatLinesTerminatedBy("\n"))
	require.Equal(t, `\\r\\n`, formatLinesTerminatedBy("\r\n"))
	require.NotEqual(t, formatLinesTerminatedBy("\n"), formatLinesTerminatedBy("\r\n"))
	require.Equal(t, "#EOL#", formatLinesTerminatedBy("#EOL#"))
}
