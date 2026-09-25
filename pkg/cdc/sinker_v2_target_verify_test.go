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

package cdc

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	gomysql "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestCDCTargetGuardSQLRetryClassification(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		retryable bool
	}{
		{"bad connection", driver.ErrBadConn, true},
		{"lost MySQL connection", &gomysql.MySQLError{Number: 2013}, true},
		{"MO RC definition changed over SQL wire", &gomysql.MySQLError{Number: moerr.ErrTxnNeedRetryWithDefChanged}, true},
		{"missing SELECT privilege", &gomysql.MySQLError{Number: 1142}, false},
		{"cancelled", context.Canceled, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			classified := classifyCDCTargetSQLError(tc.err)
			require.Equal(t, tc.retryable, IsRetryableConnectionError(classified))
		})
	}
}

func TestVerifyCDCTargetTableStructure(t *testing.T) {
	const columnsSQL = "SELECT column_name, column_type, collation_name, numeric_scale FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position"
	const indexesSQL = "SELECT index_name, non_unique, seq_in_index, column_name, sub_part FROM information_schema.statistics WHERE table_schema = ? AND table_name = ? ORDER BY index_name, seq_in_index"
	type column struct{ name, typ string }
	type index struct {
		name, column        string
		nonUnique, sequence int64
		prefix              any
	}
	tests := []struct {
		name    string
		columns []column
		indexes []index
		unique  bool
		wantErr string
	}{
		{"valid MySQL integer display width", []column{{"id", "int(11)"}, {"v", "varchar(20)"}}, []index{{"PRIMARY", "id", 0, 1, nil}}, false, ""},
		{"valid unique index", []column{{"id", "int"}, {"v", "varchar(20)"}}, []index{{"PRIMARY", "id", 0, 1, nil}, {"uk_v", "v", 0, 1, nil}}, true, ""},
		{"reordered columns", []column{{"v", "varchar(20)"}, {"id", "int"}}, nil, false, "column 1"},
		{"narrowed varchar", []column{{"id", "int"}, {"v", "varchar(5)"}}, nil, false, "column 2"},
		{"changed text collation", []column{{"id", "int"}, {"v", "varchar(20)"}}, nil, false, "collation differs"},
		{"missing column", []column{{"id", "int"}}, nil, false, "has 1 columns"},
		{"missing primary key", []column{{"id", "int"}, {"v", "varchar(20)"}}, nil, false, "primary key differs"},
		{"extra unique index", []column{{"id", "int"}, {"v", "varchar(20)"}}, []index{{"PRIMARY", "id", 0, 1, nil}, {"uk_v", "v", 0, 1, nil}}, false, "unexpected unique key"},
		{"missing unique index", []column{{"id", "int"}, {"v", "varchar(20)"}}, []index{{"PRIMARY", "id", 0, 1, nil}}, true, "missing a source unique key"},
		{"prefix unique index", []column{{"id", "int"}, {"v", "varchar(20)"}}, []index{{"PRIMARY", "id", 0, 1, nil}, {"uk_v", "v", 0, 1, int64(4)}}, true, "prefix unique key"},
		{"valid year display width", []column{{"id", "int"}, {"v", "year(4)"}}, []index{{"PRIMARY", "id", 0, 1, nil}}, false, ""},
		{"valid MO float scale", []column{{"id", "int"}, {"v", "float(5)"}}, []index{{"PRIMARY", "id", 0, 1, nil}}, false, ""},
		{"different MO float scale", []column{{"id", "int"}, {"v", "float(5)"}}, nil, false, "column 2"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			conn, err := db.Conn(context.Background())
			require.NoError(t, err)
			defer conn.Close()
			executor := &Executor{targetLockConn: conn}
			def := &plan.TableDef{
				Cols: []*plan.ColDef{
					{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}},
					{Name: "v", Typ: plan.Type{Id: int32(types.T_varchar), Width: 20, Charset: uint32(types.CharsetUTF8MB4Bin)}},
				},
				Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}},
			}
			switch tc.name {
			case "valid year display width":
				def.Cols[1].Typ = plan.Type{Id: int32(types.T_year), Width: 4}
			case "valid MO float scale", "different MO float scale":
				def.Cols[1].Typ = plan.Type{Id: int32(types.T_float32), Width: 5, Scale: 2}
			}
			if tc.unique {
				def.Indexes = []*plan.IndexDef{{IndexName: "uk_v", Parts: []string{"v"}, Unique: true}}
			}
			columnRows := sqlmock.NewRows([]string{"column_name", "column_type", "collation_name", "numeric_scale"})
			for _, col := range tc.columns {
				var collation any
				var numericScale any
				if tc.name == "valid MO float scale" {
					numericScale = int64(2)
				} else if tc.name == "different MO float scale" {
					numericScale = int64(3)
				}
				if col.name == "v" {
					collation = "utf8mb4_bin"
					if tc.name == "changed text collation" {
						collation = "utf8mb4_general_ci"
					}
				}
				columnRows.AddRow(col.name, col.typ, collation, numericScale)
			}
			mock.ExpectQuery(regexp.QuoteMeta(columnsSQL)).WithArgs("dst", "t").WillReturnRows(columnRows)
			if len(tc.columns) == 2 && tc.columns[0].name == "id" &&
				(tc.columns[1].typ == "varchar(20)" && tc.name != "changed text collation" ||
					tc.name == "valid year display width" || tc.name == "valid MO float scale") {
				indexRows := sqlmock.NewRows([]string{"index_name", "non_unique", "seq_in_index", "column_name", "sub_part"})
				for _, idx := range tc.indexes {
					indexRows.AddRow(idx.name, idx.nonUnique, idx.sequence, idx.column, idx.prefix)
				}
				mock.ExpectQuery(regexp.QuoteMeta(indexesSQL)).WithArgs("dst", "t").WillReturnRows(indexRows)
			}
			err = verifyCDCTargetTable(context.Background(), executor,
				&DbTableInfo{SinkDbName: "dst", SinkTblName: "t", SourceTblId: 42}, def, CDCSinkType_MO)
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantErr)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestNormalizeCDCTargetColumnTypeFromMOAndMySQL(t *testing.T) {
	require.Equal(t, "utf8mb4_bin", expectedCDCTargetCollation(plan.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetLegacy)}))
	require.Equal(t, "utf8mb4_bin", expectedCDCTargetCollation(plan.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8MB4Bin)}))
	require.Equal(t, "utf8mb4_general_ci", expectedCDCTargetCollation(plan.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8)}))
	for _, tc := range []struct{ source, target string }{
		{"INT", "INT(32)"},
		{"TINYINT UNSIGNED", "TINYINT UNSIGNED(8)"},
		{"SMALLINT UNSIGNED", "SMALLINT UNSIGNED(16)"},
		{"INT UNSIGNED", "INT UNSIGNED(32)"},
		{"BIGINT UNSIGNED", "BIGINT UNSIGNED(64)"},
		{"FLOAT", "FLOAT(0)"},
		{"DOUBLE", "DOUBLE(0)"},
		{"DATE", "DATE(0)"},
		{"DATETIME", "DATETIME(0)"},
		{"TIMESTAMP", "TIMESTAMP(0)"},
		{"BOOL", "BOOL(0)"},
		{"JSON", "JSON(0)"},
	} {
		require.Equal(t, normalizeCDCTargetColumnType(tc.source), normalizeCDCTargetColumnType(tc.target), tc)
	}
	require.NotEqual(t, normalizeCDCTargetColumnType("varchar(20)"), normalizeCDCTargetColumnType("varchar(5)"))
	require.NotEqual(t, normalizeCDCTargetColumnType("decimal(16,6)"), normalizeCDCTargetColumnType("decimal(8,2)"))
	require.NotEqual(t, normalizeCDCTargetColumnType("ENUM('a b')"), normalizeCDCTargetColumnType("ENUM('ab')"))
	require.NotEqual(t, normalizeCDCTargetColumnType("ENUM('integer')"), normalizeCDCTargetColumnType("ENUM('int')"))
	require.False(t, cdcTargetColumnTypeMatches(plan.Type{Id: int32(types.T_int8)}, "BOOL(0)", CDCSinkType_MO, sql.NullInt64{}))
	require.True(t, cdcTargetColumnTypeMatches(plan.Type{Id: int32(types.T_bool)}, "tinyint(1)", CDCSinkType_MySQL, sql.NullInt64{}))
	require.False(t, cdcTargetColumnTypeMatches(plan.Type{Id: int32(types.T_bool)}, "tinyint(1)", CDCSinkType_MO, sql.NullInt64{}))
}
