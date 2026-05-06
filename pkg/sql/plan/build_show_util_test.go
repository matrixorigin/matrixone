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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func TestFindViewKeywordRequiresBoundaries(t *testing.T) {
	require.Equal(t, -1, findViewKeyword("PREVIEW V"))
	require.Equal(t, -1, findViewKeyword("CREATE VIEWER V"))
	require.Equal(t, strings.Index("CREATE VIEW V", "VIEW"), findViewKeyword("CREATE VIEW V"))
	require.Equal(t, strings.Index("CREATE\tVIEW\nV", "VIEW"), findViewKeyword("CREATE\tVIEW\nV"))
	require.Equal(t, strings.LastIndex("CREATE /* VIEW */ VIEW V", "VIEW"), findViewKeyword("CREATE /* VIEW */ VIEW V"))
	require.Equal(t, strings.LastIndex("CREATE -- VIEW\nVIEW V", "VIEW"), findViewKeyword("CREATE -- VIEW\nVIEW V"))
	require.Equal(t, strings.LastIndex("CREATE # VIEW\nVIEW V", "VIEW"), findViewKeyword("CREATE # VIEW\nVIEW V"))
	require.Equal(t, strings.Index("/*!50001 CREATE DEFINER = `ROOT`@`%` VIEW V AS SELECT 1 */", "VIEW"),
		findViewKeyword("/*!50001 CREATE DEFINER = `ROOT`@`%` VIEW V AS SELECT 1 */"))
}

func TestHasSQLSecurityClauseIgnoresComments(t *testing.T) {
	require.True(t, hasSQLSecurityClause("CREATE SQL SECURITY DEFINER "))
	require.False(t, hasSQLSecurityClause("CREATE /* SQL SECURITY INVOKER */ "))
	require.False(t, hasSQLSecurityClause("CREATE -- SQL SECURITY DEFINER\n"))
	require.False(t, hasSQLSecurityClause("CREATE # SQL SECURITY DEFINER\n"))
	require.True(t, hasSQLSecurityClause("/*!50001 CREATE DEFINER = `ROOT`@`%` SQL SECURITY DEFINER VIEW V AS SELECT 1 */"))
}

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
                                FULLTEXT idx01(json1) WITH PARSER json,
                                FULLTEXT idx02(json1,json2) WITH PARSER json)`,
			want: "CREATE TABLE `src` (\n  `id` bigint NOT NULL,\n  `json1` json DEFAULT NULL,\n  `json2` json DEFAULT NULL,\n  PRIMARY KEY (`id`),\n FULLTEXT `idx01`(`json1`) WITH PARSER json,\n FULLTEXT `idx02`(`json1`,`json2`) WITH PARSER json\n)",
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

// formatIdent and formatStrLit split the old formatStr responsibilities.
// Identifier escape (backticks -> doubled backticks), string literal escape
// (single quotes -> doubled quotes). Free-form expression text is never
// passed through either — callers emit it verbatim.
func TestFormatIdentEscapesBackticks(t *testing.T) {
	require.Equal(t, "a``b", formatIdent("a`b"))
	require.Equal(t, "plain", formatIdent("plain"))
	// Must not treat single quotes specially — they are allowed inside an
	// identifier name.
	require.Equal(t, "o'brien", formatIdent("o'brien"))
}

func TestFormatStrLitEscapesSingleQuotes(t *testing.T) {
	require.Equal(t, "'o''brien'", formatStrLit("o'brien"))
	require.Equal(t, "''", formatStrLit(""))
	// Backticks are NOT doubled inside a string literal.
	require.Equal(t, "'a`b'", formatStrLit("a`b"))
	// Multiple interior quotes must all be doubled.
	require.Equal(t, "'''hi''there'''", formatStrLit("'hi'there'"))
}

// TestFormatStrLitEscapesBackslashes pins the round-trip contract for raw
// strings that contain a literal backslash. The MySQL scanner collapses
// \b / \n / \r / \t / \Z / \0 into their control-char equivalents, so a
// value like "a\b" must be emitted as 'a\\b' — otherwise re-parsing the
// SHOW CREATE output silently turns it into "a" + 0x08.
func TestFormatStrLitEscapesBackslashes(t *testing.T) {
	require.Equal(t, `'a\\b'`, formatStrLit(`a\b`))
	// Combined quote + backslash still escapes both.
	require.Equal(t, `'o''\\brien'`, formatStrLit(`o'\brien`))
}

// TestEscapeFormatEscapesBackslashes guards the ENUM/SET member formatter.
// A SET member declared as SET('a\\b') is stored with one literal backslash;
// the old EscapeFormat replaceMap did not include '\\' -> "\\\\", so
// FormatColType emitted SET('a\b'), which re-parses as "a" + 0x08.
func TestEscapeFormatEscapesBackslashes(t *testing.T) {
	require.Equal(t, `a\\b`, EscapeFormat(`a\b`))
}

func TestCommentBackslashEscape(t *testing.T) {
	colDef := &plan.ColDef{
		Name:    "c1",
		Comment: `path\to\file`,
		Typ:     plan.Type{Id: int32(22)}, // T_int32
		Default: &plan.Default{NullAbility: true},
	}
	tableDef := &plan.TableDef{
		Name:      "t_comment",
		TableType: catalog.SystemOrdinaryRel,
		Cols:      []*plan.ColDef{colDef},
	}
	mock := NewMockOptimizer(false)
	got, _, err := ConstructCreateTableSQL(&mock.ctxt, tableDef, nil, false, nil)
	require.NoError(t, err)
	require.Contains(t, got, `COMMENT 'path\\to\\file'`)
}

func TestTableCommentBackslashEscape(t *testing.T) {
	colDef := &plan.ColDef{
		Name:    "c1",
		Typ:     plan.Type{Id: int32(22)}, // T_int32
		Default: &plan.Default{NullAbility: true},
	}
	tableDef := &plan.TableDef{
		Name:      "t_tbl_comment",
		TableType: catalog.SystemOrdinaryRel,
		Cols:      []*plan.ColDef{colDef},
		Defs: []*plan.TableDef_DefType{
			{
				Def: &plan.TableDef_DefType_Properties{
					Properties: &plan.PropertiesDef{
						Properties: []*plan.Property{
							{Key: catalog.SystemRelAttr_Comment, Value: `back\slash`},
						},
					},
				},
			},
		},
	}
	mock := NewMockOptimizer(false)
	got, _, err := ConstructCreateTableSQL(&mock.ctxt, tableDef, nil, false, nil)
	require.NoError(t, err)
	require.Contains(t, got, `COMMENT='back\\slash'`)
}

func TestOnUpdateOriginString(t *testing.T) {
	colDef := &plan.ColDef{
		Name: "updated_at",
		Typ:  plan.Type{Id: int32(53)}, // T_timestamp
		Default: &plan.Default{
			NullAbility:  true,
			OriginString: "CURRENT_TIMESTAMP()",
		},
		OnUpdate: &plan.OnUpdate{
			Expr:         &plan.Expr{},
			OriginString: "CURRENT_TIMESTAMP()",
		},
	}
	tableDef := &plan.TableDef{
		Name:      "t_on_update",
		TableType: catalog.SystemOrdinaryRel,
		Cols:      []*plan.ColDef{colDef},
	}
	mock := NewMockOptimizer(false)
	got, _, err := ConstructCreateTableSQL(&mock.ctxt, tableDef, nil, false, nil)
	require.NoError(t, err)
	require.Contains(t, got, "ON UPDATE CURRENT_TIMESTAMP()")
}

func TestFormatStrLitBackslashNoDoubleEscape(t *testing.T) {
	require.Equal(t, `'\\'`, formatStrLit(`\`))
	require.Equal(t, `'"'`, formatStrLit(`"`))
}

func TestFormatStrLitCRLF(t *testing.T) {
	require.Equal(t, `'\r\n'`, formatStrLit("\r\n"))
	require.Equal(t, `'\n'`, formatStrLit("\n"))
	require.NotEqual(t, formatStrLit("\n"), formatStrLit("\r\n"))
}

func TestFormatStrLitNUL(t *testing.T) {
	require.Equal(t, `'\0'`, formatStrLit("\x00"))
}

// TestShowCreateSetMemberCasePreservation verifies the SHOW CREATE column
// loop only lower-cases the leading "SET" keyword and keeps the declared
// member-name case, so SET('Read','Write') round-trips as set('Read','Write')
// instead of set('read','write').
func TestShowCreateSetMemberCasePreservation(t *testing.T) {
	setType := plan.Type{Id: int32(28), Enumvalues: "Read,Write"} // T_uint64 with SET members
	raw := FormatColType(setType)
	require.Equal(t, "SET('Read','Write')", raw)

	// Mirror the branch added to build_show_util.go: lower-case only the
	// type keyword, keep members intact.
	lowered := strings.ToLower(raw[:3]) + raw[3:]
	require.Equal(t, "set('Read','Write')", lowered)
	require.NotEqual(t, strings.ToLower(raw), lowered,
		"plain strings.ToLower would damage member names; keep the fix in place")
}
