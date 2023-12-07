// Copyright 2023 Matrix Origin
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

package frontend

import (
	"github.com/matrixorigin/matrixone/pkg/frontend/constant"
	"testing"
)

func Test_dialectEquivalentRewrite(t *testing.T) {
	tests := []struct {
		name  string
		input *UserInput
	}{
		{
			name: "q1",
			input: &UserInput{
				sql:           "SELECT TABLE_CATALOG AS TABLE_CAT, TABLE_SCHEMA AS TABLE_SCHEM, TABLE_NAME, CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN CASE WHEN TABLE_SCHEMA = 'mysql' OR TABLE_SCHEMA = 'performance_schema' THEN 'SYSTEM TABLE' ELSE 'TABLE' END WHEN TABLE_TYPE = 'TEMPORARY' THEN 'LOCAL_TEMPORARY' ELSE TABLE_TYPE END AS TABLE_TYPE, TABLE_COMMENT AS REMARKS, NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, NULL AS TYPE_NAME, NULL AS SELF_REFERENCING_COL_NAME, NULL AS REF_GENERATION FROM INFORMATION_SCHEMA.TABLES HAVING TABLE_TYPE IN ('TABLE', null, null, null, null) ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME",
				sqlSourceType: []string{constant.ExternSql},
			},
		},
		{
			name: "q2",
			input: &UserInput{
				sql:           "SELECT TABLE_CATALOG AS TABLE_CAT, TABLE_SCHEMA AS TABLE_SCHEM, TABLE_NAME, CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN CASE WHEN TABLE_SCHEMA = 'mysql' OR TABLE_SCHEMA = 'performance_schema' THEN 'SYSTEM TABLE' ELSE 'TABLE' END WHEN TABLE_TYPE = 'TEMPORARY' THEN 'LOCAL_TEMPORARY' ELSE TABLE_TYPE END AS TABLE_TYPE, TABLE_COMMENT AS REMARKS, NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, NULL AS TYPE_NAME, NULL AS SELF_REFERENCING_COL_NAME, NULL AS REF_GENERATION FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA LIKE 'ucl360_dev' HAVING TABLE_TYPE IN ('TABLE', null, null, null, null) ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME",
				sqlSourceType: []string{constant.ExternSql},
			},
		},
		{
			name: "q3",
			input: &UserInput{
				sql:           "SELECT TABLE_CATALOG AS TABLE_CAT, TABLE_SCHEMA AS TABLE_SCHEM, TABLE_NAME, CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN CASE WHEN TABLE_SCHEMA = 'mysql' OR TABLE_SCHEMA = 'performance_schema' THEN 'SYSTEM TABLE' ELSE 'TABLE' END WHEN TABLE_TYPE = 'TEMPORARY' THEN 'LOCAL_TEMPORARY' ELSE TABLE_TYPE END AS TABLE_TYPE, TABLE_COMMENT AS REMARKS, NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, NULL AS TYPE_NAME, NULL AS SELF_REFERENCING_COL_NAME, NULL AS REF_GENERATION FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA LIKE 'ucl360_dev' HAVING TABLE_TYPE IN ('VIEW', null, null, null, null) ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME",
				sqlSourceType: []string{constant.ExternSql},
			},
		},
		{
			name: "Q1",
			input: &UserInput{
				sql:           "SELECT TABLE_CATALOG AS TABLE_CAT, TABLE_SCHEMA AS TABLE_SCHEM, TABLE_NAME, CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN CASE WHEN TABLE_SCHEMA = 'mysql' OR TABLE_SCHEMA = 'performance_schema' THEN 'SYSTEM TABLE' ELSE 'TABLE' END WHEN TABLE_TYPE = 'TEMPORARY' THEN 'LOCAL_TEMPORARY' ELSE TABLE_TYPE END AS TABLE_TYPE, TABLE_COMMENT AS REMARKS, NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, NULL AS TYPE_NAME, NULL AS SELF_REFERENCING_COL_NAME, NULL AS REF_GENERATION FROM INFORMATION_SCHEMA.TABLES HAVING TABLE_TYPE IN ('TABLE', null, null, null, null) ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME",
				sqlSourceType: []string{constant.ExternSql},
			},
		},
		{
			name: "Q2",
			input: &UserInput{
				sql:           "SELECT TABLE_SCHEMA AS TABLE_CAT, NULL AS TABLE_SCHEM, TABLE_NAME, CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN CASE WHEN TABLE_SCHEMA = 'mysql' OR TABLE_SCHEMA = 'performance_schema' THEN 'SYSTEM TABLE' ELSE 'TABLE' END WHEN TABLE_TYPE = 'TEMPORARY' THEN 'LOCAL_TEMPORARY' ELSE TABLE_TYPE END AS TABLE_TYPE, TABLE_COMMENT AS REMARKS, NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, NULL AS TYPE_NAME, NULL AS SELF_REFERENCING_COL_NAME, NULL AS REF_GENERATION FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'abc' HAVING TABLE_TYPE IN ('LOCAL TEMPORARY','TABLE','VIEW',null,null) ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME",
				sqlSourceType: []string{constant.ExternSql},
			},
		},
		{
			name: "Q3",
			input: &UserInput{
				sql:           "SELECT TABLE_SCHEMA AS TABLE_CAT, NULL AS TABLE_SCHEM, TABLE_NAME, CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN CASE WHEN TABLE_SCHEMA = 'mysql' OR TABLE_SCHEMA = 'performance_schema' THEN 'SYSTEM TABLE' ELSE 'TABLE' END WHEN TABLE_TYPE = 'TEMPORARY' THEN 'LOCAL_TEMPORARY' ELSE TABLE_TYPE END AS TABLE_TYPE, TABLE_COMMENT AS REMARKS, NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, NULL AS TYPE_NAME, NULL AS SELF_REFERENCING_COL_NAME, NULL AS REF_GENERATION FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'emis' HAVING TABLE_TYPE IN ('TABLE','VIEW',null,null,null) ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME",
				sqlSourceType: []string{constant.ExternSql},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if judgeIsClientBIQuery(tt.input) {
				dialectEquivalentRewrite(tt.input)
				t.Logf("Equivalent rewrite result: %s", tt.input.sql)
			} else {
				t.Fatalf("%+v", "Does not comply with rewrite rules")
			}
		})
	}
}
