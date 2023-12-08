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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/frontend/constant"
	"regexp"
	"strings"
)

const (
	// BI Tool Special SQL Structure Feature Clause
	targetClause1   = "CASE WHEN TABLE_TYPE='BASE TABLE' THEN CASE WHEN TABLE_SCHEMA = 'mysql' OR TABLE_SCHEMA = 'performance_schema' THEN 'SYSTEM TABLE' ELSE 'TABLE' END WHEN TABLE_TYPE='TEMPORARY' THEN 'LOCAL_TEMPORARY' ELSE TABLE_TYPE END AS TABLE_TYPE"
	targetClause2   = "FROM INFORMATION_SCHEMA.TABLES"
	targetClause3   = "HAVING TABLE_TYPE IN"
	rewriteCaseWhen = "CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN CASE WHEN TABLE_SCHEMA = 'mysql' OR TABLE_SCHEMA = 'mo_catalog' THEN 'SYSTEM TABLE' ELSE 'TABLE' END WHEN TABLE_TYPE = 'TEMPORARY' THEN 'LOCAL_TEMPORARY' ELSE TABLE_TYPE END AS TABLE_TYPE"
	aliasClause1    = "TABLE_CATALOG AS TABLE_CAT"
	aliasClause2    = "TABLE_SCHEMA AS TABLE_SCHEM"
	aliasClause3    = "NULL AS TABLE_SCHEM"
)

var aliasMap = map[string][2]string{
	aliasClause1: [2]string{"TABLE_CATALOG", "TABLE_CAT"},
	aliasClause2: [2]string{"TABLE_SCHEMA", "TABLE_SCHEM"},
	aliasClause3: [2]string{"TABLE_SCHEMA", "TABLE_SCHEM"},
}

// Determine if it is a BI query SQL
func judgeIsClientBIQuery(input *UserInput) bool {
	usersql := input.getSql()
	if len(input.sqlSourceType) == 1 && (input.sqlSourceType[0] == constant.ExternSql || input.sqlSourceType[0] == constant.CloudUserSql) {
		// Determine if SQL contains the target substring
		if strings.Contains(usersql, targetClause1) &&
			strings.Contains(usersql, targetClause2) &&
			strings.Contains(usersql, targetClause3) {
			return true
		}
	}
	return false
}

// BI Tool Special Dialect Equivalent Rewrite
func dialectEquivalentRewrite(input *UserInput) {
	var whereClause string
	var orderByClause string

	usersql := strings.Replace(input.getSql(), targetClause1, rewriteCaseWhen, -1)

	usersql, whereClause = handleHavingClause(usersql)
	usersql, orderByClause = handleOrderByClause(usersql)

	input.sql = fmt.Sprintf("SELECT * FROM (%s) AS t1 "+whereClause+" "+orderByClause, usersql)
}

func handleHavingClause(usersql string) (string, string) {
	// defining regular expressions
	re := regexp.MustCompile(`HAVING TABLE_TYPE IN (.*?\))`)
	// Find matches
	matches := re.FindStringSubmatch(usersql)
	// Check if a match is found
	if len(matches) >= 2 {
		//The first sub match is what you want
		havingClause := matches[0]

		// Replace the first "HAVING" with "WHERE"
		whereClause := strings.Replace(havingClause, "HAVING", "WHERE", 1)

		// Replace matching items with empty strings
		result := re.ReplaceAllString(usersql, "")
		return result, whereClause
	} else {
		return usersql, ""
	}
}

func handleOrderByClause(usersql string) (string, string) {
	// defining regular expressions
	re := regexp.MustCompile(`ORDER BY(.*?)$`)
	// Find matches
	matches := re.FindStringSubmatch(usersql)
	// Check if a match is found
	if len(matches) >= 2 {
		// The first sub match is what you want
		orderByClause := matches[0]

		if strings.Contains(usersql, aliasClause1) {
			orderByClause = strings.Replace(orderByClause, aliasMap[aliasClause1][0], aliasMap[aliasClause1][1], -1)
		}
		if strings.Contains(usersql, aliasClause2) {
			orderByClause = strings.Replace(orderByClause, aliasMap[aliasClause2][0], aliasMap[aliasClause2][1], -1)
		}
		if strings.Contains(usersql, aliasClause3) {
			orderByClause = strings.Replace(orderByClause, aliasMap[aliasClause3][0], aliasMap[aliasClause3][1], -1)
		}

		// Replace matching items with empty strings
		result := re.ReplaceAllString(usersql, "")
		return result, orderByClause
	} else {
		return usersql, ""
	}
}
