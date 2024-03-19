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

package util

import (
	"fmt"
	"go/constant"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func CopyBatch(bat *batch.Batch, proc *process.Process) (*batch.Batch, error) {
	rbat := batch.NewWithSize(len(bat.Vecs))
	rbat.Attrs = append(rbat.Attrs, bat.Attrs...)
	for i, srcVec := range bat.Vecs {
		vec := proc.GetVector(*srcVec.GetType())
		if err := vector.GetUnionAllFunction(*srcVec.GetType(), proc.Mp())(vec, srcVec); err != nil {
			rbat.Clean(proc.Mp())
			return nil, err
		}
		rbat.SetVector(int32(i), vec)
	}
	rbat.SetRowCount(bat.RowCount())
	return rbat, nil
}

func SplitTableAndColumn(name string) (string, string) {
	var schema string

	{ // determine if it is a function call
		i, j := strings.Index(name, "("), strings.Index(name, ")")
		if i >= 0 && j >= 0 && j > i {
			return "", name
		}
	}
	xs := strings.Split(name, ".")
	for i := 0; i < len(xs)-1; i++ {
		if i > 0 {
			schema += "."
		}
		schema += xs[i]
	}
	return schema, xs[len(xs)-1]
}

func TableIsLoggingTable(dbName string, tableName string) bool {
	if tableName == "statement_info" && dbName == "system" {
		return true
	} else if tableName == "metric" && dbName == "system_metrics" {
		return true
	} else if tableName == catalog.MO_SQL_STMT_CU && dbName == catalog.MO_SYSTEM_METRICS {
		return true
	}
	return false
}

// TableIsClusterTable check the table type is cluster table
func TableIsClusterTable(tableType string) bool {
	return tableType == catalog.SystemClusterRel
}
func DbIsSystemDb(dbName string) bool {
	if dbName == catalog.MO_CATALOG {
		return true
	}
	if dbName == catalog.MO_DATABASE {
		return true
	}
	if dbName == catalog.MO_TABLES {
		return true
	}
	if dbName == catalog.MO_COLUMNS {
		return true
	}
	if dbName == catalog.MOTaskDB {
		return true
	}
	if dbName == "information_schema" {
		return true
	}
	if dbName == "system" {
		return true
	}
	if dbName == "system_metrics" {
		return true
	}
	return false
}

// Build the filter condition AST expression for mo_database, as follows:
// account_id = cur_accountId or (account_id = 0 and datname in ('mo_catalog'))
func BuildMoDataBaseFilter(curAccountId uint64) tree.Expr {
	// left is: account_id = cur_accountId
	left := makeAccountIdEqualAst(curAccountId)

	datnameColName := &tree.UnresolvedName{
		NumParts: 1,
		Parts:    tree.NameParts{catalog.SystemDBAttr_Name},
	}

	mo_catalogConst := tree.NewNumValWithType(constant.MakeString(catalog.MO_CATALOG), catalog.MO_CATALOG, false, tree.P_char)
	inValues := tree.NewTuple(tree.Exprs{mo_catalogConst})
	// datname in ('mo_catalog')
	inExpr := tree.NewComparisonExpr(tree.IN, datnameColName, inValues)

	// account_id = 0
	accountIdEqulZero := makeAccountIdEqualAst(0)
	// andExpr is:account_id = 0 and datname in ('mo_catalog')
	andExpr := tree.NewAndExpr(accountIdEqulZero, inExpr)

	// right is:(account_id = 0 and datname in ('mo_catalog'))
	right := tree.NewParentExpr(andExpr)
	// return is: account_id = cur_accountId or (account_id = 0 and datname in ('mo_catalog'))
	return tree.NewOrExpr(left, right)
}

func BuildSysStatementInfoFilter(acctName string) tree.Expr {
	equalAccount := makeStringEqualAst("account", strings.Split(acctName, ":")[0])
	return equalAccount
}

func BuildSysMetricFilter(acctName string) tree.Expr {
	equalAccount := makeStringEqualAst("account", strings.Split(acctName, ":")[0])
	return equalAccount
}

// Build the filter condition AST expression for mo_tables, as follows:
// account_id = cur_account_id or (account_id = 0 and (relname in ('mo_tables','mo_database','mo_columns') or relkind = 'cluster'))
func BuildMoTablesFilter(curAccountId uint64) tree.Expr {
	// left is: account_id = cur_accountId
	left := makeAccountIdEqualAst(curAccountId)

	relnameColName := &tree.UnresolvedName{
		NumParts: 1,
		Parts:    tree.NameParts{catalog.SystemRelAttr_Name},
	}

	mo_databaseConst := tree.NewNumValWithType(constant.MakeString(catalog.MO_DATABASE), catalog.MO_DATABASE, false, tree.P_char)
	mo_tablesConst := tree.NewNumValWithType(constant.MakeString(catalog.MO_TABLES), catalog.MO_TABLES, false, tree.P_char)
	mo_columnsConst := tree.NewNumValWithType(constant.MakeString(catalog.MO_COLUMNS), catalog.MO_COLUMNS, false, tree.P_char)
	inValues := tree.NewTuple(tree.Exprs{mo_databaseConst, mo_tablesConst, mo_columnsConst})

	// relname in ('mo_tables','mo_database','mo_columns')
	inExpr := tree.NewComparisonExpr(tree.IN, relnameColName, inValues)
	//relkindEqualAst := makeRelkindEqualAst("cluster")

	relkindEqualAst := makeStringEqualAst(catalog.SystemRelAttr_Kind, "cluster")

	// (relname in ('mo_tables','mo_database','mo_columns') or relkind = 'cluster')
	tempExpr := tree.NewOrExpr(inExpr, relkindEqualAst)
	tempExpr2 := tree.NewParentExpr(tempExpr)
	// account_id = 0
	accountIdEqulZero := makeAccountIdEqualAst(0)
	// andExpr is: account_id = 0 and (relname in ('mo_tables','mo_database','mo_columns') or relkind = 'cluster')
	andExpr := tree.NewAndExpr(accountIdEqulZero, tempExpr2)

	// right is: (account_id = 0 and (relname in ('mo_tables','mo_database','mo_columns') or relkind = 'cluster'))
	right := tree.NewParentExpr(andExpr)

	// return is: account_id = cur_account_id or (account_id = 0 and (relname in ('mo_tables','mo_database','mo_columns') or relkind = 'cluster'));
	return tree.NewOrExpr(left, right)
}

// Build the filter condition AST expression for mo_columns, as follows:
// account_id = current_id or (account_id = 0 and attr_databse in mo_catalog and att_relname not in other tables)
func BuildMoColumnsFilter(curAccountId uint64) tree.Expr {
	// left is: account_id = cur_accountId
	left := makeAccountIdEqualAst(curAccountId)

	att_relnameColName := &tree.UnresolvedName{
		NumParts: 1,
		Parts:    tree.NameParts{catalog.SystemColAttr_RelName},
	}

	att_dblnameColName := &tree.UnresolvedName{
		NumParts: 1,
		Parts:    tree.NameParts{catalog.SystemColAttr_DBName},
	}

	mo_catalogConst := tree.NewNumValWithType(constant.MakeString(catalog.MO_CATALOG), catalog.MO_CATALOG, false, tree.P_char)
	inValues := tree.NewTuple(tree.Exprs{mo_catalogConst})
	// datname in ('mo_catalog')
	inExpr := tree.NewComparisonExpr(tree.IN, att_dblnameColName, inValues)

	mo_userConst := tree.NewNumValWithType(constant.MakeString("mo_user"), "mo_user", false, tree.P_char)
	mo_roleConst := tree.NewNumValWithType(constant.MakeString("mo_role"), "mo_role", false, tree.P_char)
	mo_user_grantConst := tree.NewNumValWithType(constant.MakeString("mo_user_grant"), "mo_user_grant", false, tree.P_char)
	mo_role_grantConst := tree.NewNumValWithType(constant.MakeString("mo_role_grant"), "mo_role_grant", false, tree.P_char)
	mo_role_privsConst := tree.NewNumValWithType(constant.MakeString("mo_role_privs"), "mo_role_privs", false, tree.P_char)
	mo_user_defined_functionConst := tree.NewNumValWithType(constant.MakeString("mo_user_defined_function"), "mo_user_defined_function", false, tree.P_char)
	mo_mysql_compatibility_modeConst := tree.NewNumValWithType(constant.MakeString("mo_mysql_compatibility_mode"), "mo_mysql_compatibility_mode", false, tree.P_char)
	mo_indexes := tree.NewNumValWithType(constant.MakeString("mo_indexes"), "mo_indexes", false, tree.P_char)
	mo_table_partitions := tree.NewNumValWithType(constant.MakeString("mo_table_partitions"), "mo_table_partitions", false, tree.P_char)
	mo_pubs := tree.NewNumValWithType(constant.MakeString("mo_pubs"), "mo_pubs", false, tree.P_char)
	mo_stored_procedure := tree.NewNumValWithType(constant.MakeString("mo_stored_procedure"), "mo_stored_procedure", false, tree.P_char)
	mo_stages := tree.NewNumValWithType(constant.MakeString("mo_stages"), "mo_stages", false, tree.P_char)

	notInValues := tree.NewTuple(tree.Exprs{mo_userConst, mo_roleConst, mo_user_grantConst, mo_role_grantConst, mo_role_privsConst,
		mo_user_defined_functionConst, mo_mysql_compatibility_modeConst, mo_indexes, mo_table_partitions, mo_pubs, mo_stored_procedure, mo_stages})

	notInexpr := tree.NewComparisonExpr(tree.NOT_IN, att_relnameColName, notInValues)

	// account_id = 0
	accountIdEqulZero := makeAccountIdEqualAst(0)
	// andExpr is:account_id = 0 and datname in ('mo_catalog')
	andExpr := tree.NewAndExpr(accountIdEqulZero, inExpr)
	andExpr = tree.NewAndExpr(andExpr, notInexpr)

	// right is:(account_id = 0 and datname in ('mo_catalog'))
	right := tree.NewParentExpr(andExpr)
	// return is: account_id = cur_accountId or (account_id = 0 and datname in ('mo_catalog'))
	return tree.NewOrExpr(left, right)
}

// build equal ast expr: colName = 'xxx'
func makeStringEqualAst(lColName, rValue string) tree.Expr {
	relkindColName := &tree.UnresolvedName{
		NumParts: 1,
		Parts:    tree.NameParts{lColName},
	}
	clusterConst := tree.NewNumValWithType(constant.MakeString(rValue), rValue, false, tree.P_char)
	return tree.NewComparisonExpr(tree.EQUAL, relkindColName, clusterConst)
}

// build ast expr: account_id = cur_accountId
func makeAccountIdEqualAst(curAccountId uint64) tree.Expr {
	accountIdColName := &tree.UnresolvedName{
		NumParts: 1,
		Parts:    tree.NameParts{"account_id"},
	}
	curAccountIdConst := tree.NewNumValWithType(constant.MakeUint64(uint64(curAccountId)), strconv.Itoa(int(curAccountId)), false, tree.P_int64)
	return tree.NewComparisonExpr(tree.EQUAL, accountIdColName, curAccountIdConst)
}

var (
	clusterTableAttributeName = "account_id"
	clusterTableAttributeType = &tree.T{InternalType: tree.InternalType{
		Family:   tree.IntFamily,
		Width:    32,
		Oid:      uint32(defines.MYSQL_TYPE_LONG),
		Unsigned: true,
	}}
)

func GetClusterTableAttributeName() string {
	return clusterTableAttributeName
}

func GetClusterTableAttributeType() *tree.T {
	return clusterTableAttributeType
}

func IsClusterTableAttribute(name string) bool {
	return name == clusterTableAttributeName
}

const partitionDelimiter = "%!%"

// IsValidNameForPartitionTable
// the name forms the partition table does not have the partitionDelimiter
func IsValidNameForPartitionTable(name string) bool {
	return !strings.Contains(name, partitionDelimiter)
}

// MakeNameOfPartitionTable
// !!!NOTE!!! With assumption: the partition name and the table name does not have partitionDelimiter.
// partition table name format : %!%partition_name%!%table_name
func MakeNameOfPartitionTable(partitionName, tableName string) (bool, string) {
	if strings.Contains(partitionName, partitionDelimiter) ||
		strings.Contains(tableName, partitionDelimiter) {
		return false, ""
	}
	if len(partitionName) == 0 ||
		len(tableName) == 0 {
		return false, ""
	}
	return true, fmt.Sprintf("%s%s%s%s", partitionDelimiter, partitionName, partitionDelimiter, tableName)
}

// SplitNameOfPartitionTable splits the partition table name into partition name and origin table name
func SplitNameOfPartitionTable(name string) (bool, string, string) {
	if !strings.HasPrefix(name, partitionDelimiter) {
		return false, "", ""
	}
	partNameIdx := len(partitionDelimiter)
	if partNameIdx >= len(name) {
		return false, "", ""
	}
	left := name[partNameIdx:]
	secondIdx := strings.Index(left, partitionDelimiter)
	if secondIdx < 0 {
		return false, "", ""
	}

	if secondIdx == 0 {
		return false, "", ""
	}
	partName := left[:secondIdx]

	tableNameIdx := secondIdx + len(partitionDelimiter)
	if tableNameIdx >= len(left) {
		return false, "", ""
	}
	tableName := left[tableNameIdx:]
	return true, partName, tableName
}
