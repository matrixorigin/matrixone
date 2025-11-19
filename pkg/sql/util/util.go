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
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	//every account have these tables.
	predefinedTables []string
	specialTables    = map[string]struct{}{
		catalog.MO_DATABASE: {},
		catalog.MO_TABLES:   {},
		catalog.MO_COLUMNS:  {},
	}
)

func InitPredefinedTables(tables []string) {
	predefinedTables = tables
}

func CopyBatch(bat *batch.Batch, proc *process.Process) (*batch.Batch, error) {
	rbat := batch.NewWithSize(len(bat.Vecs))
	rbat.Attrs = append(rbat.Attrs, bat.Attrs...)
	for i, srcVec := range bat.Vecs {
		vec := vector.NewVec(*srcVec.GetType())
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

	datnameColName := tree.NewUnresolvedColName(catalog.SystemDBAttr_Name)

	mo_catalogConst := tree.NewNumVal(catalog.MO_CATALOG, catalog.MO_CATALOG, false, tree.P_char)
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

func BuildSysStatementInfoFilter(curAccountId uint64) tree.Expr {
	return makeAccountIdEqualAst(curAccountId)
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

	relnameColName := tree.NewUnresolvedColName(catalog.SystemRelAttr_Name)

	mo_databaseConst := tree.NewNumVal(catalog.MO_DATABASE, catalog.MO_DATABASE, false, tree.P_char)
	mo_tablesConst := tree.NewNumVal(catalog.MO_TABLES, catalog.MO_TABLES, false, tree.P_char)
	mo_columnsConst := tree.NewNumVal(catalog.MO_COLUMNS, catalog.MO_COLUMNS, false, tree.P_char)
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

	att_relnameColName := tree.NewUnresolvedColName(catalog.SystemColAttr_RelName)

	att_dblnameColName := tree.NewUnresolvedColName(catalog.SystemColAttr_DBName)

	mo_catalogConst := tree.NewNumVal(catalog.MO_CATALOG, catalog.MO_CATALOG, false, tree.P_char)
	inValues := tree.NewTuple(tree.Exprs{mo_catalogConst})
	// datname in ('mo_catalog')
	inExpr := tree.NewComparisonExpr(tree.IN, att_dblnameColName, inValues)

	exprs := make([]tree.Expr, 0)
	for _, table := range predefinedTables {
		if _, ok := specialTables[table]; ok {
			continue
		}
		exprs = append(exprs, tree.NewNumVal(table, table, false, tree.P_char))
	}

	notInValues := tree.NewTuple(exprs)

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
	relkindColName := tree.NewUnresolvedColName(lColName)
	clusterConst := tree.NewNumVal(rValue, rValue, false, tree.P_char)
	return tree.NewComparisonExpr(tree.EQUAL, relkindColName, clusterConst)
}

// build ast expr: account_id = cur_accountId
func makeAccountIdEqualAst(curAccountId uint64) tree.Expr {
	accountIdColName := tree.NewUnresolvedColName("account_id")
	curAccountIdConst := tree.NewNumVal(curAccountId, strconv.Itoa(int(curAccountId)), false, tree.P_uint64)
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

// BuildAccountIdSubquery builds a subquery AST to convert account name to account_id.
// This function is primarily used for compatibility with mo-cloud's business-level usage of the account field.
//
// For account = 'xxx': returns account_id = (SELECT account_id FROM mo_catalog.mo_account WHERE account_name = 'xxx')
// For account IN ('xxx', 'yyy'): returns account_id IN (SELECT account_id FROM mo_catalog.mo_account WHERE account_name IN ('xxx', 'yyy'))
// For account LIKE 'xxx%': returns account_id IN (SELECT account_id FROM mo_catalog.mo_account WHERE account_name LIKE 'xxx%')
//
// The conversion allows mo-cloud to query statement_info table using account names instead of account_ids,
// providing a more user-friendly interface while maintaining compatibility with the underlying account_id-based storage.
func BuildAccountIdSubquery(op tree.ComparisonOp, accountValue tree.Expr) tree.Expr {
	subquery := buildMoAccountSubquery(op, accountValue)
	accountIdColName := tree.NewUnresolvedColName("account_id")

	// For EQUAL, use account_id = (SELECT ...)
	// For IN/LIKE, use account_id IN (SELECT ...)
	comparisonOp := tree.IN
	if op == tree.EQUAL {
		comparisonOp = tree.EQUAL
	}
	return tree.NewComparisonExpr(comparisonOp, accountIdColName, subquery)
}

// buildMoAccountSubquery builds a subquery to look up account_id from mo_catalog.mo_account table.
// The subquery format is: SELECT account_id FROM mo_catalog.mo_account WHERE account_name <op> <value>
//
// This helper function is used by BuildAccountIdSubquery to create the inner subquery that performs
// the account name to account_id conversion. It uses a table alias "ma" to ensure proper column binding.
func buildMoAccountSubquery(op tree.ComparisonOp, accountValue tree.Expr) *tree.Subquery {
	selectExpr := tree.SelectExpr{Expr: tree.NewUnresolvedColName("account_id")}
	moAccountTable := tree.NewTableName(
		tree.Identifier(catalog.MOAccountTable),
		tree.ObjectNamePrefix{
			SchemaName:     tree.Identifier(catalog.MO_CATALOG),
			ExplicitSchema: true,
		},
		nil,
	)
	// Use table-qualified column name with alias to ensure proper binding
	// Create aliased table expression
	aliasedTable := tree.NewAliasedTableExpr(moAccountTable, "ma", nil)
	// Use table alias qualified column name: ma.account_name
	accountNameCol := tree.NewUnresolvedName(
		tree.NewCStr("ma", 0),
		tree.NewCStr("account_name", 1),
	)
	whereCondition := tree.NewComparisonExpr(op, accountNameCol, accountValue)
	whereClause := tree.NewWhere(whereCondition)
	whereClause.Type = tree.AstWhere

	selectClause := &tree.SelectClause{
		Exprs: tree.SelectExprs{selectExpr},
		From:  tree.NewFrom(tree.TableExprs{aliasedTable}),
		Where: whereClause,
	}

	// Wrap SelectClause in Select for subquery binding
	selectStmt := tree.NewSelect(selectClause, nil, nil)
	return tree.NewSubquery(selectStmt, false)
}

// ConvertAccountToAccountId converts account column references to account_id subqueries in AST.
// This is a convenience wrapper that calls ConvertAccountToAccountIdWithTableCheck with an empty table alias map.
// For queries with table aliases, use ConvertAccountToAccountIdWithTableCheck instead.
func ConvertAccountToAccountId(astExpr tree.Expr, isSystemAccount bool, currentAccountName string, currentAccountID uint32) (tree.Expr, bool) {
	return ConvertAccountToAccountIdWithTableCheck(astExpr, isSystemAccount, currentAccountName, currentAccountID, nil)
}

// ConvertAccountToAccountIdWithTableCheck converts account column references to account_id subqueries in AST.
// This conversion is primarily designed for compatibility with mo-cloud's business-level usage of the account field.
//
// The function traverses the AST and replaces account column comparisons with account_id subqueries or direct account_id comparisons.
// For system accounts (account_id = 0), it converts account comparisons to subqueries that look up account_id from mo_catalog.mo_account.
// Special optimization: if account = 'sys', it directly converts to account_id = 0 without using a subquery for better performance.
// For non-system accounts, it only converts account = currentAccountName to account_id = currentAccountID, as other account filters
// are already implicitly restricted by the existing account_id filter.
//
// tableAliasMap is a map of table names/aliases that refer to statement_info table. If nil, only checks for unqualified column names
// or table name "statement_info". This map is used to verify if table-qualified column names (e.g., s.account) refer to statement_info.
//
// This allows mo-cloud to query statement_info table using account names (e.g., WHERE account = 'sys' or WHERE s.account = 'sys')
// instead of account_ids, providing a more intuitive interface while maintaining compatibility with the underlying account_id-based storage and access control.
//
// Returns the converted AST expression and a boolean indicating if any conversion was made.
func ConvertAccountToAccountIdWithTableCheck(astExpr tree.Expr, isSystemAccount bool, currentAccountName string, currentAccountID uint32, tableAliasMap map[string]bool) (tree.Expr, bool) {
	switch expr := astExpr.(type) {
	case *tree.ComparisonExpr:
		// Check if Left is account column from statement_info table
		if isAccountColumnFromStatementInfo(expr.Left, tableAliasMap) {
			if !isSystemAccount {
				// For non-system account, only convert if account = currentAccountName
				if expr.Op == tree.EQUAL {
					if accountValue, ok := extractAccountValue(expr.Right); ok && accountValue == currentAccountName {
						accountIdColName := tree.NewUnresolvedColName("account_id")
						accountIdConst := tree.NewNumVal(uint64(currentAccountID), strconv.Itoa(int(currentAccountID)), false, tree.P_uint64)
						return tree.NewComparisonExpr(tree.EQUAL, accountIdColName, accountIdConst), true
					}
				}
				// Other cases are ignored because account_id filter already exists
				return astExpr, false
			}
			// For system account, convert to subquery or direct comparison
			// Optimization: if account = 'sys', directly convert to account_id = 0 without subquery
			if expr.Op == tree.EQUAL {
				if accountValue, ok := extractAccountValue(expr.Right); ok && strings.EqualFold(accountValue, "sys") {
					accountIdColName := tree.NewUnresolvedColName("account_id")
					accountIdConst := tree.NewNumVal(uint64(0), "0", false, tree.P_uint64)
					return tree.NewComparisonExpr(tree.EQUAL, accountIdColName, accountIdConst), true
				}
			}
			// For other cases, use subquery
			return BuildAccountIdSubquery(expr.Op, expr.Right), true
		}
		// Recursively process Left and Right
		newLeft, leftConverted := ConvertAccountToAccountIdWithTableCheck(expr.Left, isSystemAccount, currentAccountName, currentAccountID, tableAliasMap)
		newRight, rightConverted := ConvertAccountToAccountIdWithTableCheck(expr.Right, isSystemAccount, currentAccountName, currentAccountID, tableAliasMap)
		if leftConverted || rightConverted {
			return &tree.ComparisonExpr{
				Op:     expr.Op,
				SubOp:  expr.SubOp,
				Left:   newLeft,
				Right:  newRight,
				Escape: expr.Escape,
			}, true
		}

	case *tree.AndExpr:
		newLeft, leftConverted := ConvertAccountToAccountIdWithTableCheck(expr.Left, isSystemAccount, currentAccountName, currentAccountID, tableAliasMap)
		newRight, rightConverted := ConvertAccountToAccountIdWithTableCheck(expr.Right, isSystemAccount, currentAccountName, currentAccountID, tableAliasMap)
		if leftConverted || rightConverted {
			return &tree.AndExpr{Left: newLeft, Right: newRight}, true
		}

	case *tree.OrExpr:
		newLeft, leftConverted := ConvertAccountToAccountIdWithTableCheck(expr.Left, isSystemAccount, currentAccountName, currentAccountID, tableAliasMap)
		newRight, rightConverted := ConvertAccountToAccountIdWithTableCheck(expr.Right, isSystemAccount, currentAccountName, currentAccountID, tableAliasMap)
		if leftConverted || rightConverted {
			return &tree.OrExpr{Left: newLeft, Right: newRight}, true
		}

	case *tree.NotExpr:
		newExpr, converted := ConvertAccountToAccountIdWithTableCheck(expr.Expr, isSystemAccount, currentAccountName, currentAccountID, tableAliasMap)
		if converted {
			return &tree.NotExpr{Expr: newExpr}, true
		}

	case *tree.ParenExpr:
		newExpr, converted := ConvertAccountToAccountIdWithTableCheck(expr.Expr, isSystemAccount, currentAccountName, currentAccountID, tableAliasMap)
		if converted {
			return &tree.ParenExpr{Expr: newExpr}, true
		}
	}

	return astExpr, false
}

// isAccountColumn checks if the expression is an account column reference.
// This is a convenience wrapper that calls isAccountColumnFromStatementInfo with nil tableAliasMap.
func isAccountColumn(expr tree.Expr) bool {
	return isAccountColumnFromStatementInfo(expr, nil)
}

// isAccountColumnFromStatementInfo checks if the expression is an account column reference from statement_info table.
// This is used to identify account column comparisons that need to be converted to account_id comparisons
// for compatibility with mo-cloud's business-level usage of the account field.
//
// For table-qualified column names (e.g., s.account or statement_info.account), it checks if the table name/alias
// refers to statement_info table using tableAliasMap. If tableAliasMap is nil, it only checks if table name is "statement_info".
// For unqualified column names, it returns true (will be checked by hasStatementInfoTable in caller).
func isAccountColumnFromStatementInfo(expr tree.Expr, tableAliasMap map[string]bool) bool {
	if unresolvedName, ok := expr.(*tree.UnresolvedName); ok {
		colName := unresolvedName.ColName()
		if !strings.EqualFold(colName, "account") {
			return false
		}
		// If column name has table qualification, check if table refers to statement_info
		if unresolvedName.NumParts >= 2 {
			tblName := unresolvedName.TblName()
			// Check if table name is "statement_info" (case-insensitive)
			if strings.EqualFold(tblName, catalog.MO_STATEMENT) {
				return true
			}
			// If tableAliasMap is provided, check if the table alias refers to statement_info
			if tableAliasMap != nil {
				return tableAliasMap[tblName]
			}
			// If no tableAliasMap and table name is not "statement_info", don't convert
			return false
		}
		// For unqualified column names, return true (will be checked by hasStatementInfoTable in caller)
		return true
	}
	return false
}

// extractAccountValue extracts the account value from an expression (for string literals).
// This helper function is used to extract account name values from AST expressions
// when converting account comparisons to account_id comparisons for mo-cloud compatibility.
func extractAccountValue(expr tree.Expr) (string, bool) {
	if numVal, ok := expr.(*tree.NumVal); ok {
		if numVal.ValType == tree.P_char {
			return numVal.String(), true
		}
	}
	return "", false
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
