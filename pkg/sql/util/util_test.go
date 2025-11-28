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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

type kase struct {
	a       string
	b       string
	want    string
	wantErr bool
}

func Test_MakeNameOfPartitionTable(t *testing.T) {
	kases := []kase{
		{partitionDelimiter, "abc", "", true},
		{"abc", partitionDelimiter, "", true},
		{"", "abc", "", true},
		{"abc", "", "", true},
		{"abc", "def", fmt.Sprintf("%sabc%sdef", partitionDelimiter, partitionDelimiter), false},
	}

	for _, k := range kases {
		r1, r11 := MakeNameOfPartitionTable(k.a, k.b)
		if k.wantErr {
			require.False(t, r1)
		} else {
			require.True(t, r1)
			require.Equal(t, k.want, r11)

			r2, a, b := SplitNameOfPartitionTable(r11)
			require.True(t, r2)
			require.Equal(t, a, k.a)
			require.Equal(t, b, k.b)
		}
	}
}

func Test_SplitNameOfPartitionTable(t *testing.T) {
	kases := []kase{
		{"", "", "abc", true},
		{"", "", partitionDelimiter + "abc", true},
		{"", "", partitionDelimiter, true},
		{"", "", partitionDelimiter + partitionDelimiter, true},
		{"", "", partitionDelimiter + "a" + partitionDelimiter, true},
		{"", "", partitionDelimiter + "" + partitionDelimiter + "b", true},
		{"a", "b", partitionDelimiter + "a" + partitionDelimiter + "b", false},
	}

	for _, k := range kases {
		r1, a, b := SplitNameOfPartitionTable(k.want)
		if k.wantErr {
			require.False(t, r1)
		} else {
			require.True(t, r1)
			require.Equal(t, a, k.a)
			require.Equal(t, b, k.b)
		}
	}
}

func TestConvertAccountToAccountIdWithTableCheck(t *testing.T) {
	tests := []struct {
		name               string
		astExpr            tree.Expr
		isSystemAccount    bool
		currentAccountName string
		currentAccountID   uint32
		accountIdResolver  AccountIdResolver
		wantConverted      bool
		checkResult        func(t *testing.T, result tree.Expr)
	}{
		{
			name: "system account - account equals (no resolver, should not convert)",
			astExpr: tree.NewComparisonExpr(
				tree.EQUAL,
				tree.NewUnresolvedColName("account"),
				tree.NewNumVal("test_account", "test_account", false, tree.P_char),
			),
			isSystemAccount:    true,
			currentAccountName: "",
			currentAccountID:   0,
			wantConverted:      false, // No resolver provided, keep original condition
			checkResult: func(t *testing.T, result tree.Expr) {
				compExpr, ok := result.(*tree.ComparisonExpr)
				require.True(t, ok)
				require.Equal(t, tree.EQUAL, compExpr.Op)
				require.Equal(t, "account", compExpr.Left.(*tree.UnresolvedName).ColName())
			},
		},
		{
			name: "system account - account equals sys (optimization)",
			astExpr: tree.NewComparisonExpr(
				tree.EQUAL,
				tree.NewUnresolvedColName("account"),
				tree.NewNumVal("sys", "sys", false, tree.P_char),
			),
			isSystemAccount:    true,
			currentAccountName: "",
			currentAccountID:   0,
			wantConverted:      true,
			checkResult: func(t *testing.T, result tree.Expr) {
				compExpr, ok := result.(*tree.ComparisonExpr)
				require.True(t, ok)
				require.Equal(t, tree.EQUAL, compExpr.Op)
				require.Equal(t, "account_id", compExpr.Left.(*tree.UnresolvedName).ColName())
				// Should be direct comparison, not subquery (optimization)
				_, ok = compExpr.Right.(*tree.Subquery)
				require.False(t, ok, "Right should NOT be a subquery for sys optimization")
				// Should be account_id = 0
				numVal, ok := compExpr.Right.(*tree.NumVal)
				require.True(t, ok, "Right should be a NumVal")
				require.Equal(t, "0", numVal.String())
			},
		},
		{
			name: "system account - account equals SYS (case insensitive)",
			astExpr: tree.NewComparisonExpr(
				tree.EQUAL,
				tree.NewUnresolvedColName("account"),
				tree.NewNumVal("SYS", "SYS", false, tree.P_char),
			),
			isSystemAccount:    true,
			currentAccountName: "",
			currentAccountID:   0,
			wantConverted:      true,
			checkResult: func(t *testing.T, result tree.Expr) {
				compExpr, ok := result.(*tree.ComparisonExpr)
				require.True(t, ok)
				require.Equal(t, tree.EQUAL, compExpr.Op)
				require.Equal(t, "account_id", compExpr.Left.(*tree.UnresolvedName).ColName())
				// Should be direct comparison, not subquery (optimization)
				_, ok = compExpr.Right.(*tree.Subquery)
				require.False(t, ok, "Right should NOT be a subquery for sys optimization")
				// Should be account_id = 0
				numVal, ok := compExpr.Right.(*tree.NumVal)
				require.True(t, ok, "Right should be a NumVal")
				require.Equal(t, "0", numVal.String())
			},
		},
		{
			name: "system account - table qualified column (s.account)",
			astExpr: tree.NewComparisonExpr(
				tree.EQUAL,
				tree.NewUnresolvedName(
					tree.NewCStr("s", 0),
					tree.NewCStr("account", 1),
				),
				tree.NewNumVal("sys", "sys", false, tree.P_char),
			),
			isSystemAccount:    true,
			currentAccountName: "",
			currentAccountID:   0,
			wantConverted:      false, // 表别名 "s" 在没有 tableAliasMap 时不会被转换
			checkResult: func(t *testing.T, result tree.Expr) {
				// 没有 tableAliasMap 时，表别名不会被转换
				compExpr, ok := result.(*tree.ComparisonExpr)
				require.True(t, ok)
				unresolvedName, ok := compExpr.Left.(*tree.UnresolvedName)
				require.True(t, ok)
				require.Equal(t, "account", unresolvedName.ColName())
				require.Equal(t, "s", unresolvedName.TblName())
			},
		},
		{
			name: "system account - full table name qualified column (statement_info.account)",
			astExpr: tree.NewComparisonExpr(
				tree.EQUAL,
				tree.NewUnresolvedName(
					tree.NewCStr("statement_info", 0),
					tree.NewCStr("account", 1),
				),
				tree.NewNumVal("sys", "sys", false, tree.P_char),
			),
			isSystemAccount:    true,
			currentAccountName: "",
			currentAccountID:   0,
			wantConverted:      true, // 完整表名 "statement_info" 会被转换
			checkResult: func(t *testing.T, result tree.Expr) {
				// 应该被转换为 account_id = 0
				compExpr, ok := result.(*tree.ComparisonExpr)
				require.True(t, ok)
				require.Equal(t, tree.EQUAL, compExpr.Op)
				require.Equal(t, "account_id", compExpr.Left.(*tree.UnresolvedName).ColName())
				// Should be direct comparison, not subquery (optimization)
				_, ok = compExpr.Right.(*tree.Subquery)
				require.False(t, ok, "Right should NOT be a subquery for sys optimization")
				// Should be account_id = 0
				numVal, ok := compExpr.Right.(*tree.NumVal)
				require.True(t, ok, "Right should be a NumVal")
				require.Equal(t, "0", numVal.String())
			},
		},
		{
			name: "system account - account IN (should not convert)",
			astExpr: tree.NewComparisonExpr(
				tree.IN,
				tree.NewUnresolvedColName("account"),
				tree.NewTuple(tree.Exprs{
					tree.NewNumVal("acc1", "acc1", false, tree.P_char),
					tree.NewNumVal("acc2", "acc2", false, tree.P_char),
				}),
			),
			isSystemAccount:    true,
			currentAccountName: "",
			currentAccountID:   0,
			wantConverted:      false, // IN operation is not converted
			checkResult: func(t *testing.T, result tree.Expr) {
				compExpr, ok := result.(*tree.ComparisonExpr)
				require.True(t, ok)
				require.Equal(t, tree.IN, compExpr.Op)
				require.Equal(t, "account", compExpr.Left.(*tree.UnresolvedName).ColName())
			},
		},
		{
			name: "non-system account - account equals currentAccountName (should not convert)",
			astExpr: tree.NewComparisonExpr(
				tree.EQUAL,
				tree.NewUnresolvedColName("account"),
				tree.NewNumVal("my_account", "my_account", false, tree.P_char),
			),
			isSystemAccount:    false,
			currentAccountName: "my_account",
			currentAccountID:   100,
			wantConverted:      false, // Non-system account: no conversion, account_id filter already in tablescan
			checkResult: func(t *testing.T, result tree.Expr) {
				compExpr, ok := result.(*tree.ComparisonExpr)
				require.True(t, ok)
				require.Equal(t, "account", compExpr.Left.(*tree.UnresolvedName).ColName())
			},
		},
		{
			name: "non-system account - account not equals currentAccountName",
			astExpr: tree.NewComparisonExpr(
				tree.EQUAL,
				tree.NewUnresolvedColName("account"),
				tree.NewNumVal("other_account", "other_account", false, tree.P_char),
			),
			isSystemAccount:    false,
			currentAccountName: "my_account",
			currentAccountID:   100,
			wantConverted:      false,
			checkResult: func(t *testing.T, result tree.Expr) {
				compExpr, ok := result.(*tree.ComparisonExpr)
				require.True(t, ok)
				require.Equal(t, "account", compExpr.Left.(*tree.UnresolvedName).ColName())
			},
		},
		{
			name: "non-system account - account IN (should not convert)",
			astExpr: tree.NewComparisonExpr(
				tree.IN,
				tree.NewUnresolvedColName("account"),
				tree.NewTuple(tree.Exprs{
					tree.NewNumVal("acc1", "acc1", false, tree.P_char),
				}),
			),
			isSystemAccount:    false,
			currentAccountName: "my_account",
			currentAccountID:   100,
			wantConverted:      false,
			checkResult: func(t *testing.T, result tree.Expr) {
				compExpr, ok := result.(*tree.ComparisonExpr)
				require.True(t, ok)
				require.Equal(t, "account", compExpr.Left.(*tree.UnresolvedName).ColName())
			},
		},
		{
			name: "non-account column - should not convert",
			astExpr: tree.NewComparisonExpr(
				tree.EQUAL,
				tree.NewUnresolvedColName("user_id"),
				tree.NewNumVal(int64(123), "123", false, tree.P_int64),
			),
			isSystemAccount:    true,
			currentAccountName: "",
			currentAccountID:   0,
			wantConverted:      false,
			checkResult: func(t *testing.T, result tree.Expr) {
				compExpr, ok := result.(*tree.ComparisonExpr)
				require.True(t, ok)
				require.Equal(t, "user_id", compExpr.Left.(*tree.UnresolvedName).ColName())
			},
		},
		{
			name: "AND expression with account (sys)",
			astExpr: tree.NewAndExpr(
				tree.NewComparisonExpr(
					tree.EQUAL,
					tree.NewUnresolvedColName("account"),
					tree.NewNumVal("sys", "sys", false, tree.P_char),
				),
				tree.NewComparisonExpr(
					tree.EQUAL,
					tree.NewUnresolvedColName("status"),
					tree.NewNumVal("active", "active", false, tree.P_char),
				),
			),
			isSystemAccount:    true,
			currentAccountName: "",
			currentAccountID:   0,
			wantConverted:      true,
			checkResult: func(t *testing.T, result tree.Expr) {
				andExpr, ok := result.(*tree.AndExpr)
				require.True(t, ok)
				leftComp, ok := andExpr.Left.(*tree.ComparisonExpr)
				require.True(t, ok)
				require.Equal(t, "account_id", leftComp.Left.(*tree.UnresolvedName).ColName())
			},
		},
		{
			name: "OR expression with account (sys)",
			astExpr: tree.NewOrExpr(
				tree.NewComparisonExpr(
					tree.EQUAL,
					tree.NewUnresolvedColName("account"),
					tree.NewNumVal("sys", "sys", false, tree.P_char),
				),
				tree.NewComparisonExpr(
					tree.EQUAL,
					tree.NewUnresolvedColName("account"),
					tree.NewNumVal("sys", "sys", false, tree.P_char),
				),
			),
			isSystemAccount:    true,
			currentAccountName: "",
			currentAccountID:   0,
			wantConverted:      true,
			checkResult: func(t *testing.T, result tree.Expr) {
				orExpr, ok := result.(*tree.OrExpr)
				require.True(t, ok)
				leftComp, ok := orExpr.Left.(*tree.ComparisonExpr)
				require.True(t, ok)
				require.Equal(t, "account_id", leftComp.Left.(*tree.UnresolvedName).ColName())
				rightComp, ok := orExpr.Right.(*tree.ComparisonExpr)
				require.True(t, ok)
				require.Equal(t, "account_id", rightComp.Left.(*tree.UnresolvedName).ColName())
			},
		},
		{
			name: "NOT expression with account (sys)",
			astExpr: &tree.NotExpr{
				Expr: tree.NewComparisonExpr(
					tree.EQUAL,
					tree.NewUnresolvedColName("account"),
					tree.NewNumVal("sys", "sys", false, tree.P_char),
				),
			},
			isSystemAccount:    true,
			currentAccountName: "",
			currentAccountID:   0,
			wantConverted:      true,
			checkResult: func(t *testing.T, result tree.Expr) {
				notExpr, ok := result.(*tree.NotExpr)
				require.True(t, ok)
				compExpr, ok := notExpr.Expr.(*tree.ComparisonExpr)
				require.True(t, ok)
				require.Equal(t, "account_id", compExpr.Left.(*tree.UnresolvedName).ColName())
			},
		},
		{
			name: "ParenExpr with account (sys)",
			astExpr: &tree.ParenExpr{
				Expr: tree.NewComparisonExpr(
					tree.EQUAL,
					tree.NewUnresolvedColName("account"),
					tree.NewNumVal("sys", "sys", false, tree.P_char),
				),
			},
			isSystemAccount:    true,
			currentAccountName: "",
			currentAccountID:   0,
			wantConverted:      true,
			checkResult: func(t *testing.T, result tree.Expr) {
				parenExpr, ok := result.(*tree.ParenExpr)
				require.True(t, ok)
				compExpr, ok := parenExpr.Expr.(*tree.ComparisonExpr)
				require.True(t, ok)
				require.Equal(t, "account_id", compExpr.Left.(*tree.UnresolvedName).ColName())
			},
		},
		{
			name: "complex nested expression (sys)",
			astExpr: tree.NewAndExpr(
				&tree.ParenExpr{
					Expr: tree.NewOrExpr(
						tree.NewComparisonExpr(
							tree.EQUAL,
							tree.NewUnresolvedColName("account"),
							tree.NewNumVal("sys", "sys", false, tree.P_char),
						),
						tree.NewComparisonExpr(
							tree.EQUAL,
							tree.NewUnresolvedColName("account"),
							tree.NewNumVal("sys", "sys", false, tree.P_char),
						),
					),
				},
				tree.NewComparisonExpr(
					tree.EQUAL,
					tree.NewUnresolvedColName("status"),
					tree.NewNumVal("active", "active", false, tree.P_char),
				),
			),
			isSystemAccount:    true,
			currentAccountName: "",
			currentAccountID:   0,
			wantConverted:      true,
			checkResult: func(t *testing.T, result tree.Expr) {
				andExpr, ok := result.(*tree.AndExpr)
				require.True(t, ok)
				parenExpr, ok := andExpr.Left.(*tree.ParenExpr)
				require.True(t, ok)
				orExpr, ok := parenExpr.Expr.(*tree.OrExpr)
				require.True(t, ok)
				leftComp, ok := orExpr.Left.(*tree.ComparisonExpr)
				require.True(t, ok)
				require.Equal(t, "account_id", leftComp.Left.(*tree.UnresolvedName).ColName())
			},
		},
		{
			name: "system account - account equals with resolver (account found)",
			astExpr: tree.NewComparisonExpr(
				tree.EQUAL,
				tree.NewUnresolvedColName("account"),
				tree.NewNumVal("test_account", "test_account", false, tree.P_char),
			),
			isSystemAccount:    true,
			currentAccountName: "",
			currentAccountID:   0,
			accountIdResolver: func(accountName string) (uint32, error) {
				if accountName == "test_account" {
					return 123, nil
				}
				return 0, nil
			},
			wantConverted: true,
			checkResult: func(t *testing.T, result tree.Expr) {
				compExpr, ok := result.(*tree.ComparisonExpr)
				require.True(t, ok)
				require.Equal(t, tree.EQUAL, compExpr.Op)
				require.Equal(t, "account_id", compExpr.Left.(*tree.UnresolvedName).ColName())
				numVal, ok := compExpr.Right.(*tree.NumVal)
				require.True(t, ok, "Right should be a NumVal")
				val, _ := numVal.Uint64()
				require.Equal(t, uint64(123), val)
			},
		},
		{
			name: "system account - account equals with resolver (account not found)",
			astExpr: tree.NewComparisonExpr(
				tree.EQUAL,
				tree.NewUnresolvedColName("account"),
				tree.NewNumVal("non_exist_account", "non_exist_account", false, tree.P_char),
			),
			isSystemAccount:    true,
			currentAccountName: "",
			currentAccountID:   0,
			accountIdResolver: func(accountName string) (uint32, error) {
				// Account not found, return 0
				return 0, nil
			},
			wantConverted: false,
			checkResult: func(t *testing.T, result tree.Expr) {
				compExpr, ok := result.(*tree.ComparisonExpr)
				require.True(t, ok)
				require.Equal(t, "account", compExpr.Left.(*tree.UnresolvedName).ColName())
			},
		},
		{
			name: "system account - account equals with resolver (resolver error)",
			astExpr: tree.NewComparisonExpr(
				tree.EQUAL,
				tree.NewUnresolvedColName("account"),
				tree.NewNumVal("test_account", "test_account", false, tree.P_char),
			),
			isSystemAccount:    true,
			currentAccountName: "",
			currentAccountID:   0,
			accountIdResolver: func(accountName string) (uint32, error) {
				return 0, moerr.NewInternalErrorNoCtx("resolver error")
			},
			wantConverted: false,
			checkResult: func(t *testing.T, result tree.Expr) {
				compExpr, ok := result.(*tree.ComparisonExpr)
				require.True(t, ok)
				require.Equal(t, "account", compExpr.Left.(*tree.UnresolvedName).ColName())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, converted := ConvertAccountToAccountIdWithTableCheck(
				tt.astExpr,
				tt.isSystemAccount,
				tt.currentAccountName,
				tt.currentAccountID,
				nil, // tableAliasMap
				tt.accountIdResolver,
			)
			require.Equal(t, tt.wantConverted, converted, "conversion status mismatch")
			if tt.checkResult != nil {
				tt.checkResult(t, result)
			}
		})
	}
}

func TestExtractAccountValue(t *testing.T) {
	tests := []struct {
		name     string
		expr     tree.Expr
		expected string
		ok       bool
	}{
		{
			name:     "string literal",
			expr:     tree.NewNumVal("test_account", "test_account", false, tree.P_char),
			expected: "test_account",
			ok:       true,
		},
		{
			name:     "non-string literal",
			expr:     tree.NewNumVal(int64(123), "123", false, tree.P_int64),
			expected: "",
			ok:       false,
		},
		{
			name:     "not NumVal",
			expr:     tree.NewUnresolvedColName("account"),
			expected: "",
			ok:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := extractAccountValue(tt.expr)
			require.Equal(t, tt.ok, ok)
			if tt.ok {
				require.Equal(t, tt.expected, result)
			}
		})
	}
}
