// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMySQLEffectiveColumnAttributes(t *testing.T) {
	attrs := make([]ColumnAttribute, 0, 32)
	attrs = append(attrs,
		&AttributeDefault{Expr: &NumVal{}},
		&AttributeDefault{Expr: &ParenExpr{Expr: &NumVal{}}},
		&AttributeNull{Is: false},
		&AttributeNull{Is: true},
		&AttributePrimaryKey{},
		&AttributeKey{},
		&AttributeUnique{},
		&AttributeUniqueKey{},
		&AttributeAutoIncrement{},
		&AttributeComment{},
		&AttributeOnUpdate{},
		&AttributeColumnFormat{},
		&AttributeStorage{},
		&AttributeSRID{},
		&AttributeVisable{},
		&AttributeCollate{Collate: "binary"},
		&AttributeCollate{Collate: "binary"},
	)
	original := append([]ColumnAttribute(nil), attrs...)
	attrs = append(attrs, attrs[:15]...)
	var indices []int
	for i, attr := range MySQLEffectiveColumnAttributes(attrs) {
		indices = append(indices, i)
		require.Same(t, attrs[i], attr)
	}
	require.Equal(t, []int{15, 16, 17, 18, 19, 20, 22, 24, 25, 26, 27, 28, 29, 30, 31}, indices)
	require.Equal(t, original, attrs[:len(original)])
	count := 0
	for range MySQLEffectiveColumnAttributes(attrs) {
		count++
		break
	}
	require.Equal(t, 1, count)
	for range MySQLEffectiveColumnAttributes(nil) {
		t.Fatal("empty list yielded")
	}
}

func TestMySQLColumnDefaultCollationOwnership(t *testing.T) {
	value := &NumVal{}
	collated := &CollateExpr{Expr: value, Collation: "binary"}
	unary := &UnaryExpr{Op: UNARY_MINUS, Expr: collated}
	expr, name := MySQLColumnDefaultExpression(unary)
	require.Equal(t, "binary", name)
	require.Same(t, value, expr.(*UnaryExpr).Expr)
	require.Same(t, collated, unary.Expr)
	grouped := &ParenExpr{Expr: collated}
	expr, name = MySQLColumnDefaultExpression(grouped)
	require.Empty(t, name)
	require.Same(t, grouped, expr)
	expr, name = MySQLColumnDefaultExpression(collated)
	require.Equal(t, "binary", name)
	require.Same(t, value, expr)
}
