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

package tree

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

type passthroughExprVisitor struct{}

func (passthroughExprVisitor) Enter(expr Expr) (Expr, bool) {
	return expr, false
}

func (passthroughExprVisitor) Exit(expr Expr) (Expr, bool) {
	return expr, true
}

func TestCaseExprAcceptHandlesOptionalOperands(t *testing.T) {
	num := func(value int64) *NumVal {
		return NewNumVal(value, strconv.FormatInt(value, 10), false, P_int64)
	}

	for _, tc := range []struct {
		name string
		expr *CaseExpr
	}{
		{
			name: "searched case has no input expression",
			expr: NewCaseExpr(
				nil,
				[]*When{NewWhen(num(1), num(1))},
				num(0),
			),
		},
		{
			name: "case without else",
			expr: NewCaseExpr(
				num(1),
				[]*When{NewWhen(num(1), num(1))},
				nil,
			),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			visited, ok := tc.expr.Accept(passthroughExprVisitor{})
			require.True(t, ok)
			require.Same(t, tc.expr, visited)
		})
	}
}
