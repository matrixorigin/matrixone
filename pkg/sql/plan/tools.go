// Copyright 2021 - 2022 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"go/constant"
)

const (
	moRecursiveLevelCol      = "__mo_recursive_level_col"
	moDefaultRecursionMax    = 100
	moCheckRecursionLevelFun = "mo_check_level"
)

func makeZeroRecursiveLevel() tree.SelectExpr {
	return tree.SelectExpr{
		Expr: tree.NewNumValWithType(constant.MakeInt64(0), "0", false, tree.P_int64),
		As:   tree.NewCStr(moRecursiveLevelCol, 1),
	}

}

func makePlusRecursiveLevel(name string) tree.SelectExpr {
	a := tree.SetUnresolvedName(name, moRecursiveLevelCol)
	b := tree.NewNumValWithType(constant.MakeInt64(1), "1", false, tree.P_int64)
	return tree.SelectExpr{
		Expr: tree.NewBinaryExpr(tree.PLUS, a, b),
		As:   tree.NewCStr("", 1),
	}
}
