// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tools

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func Test_output(t *testing.T) {
	p :=
		TNode(plan2.Node_PROJECT,
			TNode(plan2.Node_TABLE_SCAN).WithAlias(
				"L_ORDERKEY",
				TColumnRef("lineitem", "l_orderkey"),
			),
		).WithOutputs("L_ORDERKEY")
	err := AssertPlan(context.Background(), "SELECT l_orderkey FROM lineitem", p)
	assert.Nil(t, err)
}

func Test_outputTwoColumns(t *testing.T) {
	p :=
		TOutput([]string{"L_ORDERKEY", "L_ORDERKEY"},
			TTableScan("lineitem",
				NewStringMap(
					NewStringPair("L_ORDERKEY", "l_orderkey"),
				),
			),
		)
	err := AssertPlan(context.Background(), "SELECT l_orderkey, l_orderkey FROM lineitem", p)
	assert.Nil(t, err)
}

func Test_outputTwoColumns2(t *testing.T) {
	p :=
		TOutput([]string{"L_ORDERKEY", "L_ORDERKEY"},
			TTableScan("lineitem",
				NewStringMap(
					NewStringPair("L_ORDERKEY", "l_orderkey"),
				),
			),
		)
	err := AssertPlan(context.Background(), "SELECT l_extendedprice, l_orderkey, l_discount, l_orderkey, l_linenumber FROM lineitem", p)
	assert.Nil(t, err)
}

func Test_strictOutput(t *testing.T) {
	p :=
		TStrictOutput([]string{"L_ORDERKEY", "L_EXTENDEDPRICE"},
			TTableScan("lineitem",
				NewStringMap(
					NewStringPair("L_ORDERKEY", "l_orderkey"),
					NewStringPair("L_EXTENDEDPRICE", "l_extendedprice"),
				),
			),
		)
	err := AssertPlan(context.Background(), "SELECT  l_orderkey, l_extendedprice FROM lineitem", p)
	assert.Nil(t, err)
}

func Test_strictTableScan(t *testing.T) {
	p :=
		TOutput([]string{"L_ORDERKEY", "L_EXTENDEDPRICE"},
			TStrictTableScan("lineitem",
				NewStringMap(
					NewStringPair("L_ORDERKEY", "l_orderkey"),
					NewStringPair("L_EXTENDEDPRICE", "l_extendedprice"),
				),
			),
		)
	err := AssertPlan(context.Background(), "SELECT  l_orderkey, l_extendedprice FROM lineitem", p)
	assert.Nil(t, err)
}

func Test_constant(t *testing.T) {
	//mo does not have the OUTPUT node
	p :=
		TOutput([]string{"L_ORDERKEY"},
			TAnyTree(
				TTableScan("lineitem",
					NewStringMap(
						NewStringPair("L_ORDERKEY", "l_orderkey"),
					),
				),
			),
		)
	err := AssertPlan(context.Background(), "SELECT  l_orderkey, 2 FROM lineitem group by l_orderkey", p)
	assert.Nil(t, err)
}

func Test_aliasConstant(t *testing.T) {
	p :=
		TProject(
			NewAssignMap(NewAssignPair("TWO", TExpr("2"))),
			TTableScan("lineitem",
				NewStringMap(
					NewStringPair("L_ORDERKEY", "l_orderkey"),
				),
			),
		)
	err := AssertPlan(context.Background(), "SELECT  l_orderkey, 2 FROM lineitem", p)
	assert.Nil(t, err)
}

func Test_aliasExpr(t *testing.T) {
	p :=
		TProject(
			NewAssignMap(NewAssignPair("EXPR", TExpr("1 + L_ORDERKEY"))),
			TTableScan("lineitem",
				NewStringMap(
					NewStringPair("L_ORDERKEY", "l_orderkey"),
				),
			),
		)
	err := AssertPlan(context.Background(), "SELECT  l_orderkey, 1 + l_orderkey FROM lineitem", p)
	assert.Nil(t, err)
}

func Test_strictProject(t *testing.T) {
	p :=
		TStrictProject(
			NewAssignMap(
				NewAssignPair("EXPR", TExpr("1 + L_ORDERKEY")),
				NewAssignPair("L_ORDERKEY", TExpr("L_ORDERKEY")),
			),
			TTableScan("lineitem",
				NewStringMap(
					NewStringPair("L_ORDERKEY", "l_orderkey"),
				),
			),
		)
	err := AssertPlan(context.Background(), "SELECT  l_orderkey, 1 + l_orderkey FROM lineitem", p)
	assert.Nil(t, err)
}

func Test_identityAlias(t *testing.T) {
	p :=
		TProject(
			NewAssignMap(
				NewAssignPair("L_ORDERKEY", TExpr("L_ORDERKEY")),
				NewAssignPair("EXPR", TExpr("1 + L_ORDERKEY")),
			),
			TTableScan("lineitem",
				NewStringMap(
					NewStringPair("L_ORDERKEY", "l_orderkey"),
				),
			),
		)
	err := AssertPlan(context.Background(), "SELECT  l_orderkey, 1 + l_orderkey FROM lineitem", p)
	assert.Nil(t, err)
}

func Test_tableScan(t *testing.T) {
	p :=
		TOutput(
			[]string{
				"L_ORDERKEY",
			},
			TTableScan("lineitem",
				NewStringMap(
					NewStringPair("L_ORDERKEY", "l_orderkey"),
				),
			),
		)
	err := AssertPlan(context.Background(), "SELECT  l_orderkey FROM lineitem", p)
	assert.Nil(t, err)
}

func Test_joinMatcher(t *testing.T) {
	p :=
		TAnyTree(
			TJoin(plan2.Node_INNER,
				[]string{"LINEITEM_OK = ORDERS_OK"},
				nil,
				TTableScanWithoutColRef("lineitem").
					WithAlias("LINEITEM_OK",
						TColumnRef("lineitem", "l_orderkey")),

				TTableScanWithoutColRef("orders").
					WithAlias("ORDERS_OK",
						TColumnRef("orders", "o_orderkey")),
			),
		)

	err := AssertPlan(context.Background(), "SELECT o.o_orderkey FROM orders o, lineitem l WHERE l.l_orderkey = o.o_orderkey", p)
	assert.Nil(t, err)
}

func Test_selfJoin(t *testing.T) {
	p :=
		TAnyTree(
			TJoin(plan2.Node_INNER,
				[]string{"L_ORDERS_OK = R_ORDERS_OK"},
				nil,
				TTableScanWithoutColRef("orders").
					WithAlias("L_ORDERS_OK",
						TColumnRef("orders", "o_orderkey")),

				TTableScanWithoutColRef("orders").
					WithAlias("R_ORDERS_OK",
						TColumnRef("orders", "o_orderkey")),
			),
		)

	err := AssertPlan(context.Background(), "SELECT l.o_orderkey FROM orders l, orders r WHERE l.o_orderkey = r.o_orderkey", p)
	assert.Nil(t, err)
}

func Test_aggr(t *testing.T) {
	p :=
		TOutput(
			[]string{"COUNT"},
			TAggr(
				NewAggrMap(
					NewAggrPair("COUNT", NewAggrFuncMatcher("COUNT(N_NATIONKEY)")),
				),
				TTableScan("nation",
					NewStringMap(
						NewStringPair("N_NATIONKEY", "n_nationkey"),
					),
				),
			),
		)

	err := AssertPlan(context.Background(), "SELECT COUNT(n_nationkey) FROM nation", p)
	assert.Nil(t, err)
}

func Test_aliasDoesNotExists(t *testing.T) {
	p :=
		TNode(plan2.Node_PROJECT,
			TNode(plan2.Node_TABLE_SCAN).WithAlias("ORDERKEY",
				TColumnRef("lineitem", "xxxxx")))

	err := AssertPlan(context.Background(), "SELECT l_orderkey FROM lineitem", p)
	assert.Error(t, err)
}

func Test_referAliasDoesNotExists(t *testing.T) {
	p :=
		TOutput([]string{"xxxx"},
			TTableScan("lineitem",
				NewStringMap(
					NewStringPair("ORDERKEY", "l_orderkey"),
				),
			),
		)

	err := AssertPlan(context.Background(), "SELECT l_orderkey FROM lineitem", p)
	assert.Error(t, err)
}

func Test_strictOutputExtraSymbols(t *testing.T) {
	p :=
		TStrictOutput([]string{"ORDERKEY"},
			TTableScan("lineitem",
				NewStringMap(
					NewStringPair("ORDERKEY", "l_orderkey"),
					NewStringPair("EXTENDEDPRICE", "l_extendedprice"),
				),
			),
		)

	err := AssertPlan(context.Background(), "SELECT l_orderkey, l_extendedprice FROM lineitem", p)
	assert.Error(t, err)
}

func Test_strictTableScanExtraSymbols(t *testing.T) {
	p :=
		TOutput([]string{"ORDERKEY", "EXTENDEDPRICE"},
			TStrictTableScan("lineitem",
				NewStringMap(
					NewStringPair("ORDERKEY", "l_orderkey"),
				),
			),
		)

	err := AssertPlan(context.Background(), "SELECT l_orderkey, l_extendedprice FROM lineitem", p)
	assert.Error(t, err)
}

func Test_strictProjectExtraSymbols(t *testing.T) {
	p :=
		TStrictProject(
			NewAssignMap(
				NewAssignPair("ORDERKEY", TExpr("l_orderkey")),
				NewAssignPair("EXPR", TExpr("1 + l_orderkey")),
			),
			TTableScan("lineitem",
				NewStringMap(
					NewStringPair("ORDERKEY", "l_orderkey"),
				),
			),
		)

	err := AssertPlan(context.Background(), "SELECT l_discount, l_orderkey, 1 + l_orderkey FROM lineitem", p)
	assert.Error(t, err)
}

func Test_duplicateAliases(t *testing.T) {
	p :=
		TAnyTree(
			TJoin(plan2.Node_INNER,
				[]string{"ORDERS_OK", "LINEITEM_OK"},
				nil,
				TAnyTree(
					TTableScanWithoutColRef("orders").WithAlias("ORDERS_OK",
						TColumnRef("orders", "o_orderkey")),
				),
				TAnyTree(
					TTableScanWithoutColRef("lineitem").WithAlias("ORDERS_OK",
						TColumnRef("lineitem", "l_orderkey")),
				),
			),
		)

	err := AssertPlan(context.Background(), "SELECT o.o_orderkey FROM orders o, lineitem l WHERE l.l_orderkey = o.o_orderkey", p)
	assert.Error(t, err)
}

func Test_projectLimit(t *testing.T) {
	p :=
		TProject(
			NewAssignMap(
				NewAssignPair("EXPR", TExpr("1 + X_ORDERKEY")),
			),
			TTableScan("lineitem",
				NewStringMap(
					NewStringPair("L_ORDERKEY", "l_orderkey"),
				),
			),
		)
	err := AssertPlan(context.Background(), "SELECT  1 + l_orderkey FROM lineitem", p)
	assert.Error(t, err)
}
