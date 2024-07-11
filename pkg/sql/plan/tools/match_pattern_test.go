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
	pattern :=
		TNode(plan2.Node_PROJECT,
			TNode(plan2.Node_TABLE_SCAN).WithAlias(
				"L_ORDERKEY",
				TColumnRef("lineitem", "l_orderkey"),
			),
		).WithOutputs("L_ORDERKEY")
	err := AssertPlan(context.Background(), nil, "SELECT l_orderkey FROM lineitem", pattern)
	assert.Nil(t, err)
}

func Test_outputTwoColumns(t *testing.T) {
	p := TOutput([]string{"L_ORDERKEY", "L_ORDERKEY"},
		TTableScan("lineitem",
			NewStringMap(
				NewStringPair("L_ORDERKEY", "l_orderkey"),
			),
		),
	)
	err := AssertPlan(context.Background(), nil, "SELECT l_orderkey, l_orderkey FROM lineitem", p)
	assert.Nil(t, err)
}

func Test_outputTwoColumns2(t *testing.T) {
	p := TOutput([]string{"L_ORDERKEY", "L_ORDERKEY"},
		TTableScan("lineitem",
			NewStringMap(
				NewStringPair("L_ORDERKEY", "l_orderkey"),
			),
		),
	)
	err := AssertPlan(context.Background(), nil,
		"SELECT l_extendedprice, l_orderkey, l_discount, l_orderkey, l_linenumber FROM lineitem", p)
	assert.Nil(t, err)
}

func Test_strictOutput(t *testing.T) {
	p := TStrictOutput([]string{"L_ORDERKEY", "L_EXTENDEDPRICE"},
		TTableScan("lineitem",
			NewStringMap(
				NewStringPair("L_ORDERKEY", "l_orderkey"),
				NewStringPair("L_EXTENDEDPRICE", "l_extendedprice"),
			),
		),
	)
	err := AssertPlan(context.Background(), nil,
		"SELECT  l_orderkey, l_extendedprice FROM lineitem", p)
	assert.Nil(t, err)
}

func Test_strictTableScan(t *testing.T) {
	p := TOutput([]string{"L_ORDERKEY", "L_EXTENDEDPRICE"},
		TStrictTableScan("lineitem",
			NewStringMap(
				NewStringPair("L_ORDERKEY", "l_orderkey"),
				NewStringPair("L_EXTENDEDPRICE", "l_extendedprice"),
			),
		),
	)
	err := AssertPlan(context.Background(), nil,
		"SELECT  l_orderkey, l_extendedprice FROM lineitem", p)
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
	err := AssertPlan(context.Background(), nil,
		"SELECT  l_orderkey, 2 FROM lineitem group by l_orderkey", p)
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
	err := AssertPlan(context.Background(), nil,
		"SELECT  l_orderkey, 2 FROM lineitem", p)
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
	err := AssertPlan(context.Background(), nil,
		"SELECT  l_orderkey, 1 + l_orderkey FROM lineitem", p)
	assert.Nil(t, err)
}
