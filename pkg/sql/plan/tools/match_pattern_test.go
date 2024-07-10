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
	"fmt"
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
	fmt.Println(pattern)
	err := AssertPlan(context.Background(), nil, "SELECT l_orderkey FROM lineitem", pattern)
	assert.Nil(t, err)
}
