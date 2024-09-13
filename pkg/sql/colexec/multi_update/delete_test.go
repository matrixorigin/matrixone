// Copyright 2021-2024 Matrix Origin
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

package multi_update

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func TestDeleteSimpleTable(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t)
	eng := prepareTestEng(ctrl)

	hasUniqueKey := false
	hasSecondaryKey := false
	isPartition := false

	case1 := buildDeleteTestCase(eng, hasUniqueKey, hasSecondaryKey, isPartition)

	runTestCases(t, proc, []*testCase{case1})
}

func TestDeleteTableWithUniqueKeyAndSecondaryKey(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t)
	eng := prepareTestEng(ctrl)

	hasUniqueKey := true
	hasSecondaryKey := true
	isPartition := false

	case1 := buildDeleteTestCase(eng, hasUniqueKey, hasSecondaryKey, isPartition)

	runTestCases(t, proc, []*testCase{case1})
}

func TestDeletePartitionTable(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t)
	eng := prepareTestEng(ctrl)

	hasUniqueKey := false
	hasSecondaryKey := false
	isPartition := true

	case1 := buildDeleteTestCase(eng, hasUniqueKey, hasSecondaryKey, isPartition)

	runTestCases(t, proc, []*testCase{case1})
}

// delete s3

// multi delete

// ----- util function ----
func buildDeleteTestCase(eng engine.Engine, hasUniqueKey bool, hasSecondaryKey bool, isPartition bool) *testCase {
	batchs, affectRows := prepareTestDeleteBatchs(hasUniqueKey, hasSecondaryKey, isPartition)
	multiUpdateCtxs := prepareTestDeleteMultiUpdateCtx(hasUniqueKey, hasSecondaryKey, isPartition)

	retCase := buildTestCase(multiUpdateCtxs, eng, batchs, affectRows)
	return retCase
}
