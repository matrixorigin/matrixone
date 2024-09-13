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

	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func TestInsertSimpleTable(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t)
	eng := prepareTestEng(ctrl)

	hasUniqueKey := false
	hasSecondaryKey := false
	isPartition := false

	case1 := buildInsertTestCase(eng, hasUniqueKey, hasSecondaryKey, isPartition)

	runTestCases(t, proc, []*testCase{case1})
}

func TestInsertTableWithUniqueKeyAndSecondaryKey(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t)
	eng := prepareTestEng(ctrl)

	hasUniqueKey := true
	hasSecondaryKey := true
	isPartition := false

	case1 := buildInsertTestCase(eng, hasUniqueKey, hasSecondaryKey, isPartition)

	runTestCases(t, proc, []*testCase{case1})
}

func TestInsertPartitionTable(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t)
	eng := prepareTestEng(ctrl)

	hasUniqueKey := false
	hasSecondaryKey := false
	isPartition := true

	case1 := buildInsertTestCase(eng, hasUniqueKey, hasSecondaryKey, isPartition)

	runTestCases(t, proc, []*testCase{case1})
}

// insert s3

// ----- util function ----
func buildInsertTestCase(eng engine.Engine, hasUniqueKey bool, hasSecondaryKey bool, isPartition bool) *testCase {
	batchs, affectRows := prepareTestInsertBatchs(hasUniqueKey, hasSecondaryKey, isPartition)
	multiUpdateCtxs := prepareTestMultiUpdateCtx(hasUniqueKey, hasSecondaryKey, isPartition)

	retCase := &testCase{
		op: &MultiUpdate{
			ctr:                    container{},
			MultiUpdateCtx:         multiUpdateCtxs,
			ToWriteS3:              false,
			IsOnduplicateKeyUpdate: false,
			Engine:                 eng,
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
		},
		inputBatchs:  batchs,
		expectErr:    false,
		affectedRows: affectRows,
	}

	return retCase
}
