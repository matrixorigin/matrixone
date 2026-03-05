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

package frontend

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	util2 "github.com/matrixorigin/matrixone/pkg/util"
	"github.com/stretchr/testify/assert"
)

type mockCompile struct {
	runFunc     func(uint64) (*util2.RunResult, error)
	getPlanFunc func() *plan.Plan
	releaseFunc func()
}

func (m *mockCompile) Run(ts uint64) (*util2.RunResult, error) { return m.runFunc(ts) }
func (m *mockCompile) GetPlan() *plan.Plan                     { return m.getPlanFunc() }
func (m *mockCompile) Release()                                { m.releaseFunc() }
func (m *mockCompile) SetOriginSQL(s string)                   {}

func TestTxnComputationWrapper_Run(t *testing.T) {
	expectedResult := &util2.RunResult{AffectRows: 10}
	expectedPlan := &plan.Plan{}

	mockComp := &mockCompile{
		runFunc: func(ts uint64) (*util2.RunResult, error) {
			return expectedResult, nil
		},
		getPlanFunc: func() *plan.Plan {
			return expectedPlan
		},
		releaseFunc: func() {},
	}

	cwft := &TxnComputationWrapper{
		compile: mockComp,
	}

	// Test successful run
	res, err := cwft.Run(100)
	assert.NoError(t, err)
	assert.Equal(t, expectedResult, res)
	assert.Equal(t, expectedPlan, cwft.plan)
	assert.Equal(t, expectedResult, cwft.runResult)
	assert.Nil(t, cwft.compile) // Should be cleared after Run
}

func TestTxnComputationWrapper_Run_Error(t *testing.T) {
	expectedErr := assert.AnError
	expectedPlan := &plan.Plan{}

	mockComp := &mockCompile{
		runFunc: func(ts uint64) (*util2.RunResult, error) {
			return nil, expectedErr
		},
		getPlanFunc: func() *plan.Plan {
			return expectedPlan
		},
		releaseFunc: func() {},
	}

	cwft := &TxnComputationWrapper{
		compile: mockComp,
	}

	// Test error run
	res, err := cwft.Run(100)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.Nil(t, res)
	assert.Equal(t, expectedPlan, cwft.plan)
	assert.Nil(t, cwft.compile)
}
