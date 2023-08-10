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

package compatibility

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func runPrepareCase(prepareCase PrepareCase, t *testing.T) {
	t.Logf("Prepare case: [%s: %s]", prepareCase.name, prepareCase.desc)
	prepareCase.prepareFn(
		prepareCase, t,
	)
}

func runAllPrepare(t *testing.T) {
	t.Log("Run all compatibility prepare cases")
	err := InitPrepareEnv()
	require.NoError(t, err)
	for _, prepareCase := range PrepareCases {
		runPrepareCase(prepareCase, t)
	}
}

func runTestCase(testCase TestCase, t *testing.T) {
	t.Logf("Execute case: [%s: %s]", testCase.name, testCase.desc)
	testCase.testFn(
		testCase, t,
	)
}

func runAllTestCase(t *testing.T) {
	t.Log("Run all compatibility test cases")
	err := EnsurePrepareEnvOK()
	require.NoError(t, err)
	err = InitExecuteEnv()
	require.NoError(t, err)
	for _, testCase := range TestCases {
		runTestCase(testCase, t)
	}
}

func TestAll(t *testing.T) {
	prepareEnv := os.Getenv("PREPCOMP")
	execEnv := os.Getenv("EXECCOMP")
	if prepareEnv == "false" && execEnv == "false" {
		t.Skip("skip compatibility test")
	} else if prepareEnv == "true" {
		runAllPrepare(t)
	} else if execEnv == "true" {
		runAllTestCase(t)
	} else {
		runAllPrepare(t)
		runAllTestCase(t)
	}
}
