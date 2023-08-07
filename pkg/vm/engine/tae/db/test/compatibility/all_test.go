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
	prepareCase.prepareFn(
		prepareCase, t,
	)
}

func runAllPrepare(t *testing.T) {
	if os.Getenv("PREPCOMP") != "true" {
		t.Skip("skip prepare compatibility test")
		return
	}
	err := InitPrepareEnv()
	require.NoError(t, err)
	for _, prepareCase := range PrepareCases {
		runPrepareCase(prepareCase, t)
	}
	return
}

func runTestCase(testCase TestCase, t *testing.T) {
	testCase.testFn(
		testCase, t,
	)
}

func runAllTestCase(t *testing.T) {
	if os.Getenv("EXECCOMP") != "true" {
		t.Skip("skip execute compatibility test")
		return
	}
	err := EnsurePrepareEnvOK()
	require.NoError(t, err)
	err = InitExecuteEnv()
	require.NoError(t, err)
	for _, testCase := range TestCases {
		runTestCase(testCase, t)
	}
	return
}

func TestPrepare(t *testing.T) {
	runAllPrepare(t)
}

func TestExecute(t *testing.T) {
	runAllTestCase(t)
}
