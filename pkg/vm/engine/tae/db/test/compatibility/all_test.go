package compatibility

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

type PrepareStatementType int

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
