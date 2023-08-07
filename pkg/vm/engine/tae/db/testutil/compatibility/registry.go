package compatibility

import "testing"

var (
	version = 1
)

type PrepareCase struct {
	typ       int
	desc      string
	prepareFn func(tc PrepareCase, t *testing.T)
}

type TestCase struct {
	name      string
	desc      string
	dependsOn int
	testFn    func(tc TestCase, t *testing.T)
}

var PrepareCases map[int]PrepareCase
var TestCases map[string]TestCase

func TestCaseRegister(testCase TestCase) {
	if TestCases == nil {
		TestCases = make(map[string]TestCase)
	}
	if _, ok := TestCases[testCase.name]; ok {
		panic("TestCaseRegister: duplicate test case name")
	}
	TestCases[testCase.name] = testCase
}

func PrepareCaseRegister(prepareCase PrepareCase) {
	if PrepareCases == nil {
		PrepareCases = make(map[int]PrepareCase)
	}
	if _, ok := PrepareCases[prepareCase.typ]; ok {
		panic("PrepareCaseRegister: duplicate prepare case type")
	}
	PrepareCases[prepareCase.typ] = prepareCase
}
