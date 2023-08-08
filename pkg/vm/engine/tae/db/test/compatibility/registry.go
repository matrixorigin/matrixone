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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

var (
	version = 1
)

type PrepareCase struct {
	typ        int
	desc       string
	prepareFn  func(tc PrepareCase, t *testing.T)
	getBatch   func(tc PrepareCase, t *testing.T) *containers.Batch
	getSchema  func(tc PrepareCase, t *testing.T) *catalog.Schema
	getEngine  func(tc PrepareCase, t *testing.T) *testutil.TestEngine
	getOptions func(tc PrepareCase, t *testing.T) *options.Options
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

func GetPrepareCase(typ int) PrepareCase {
	if prepareCase, ok := PrepareCases[typ]; ok {
		return prepareCase
	}
	panic("GetPrepareCase: prepare case not found")
}
