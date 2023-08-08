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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/stretchr/testify/assert"
)

var (
	version = 1
)

func initPrepareTest(pc PrepareCase, opts *options.Options, t *testing.T) *testutil.TestEngine {
	dir, err := InitPrepareDirByType(pc.typ)
	assert.NoError(t, err)
	ctx := context.Background()
	tae := testutil.NewTestEngineWithDir(ctx, dir, t, opts)
	tae.BindSchema(pc.getSchema(pc, t))
	return tae
}
func initTestEngine(tc TestCase, t *testing.T) *testutil.TestEngine {
	pc := GetPrepareCase(tc.dependsOn)
	opts := pc.getOptions(pc, t)
	dir, err := InitTestCaseExecuteDir(tc.name)
	assert.NoError(t, err)
	err = CopyDir(GetPrepareDirByType(pc.typ), dir)
	assert.NoError(t, err)
	ctx := context.Background()
	tae := testutil.NewTestEngineWithDir(ctx, dir, t, opts)
	tae.BindSchema(pc.getSchema(pc, t))
	return tae
}

type PrepareCase struct {
	typ        int
	desc       string
	prepareFn  func(tc PrepareCase, t *testing.T)
	getBatch   func(tc PrepareCase, t *testing.T) *containers.Batch
	getSchema  func(tc PrepareCase, t *testing.T) *catalog.Schema
	getOptions func(tc PrepareCase, t *testing.T) *options.Options
}

func (pc PrepareCase) GetEngine(t *testing.T) *testutil.TestEngine {
	opts := pc.getOptions(pc, t)
	e := initPrepareTest(pc, opts, t)
	return e
}

type TestCase struct {
	name      string
	desc      string
	dependsOn int
	testFn    func(tc TestCase, t *testing.T)
}

func (tc TestCase) GetEngine(t *testing.T) *testutil.TestEngine {
	tae := initTestEngine(tc, t)
	return tae
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
