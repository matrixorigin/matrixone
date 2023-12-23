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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/assert"
)

var (
	version = 6
)

type optType int

const (
	quickOpt optType = iota
	longOpt
)

func initPrepareTest(pc PrepareCase, opts *options.Options, t *testing.T) *testutil.TestEngine {
	dir, err := InitPrepareDirByName(pc.name)
	assert.NoError(t, err)
	ctx := context.Background()
	tae := testutil.NewTestEngineWithDir(ctx, dir, t, opts)
	tae.BindSchema(pc.GetSchema(t))
	return tae
}
func initTestEngine(tc TestCase, t *testing.T) *testutil.TestEngine {
	pc := GetPrepareCase(tc.dependsOn)
	opts := pc.GetOptions(t)
	dir, err := InitTestCaseExecuteDir(tc.name)
	assert.NoError(t, err)
	err = CopyDir(GetPrepareDirByName(pc.name), dir)
	assert.NoError(t, err)
	ctx := context.Background()
	tae := testutil.NewTestEngineWithDir(ctx, dir, t, opts)
	tae.BindSchema(pc.GetSchema(t))
	return tae
}

type schemaCfg struct {
	blockMaxRows    uint32
	ObjectMaxBlocks uint16
	colCnt          int
	pkIdx           int
}

type PrepareCase struct {
	name      string
	desc      string
	schemaCfg schemaCfg
	batchSize int
	optType   optType
	prepareFn func(tc PrepareCase, t *testing.T)
}

func (pc PrepareCase) GetOptions(t *testing.T) *options.Options {
	switch pc.optType {
	case quickOpt:
		return config.WithQuickScanAndCKPOpts(nil)
	case longOpt:
		return config.WithLongScanAndCKPOpts(nil)
	default:
		panic("PrepareCase.GetOptions: unknown optType")
	}
}

func (pc PrepareCase) GetEngine(t *testing.T) *testutil.TestEngine {
	opts := pc.GetOptions(t)
	e := initPrepareTest(pc, opts, t)
	return e
}

func (pc PrepareCase) GetSchema(t *testing.T) *catalog.Schema {
	schema := catalog.MockSchemaAll(pc.schemaCfg.colCnt, pc.schemaCfg.pkIdx)
	schema.BlockMaxRows = pc.schemaCfg.blockMaxRows
	schema.ObjectMaxBlocks = pc.schemaCfg.ObjectMaxBlocks
	ver, err := ReadPrepareVersion()
	if err != nil {
		t.Error(err)
	}
	schema.Name = fmt.Sprintf("test_%d", ver)
	return schema
}

func (pc PrepareCase) GetBatch(t *testing.T) *containers.Batch {
	schema := pc.GetSchema(t)
	bat := catalog.MockBatch(schema, pc.batchSize)
	return bat
}

type TestCase struct {
	name      string
	desc      string
	dependsOn string
	testFn    func(tc TestCase, t *testing.T)
}

func (tc TestCase) GetEngine(t *testing.T) *testutil.TestEngine {
	tae := initTestEngine(tc, t)
	return tae
}

var PrepareCases map[string]PrepareCase
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
		PrepareCases = make(map[string]PrepareCase)
	}
	if _, ok := PrepareCases[prepareCase.name]; ok {
		panic("PrepareCaseRegister: duplicate prepare case type")
	}
	PrepareCases[prepareCase.name] = prepareCase
}

func GetPrepareCase(name string) PrepareCase {
	if prepareCase, ok := PrepareCases[name]; ok {
		return prepareCase
	}
	panic("GetPrepareCase: prepare case not found")
}

func MakePrepareCase(
	prepareFn func(tc PrepareCase, t *testing.T),
	name string,
	desc string,
	schemaCfg schemaCfg,
	batchSize int,
	optType optType,
) PrepareCase {
	return PrepareCase{
		name:      name,
		desc:      desc,
		schemaCfg: schemaCfg,
		batchSize: batchSize,
		optType:   optType,
		prepareFn: prepareFn,
	}
}

func MakeTestCase(
	testFn func(tc TestCase, t *testing.T),
	dependsOn string,
	name string,
	desc string,
) TestCase {
	return TestCase{
		name:      name,
		desc:      desc,
		dependsOn: dependsOn,
		testFn:    testFn,
	}
}
