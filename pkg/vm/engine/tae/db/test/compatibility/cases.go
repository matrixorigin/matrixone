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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/assert"
)

func init() {
	PrepareCaseRegister(makePrepare1(1, "prepare-1"))
	TestCaseRegister(makeTest1(1, "test-1", "prepare-1=>test-1"))
}

func makePrepare1(id int, desc string) PrepareCase {
	getSchema := func(tc PrepareCase, t *testing.T) *catalog.Schema {
		schema := catalog.MockSchemaAll(18, 13)
		schema.Name = fmt.Sprintf("test-%d", tc.id)
		schema.BlockMaxRows = 10
		schema.SegmentMaxBlocks = 2
		return schema
	}
	getBatch := func(tc PrepareCase, t *testing.T) *containers.Batch {
		schema := getSchema(tc, t)
		bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*3+1))
		return bat
	}
	getOptions := func(tc PrepareCase, t *testing.T) *options.Options {
		opts := config.WithLongScanAndCKPOpts(nil)
		return opts
	}

	prepareFn := func(tc PrepareCase, t *testing.T) {
		tae := tc.GetEngine(t)
		defer tae.Close()

		schema := tc.getSchema(tc, t)
		bat := tc.getBatch(tc, t)
		defer bat.Close()
		bats := bat.Split(4)

		tae.CreateRelAndAppend(bats[0], true)
		txn, rel := tae.GetRelation()
		v := testutil.GetSingleSortKeyValue(bats[0], schema, 2)
		filter := handle.NewEQFilter(v)
		err := rel.DeleteByFilter(context.Background(), filter)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))

		txn, rel = tae.GetRelation()
		window := bat.CloneWindow(2, 1)
		defer window.Close()
		err = rel.Append(context.Background(), window)
		assert.NoError(t, err)
		_ = txn.Rollback(context.Background())
	}
	return PrepareCase{
		desc:       desc,
		id:         id,
		prepareFn:  prepareFn,
		getSchema:  getSchema,
		getBatch:   getBatch,
		getOptions: getOptions,
	}
}

func makeTest1(dependsOn int, name, desc string) TestCase {
	testFn := func(tc TestCase, t *testing.T) {
		pc := GetPrepareCase(tc.dependsOn)
		tae := tc.GetEngine(t)

		bat := pc.getBatch(pc, t)
		defer bat.Close()
		bats := bat.Split(4)
		window := bat.CloneWindow(2, 1)
		defer window.Close()

		txn, rel := tae.GetRelation()
		testutil.CheckAllColRowsByScan(t, rel, bats[0].Length()-1, true)
		err := rel.Append(context.Background(), bats[0])
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
		assert.NoError(t, txn.Commit(context.Background()))

		txn, rel = tae.GetRelation()
		err = rel.Append(context.Background(), window)
		assert.NoError(t, err)
		_ = txn.Rollback(context.Background())

		schema := pc.getSchema(pc, t)
		txn, rel = testutil.GetDefaultRelation(t, tae.DB, schema.Name)
		testutil.CheckAllColRowsByScan(t, rel, bats[0].Length()-1, true)
		assert.NoError(t, txn.Commit(context.Background()))

		txn, rel = testutil.GetDefaultRelation(t, tae.DB, schema.Name)
		err = rel.Append(context.Background(), window)
		assert.NoError(t, err)
		_ = txn.Rollback(context.Background())
	}
	return TestCase{
		name:      name,
		desc:      desc,
		dependsOn: dependsOn,
		testFn:    testFn,
	}
}
