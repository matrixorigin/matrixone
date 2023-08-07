package compatibility

import (
	"testing"
)

func init() {
	PrepareCaseRegister(PrepareCase{
		desc:      "prepare-1",
		typ:       1,
		prepareFn: prepare1,
	})
	PrepareCaseRegister(PrepareCase{
		desc:      "prepare-2",
		typ:       2,
		prepareFn: prepare2,
	})

	TestCaseRegister(TestCase{
		desc:      "test-1",
		testFn:    test1,
		dependsOn: 1,
	})
}

func prepare1(tc PrepareCase, t *testing.T) {
	// dir, err := InitPrepareDirByType(tc.typ)
	// require.NoError(t, err)

	// ctx := context.Background()
	// opts := config.WithLongScanAndCKPOpts(nil)
	// tae := testutil.NewTestEngineWithDir(ctx, dir, t, opts)
	// defer tae.Close()

	// schema := catalog.MockSchemaAll(18, 13)
	// schema.BlockMaxRows = 10
	// schema.SegmentMaxBlocks = 2
	// tae.BindSchema(schema)

	// bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*3+1))
	// defer bat.Close()
	// bats := bat.Split(4)

	// tae.CreateRelAndAppend(bats[0], true)
	// txn, rel := tae.GetRelation()
	// v := testutil.GetSingleSortKeyValue(bats[0], schema, 2)
	// filter := handle.NewEQFilter(v)
	// err = rel.DeleteByFilter(context.Background(), filter)
	// assert.NoError(t, err)
	// assert.NoError(t, txn.Commit(context.Background()))

	// txn, rel = tae.GetRelation()
	// window := bat.CloneWindow(2, 1)
	// defer window.Close()
	// err = rel.Append(context.Background(), window)
	// assert.NoError(t, err)
	// _ = txn.Rollback(context.Background())
}

func prepare2(tc PrepareCase, t *testing.T) {
	t.Logf("Prepare %s", tc.desc)
}

func test1(testCase TestCase, t *testing.T) {
	t.Logf("Test %s", testCase.desc)
}
