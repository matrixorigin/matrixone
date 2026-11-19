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

package tables

import (
	"testing"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/stretchr/testify/require"
)

func evolutionSchemas(t *testing.T) (*catalog.Schema, *catalog.Schema) {
	t.Helper()
	old, current := catalog.NewEmptySchema("oracle"), catalog.NewEmptySchema("oracle")
	for _, schema := range []*catalog.Schema{old, current} {
		require.NoError(t, schema.AppendCol("id", types.T_int64.ToType()))
	}
	require.NoError(t, old.AppendCol("dropped", types.T_int64.ToType()))
	require.NoError(t, current.AppendColWithAttribute(engine.Attribute{Name: "flag", Seqnum: 2, Type: types.T_bool.ToType(),
		Default: &plan.Default{NullAbility: true,
			Expr: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: true}}}}}}))
	current.Extra.FeatureFlag = features.SchemaEvolved
	current.Extra.NextColSeqnum = 3
	require.NoError(t, old.Finalize(false))
	require.NoError(t, current.Finalize(false))
	return old, current
}

func TestSchemaEvolutionMemoryNodeOracle(t *testing.T) {
	old, current := evolutionSchemas(t)
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	rt := dbutils.NewRuntime(dbutils.WithRuntimeTransientPool(containers.NewVectorPool("oracle", 0, containers.WithMPool(mp))))
	makeInput := func(t *testing.T, id int64, isNull bool, schema *catalog.Schema) *containers.Batch {
		bat := containers.BuildBatch(schema.AllNames(), schema.AllTypes(), containers.Options{Allocator: mp})
		t.Cleanup(bat.Close)
		require.NoError(t, vector.AppendAny(bat.GetVectorByName("id"), id, false, bat.GetVectorByName("id").MPool()))
		require.NoError(t, vector.AppendAny(bat.GetVectorByName(catalog.PhyAddrColumnName), nil, true, bat.GetVectorByName(catalog.PhyAddrColumnName).MPool()))
		if schema == current {
			require.NoError(t, vector.AppendAny(bat.GetVectorByName("flag"), true, isNull, bat.GetVectorByName("flag").MPool()))
		} else {
			require.NoError(t, vector.AppendAny(bat.GetVectorByName("dropped"), int64(99), false, bat.GetVectorByName("dropped").MPool()))
		}
		return bat
	}
	t.Run("old node scan in both branches", func(t *testing.T) {
		node := &memoryNode{object: &baseObject{rt: rt}, writeSchema: old}
		defer node.mustData().Close()
		_, err := node.ApplyAppendLocked(makeInput(t, 2, false, old), old)
		require.NoError(t, err)
		var out *containers.Batch
		require.NoError(t, node.getDataWindowLocked(&out, current, []int{0, 1}, 0, 1, mp))
		defer out.Close()
		require.Equal(t, int64(2), out.Vecs[0].GetValue(0))
		require.True(t, out.Vecs[1].IsNull(0))
		require.NoError(t, node.getDataWindowLocked(&out, current, []int{0, 1}, 0, 1, mp))
		require.Equal(t, 2, out.Length())
		require.Equal(t, int64(2), out.Vecs[0].GetValue(1))
		require.True(t, out.Vecs[1].IsNull(1))
	})
	for _, mode := range []string{"append", "out-of-order WAL payload"} {
		t.Run(mode, func(t *testing.T) {
			node := &memoryNode{object: &baseObject{rt: rt}, writeSchema: current}
			defer node.mustData().Close()
			oldRow := makeInput(t, 2, false, old)
			require.Equal(t, len(current.ColDefs), len(oldRow.Attrs), "DROP and ADD leave the WAL width unchanged")
			newRow := makeInput(t, 3, false, current)
			nullRow := makeInput(t, 4, true, current)
			if mode == "append" {
				for i, row := range []*containers.Batch{oldRow, newRow, nullRow} {
					source := []*catalog.Schema{old, current, current}[i]
					_, err := node.ApplyAppendLocked(row, source)
					require.NoError(t, err)
				}
			} else {
				require.NoError(t, node.ApplyAppendAtLocked(newRow, 1, current, nil))
				require.NoError(t, node.ApplyAppendAtLocked(oldRow, 0, old, nil))
				require.NoError(t, node.ApplyAppendAtLocked(nullRow, 2, current, nil))
			}
			var out *containers.Batch
			require.NoError(t, node.getDataWindowLocked(&out, current, []int{0, 1}, 0, 1, mp))
			defer out.Close()
			require.NoError(t, node.getDataWindowLocked(&out, current, []int{0, 1}, 1, 3, mp))
			require.Equal(t, 3, out.Length())
			require.Equal(t, []int64{2, 3, 4}, vector.MustFixedColWithTypeCheck[int64](out.Vecs[0]))
			require.True(t, out.Vecs[1].IsNull(0))
			require.Equal(t, true, out.Vecs[1].GetValue(1))
			require.True(t, out.Vecs[1].IsNull(2))
		})
	}
}

func TestSchemaEvolutionWALRename(t *testing.T) {
	for _, tc := range []struct {
		name       string
		dropAndAdd bool
	}{
		{name: "tail rename"},
		{name: "rename drop and re-add", dropAndAdd: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typ := types.T_int64.ToType()
			old := catalog.NewEmptySchema("wal_rename")
			old.Extra.NextColSeqnum = 8
			require.NoError(t, old.AppendColWithAttribute(engine.Attribute{Name: "id", Type: typ}))
			if tc.dropAndAdd {
				require.NoError(t, old.AppendColWithAttribute(engine.Attribute{Name: "note", Seqnum: 3, Type: typ}))
			}
			require.NoError(t, old.AppendColDef(&catalog.ColDef{
				Name: "price", SeqNum: 7, Type: typ, Primary: true, SortKey: true,
			}))
			require.NoError(t, old.Finalize(false))
			current := old.Clone()
			require.NoError(t, current.ApplyAlterTable(api.NewRenameColumnReq(0, 0, "price", "amount", 7)))
			if tc.dropAndAdd {
				require.NoError(t, current.ApplyAlterTable(api.NewRemoveColumnReq(0, 0, 1, 3)))
				require.NoError(t, current.ApplyAlterTable(api.NewAddColumnReq(0, 0, "note", types.NewProtoType(types.T_int64), 2)))
				require.Equal(t, uint16(8), current.ColDefs[current.GetColIdx("note")].SeqNum)
			}
			current.Version++
			current.Extra.FeatureFlag = features.SchemaEvolved
			require.Equal(t, uint16(7), current.ColDefs[current.GetColIdx("amount")].SeqNum)
			require.Equal(t, -1, current.GetColIdx("price"))

			makeBatch := func(names []string, values ...int64) *containers.Batch {
				bat := containers.NewBatch()
				t.Cleanup(bat.Close)
				for i, name := range names {
					vec := containers.MakeVector(typ, common.DefaultAllocator)
					require.NoError(t, vector.AppendAny(vec, values[i], false, vec.MPool()))
					bat.AddVector(name, vec)
				}
				return bat
			}
			oldNames, oldValues := []string{"id", "price"}, []int64{1, 42}
			newNames, newValues := []string{"id", "amount"}, []int64{2, 99}
			if tc.dropAndAdd {
				oldNames, oldValues = []string{"id", "note", "price"}, []int64{1, 123, 42}
				newNames, newValues = []string{"id", "amount", "note"}, []int64{2, 99, 456}
			}
			payload := makeBatch(oldNames, oldValues...)
			initial := makeBatch(newNames, newValues...)
			unknown := makeBatch([]string{"id", "unknown"}, 1, 10)
			missingTail := makeBatch(oldNames[:len(oldNames)-1], oldValues[:len(oldValues)-1]...)
			missingCurrent := makeBatch(newNames[:len(newNames)-1], newValues[:len(newValues)-1]...)
			wrongType := makeBatch(oldNames, oldValues...)
			containers.ReleaseVector(wrongType.Vecs[0])
			wrongType.Vecs[0] = containers.MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
			require.NoError(t, vector.AppendAny(wrongType.Vecs[0], int32(1), false, wrongType.Vecs[0].MPool()))
			wrongLength := makeBatch(oldNames, oldValues...)
			require.NoError(t, vector.AppendAny(wrongLength.Vecs[1], int64(456), false, wrongLength.Vecs[1].MPool()))

			for _, positional := range []bool{false, true} {
				mode := "sequential"
				if positional {
					mode = "positional"
				}
				t.Run(mode, func(t *testing.T) {
					table := catalog.MockStaloneTableEntry(1, current)
					oid := objectio.NewObjectid()
					stats := objectio.NewObjectStatsWithObjectID(&oid, true, false, false)
					meta := catalog.MockObjectEntry(table, stats, false, nil, types.BuildTS(1, 0))
					obj := newAObject(meta, dbutils.NewRuntime(), false)
					t.Cleanup(func() { obj.node.Load().Unref() })
					n := obj.PinNode()
					defer n.Unref()
					node := n.MustMNode()
					appender := newAppender(obj)
					if positional {
						require.NoError(t, appender.ApplyAppendAt(initial, 0, current))
					}
					for _, bad := range []struct {
						bat     *containers.Batch
						source  *catalog.Schema
						message string
					}{
						{unknown, old, "unknown append column"},
						{missingTail, old, "required by source schema"},
						{missingCurrent, current, "required by source schema"},
						{payload, current, "unknown append column"},
						{wrongType, old, "invalid append vector"},
						{wrongLength, old, "invalid append vector"},
					} {
						if positional {
							require.ErrorContains(t, appender.ApplyAppendAt(bad.bat, 0, bad.source), bad.message)
							keys, err := node.pkIndex.GetActiveRow(int64(99))
							require.NoError(t, err)
							require.Equal(t, []uint32{0}, keys)
							require.Equal(t, int64(99), node.data.GetVectorByName("amount").Get(0))
						} else {
							_, err := node.ApplyAppendLocked(bad.bat, bad.source)
							require.ErrorContains(t, err, bad.message)
							require.Nil(t, node.data)
						}
					}
					if positional {
						require.NoError(t, appender.ApplyAppendAt(payload, 0, old))
						_, err := node.pkIndex.GetActiveRow(int64(99))
						require.Error(t, err)
						keys, err := node.pkIndex.GetActiveRow(int64(42))
						require.NoError(t, err)
						require.Equal(t, []uint32{0}, keys)
					} else {
						from, err := node.ApplyAppendLocked(payload, old)
						require.NoError(t, err)
						require.Zero(t, from)
					}
					mp := mpool.MustNewZero()
					defer mpool.DeleteMPool(mp)
					cols := []int{current.GetColIdx("amount")}
					if tc.dropAndAdd {
						cols = append(cols, current.GetColIdx("note"))
					}
					var out *containers.Batch
					require.NoError(t, node.getDataWindowLocked(&out, current, cols, 0, 1, mp))
					defer out.Close()
					require.False(t, out.Vecs[0].IsNull(0))
					require.Equal(t, int64(42), out.Vecs[0].GetValue(0))
					if tc.dropAndAdd {
						require.Equal(t, typ, *out.Vecs[1].GetType())
						require.True(t, out.Vecs[1].IsNull(0))
					}
				})
			}
		})
	}
}

func newPositionalWALTestNode(t *testing.T) (*catalog.Schema, *mpool.MPool, *memoryNode, *objectAppender) {
	t.Helper()
	schema := catalog.NewEmptySchema("positional_replay")
	idType := types.T_int64.ToType()
	require.NoError(t, schema.AppendPKCol("id", idType, 0))
	require.NoError(t, schema.AppendCol("flag", types.T_bool.ToType()))
	require.NoError(t, schema.AppendCol("memo", types.T_text.ToType()))
	schema.Extra.FeatureFlag = features.SchemaEvolved
	require.NoError(t, schema.Finalize(false))

	mp, err := mpool.NewMPool("positional-wal-test", 1024*1024, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	table := catalog.MockStaloneTableEntry(1, schema)
	oid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&oid, true, false, false)
	meta := catalog.MockObjectEntry(table, stats, false, nil, types.BuildTS(1, 0))
	mvcc := updates.NewAppendMVCCHandle(meta)
	base := &baseObject{RWMutex: mvcc.RWMutex, appendMVCC: mvcc}
	base.meta.Store(meta)
	node := newMemoryNode(base, false)
	node.Ref()
	t.Cleanup(node.Unref)
	node.data = containers.BuildBatch(schema.AllNames(), schema.AllTypes(), containers.Options{Allocator: mp})
	obj := &aobject{baseObject: base}
	obj.node.Store(NewNode(node))
	appender := newAppender(obj)

	return schema, mp, node, appender
}

func TestApplyAppendAtWALFillFailurePreservesRowsAndPrimaryKey(t *testing.T) {
	schema, mp, node, appender := newPositionalWALTestNode(t)
	idType := types.T_int64.ToType()

	inputPool := mpool.MustNewZero()
	defer mpool.DeleteMPool(inputPool)
	initial := containers.BuildBatch(schema.AllNames(), schema.AllTypes(), containers.Options{Allocator: inputPool})
	defer initial.Close()
	require.NoError(t, vector.AppendAny(initial.GetVectorByName("id"), int64(7), false, initial.GetVectorByName("id").MPool()))
	require.NoError(t, vector.AppendAny(initial.GetVectorByName("flag"), true, false, initial.GetVectorByName("flag").MPool()))
	require.NoError(t, vector.AppendAny(initial.GetVectorByName("memo"), []byte("unchanged"), false, initial.GetVectorByName("memo").MPool()))
	require.NoError(t, vector.AppendAny(initial.GetVectorByName(catalog.PhyAddrColumnName), nil, true, initial.GetVectorByName(catalog.PhyAddrColumnName).MPool()))
	require.NoError(t, appender.ApplyAppendAt(initial, 0, nil))
	positions, err := node.pkIndex.GetActiveRow(int64(7))
	require.NoError(t, err)
	require.Equal(t, []uint32{0}, positions)

	oldSchema := catalog.NewEmptySchema(schema.Name)
	require.NoError(t, oldSchema.AppendPKCol("id", idType, 0))
	require.NoError(t, oldSchema.Finalize(false))
	old := containers.BuildBatch([]string{"id"}, []types.Type{idType}, containers.Options{Allocator: inputPool})
	defer old.Close()
	for range 128 {
		require.NoError(t, vector.AppendAny(old.Vecs[0], int64(99), false, old.Vecs[0].MPool()))
	}
	// Room remains for the small fixed-width column, but not for memo to grow.
	padding, err := mp.Alloc(int(mp.Cap()-mp.CurrNB()-512), true)
	require.NoError(t, err)
	defer mp.Free(padding)
	before := mp.CurrNB()
	err = appender.ApplyAppendAt(old, 0, oldSchema)
	require.True(t, mpool.IsMPoolCapacityFailure(err), "expected missing column growth to exceed the pool: %v", err)
	require.Equal(t, before, mp.CurrNB())
	for _, vec := range node.data.Vecs {
		require.Equal(t, 1, vec.Length())
	}
	require.Equal(t, int64(7), node.data.GetVectorByName("id").Get(0))
	require.Equal(t, true, node.data.GetVectorByName("flag").Get(0))
	require.Equal(t, []byte("unchanged"), node.data.GetVectorByName("memo").Get(0))
	positions, err = node.pkIndex.GetActiveRow(int64(7))
	require.NoError(t, err)
	require.Equal(t, []uint32{0}, positions)
	_, err = node.pkIndex.GetActiveRow(int64(99))
	require.Error(t, err)

	// Retrying with a payload that fits removes the old key and fills NULLs.
	old.Vecs[0].SetLength(1)
	require.NoError(t, appender.ApplyAppendAt(old, 0, oldSchema))
	_, err = node.pkIndex.GetActiveRow(int64(7))
	require.Error(t, err)
	positions, err = node.pkIndex.GetActiveRow(int64(99))
	require.NoError(t, err)
	require.Equal(t, []uint32{0}, positions)
	require.Equal(t, int64(99), node.data.GetVectorByName("id").Get(0))
	require.True(t, node.data.GetVectorByName("flag").IsNull(0))
	require.True(t, node.data.GetVectorByName("memo").IsNull(0))
}

func TestApplyAppendAtInRangeNullBitmapFailure(t *testing.T) {
	schema, mp, node, appender := newPositionalWALTestNode(t)
	inputPool := mpool.MustNewZero()
	defer mpool.DeleteMPool(inputPool)
	initial := containers.BuildBatch(schema.AllNames(), schema.AllTypes(), containers.Options{Allocator: inputPool})
	defer initial.Close()
	for i, key := range []int64{7, 8, 9} {
		require.NoError(t, vector.AppendAny(initial.Vecs[0], key, false, initial.Vecs[0].MPool()))
		require.NoError(t, vector.AppendAny(initial.Vecs[1], i != 2, false, initial.Vecs[1].MPool()))
		require.NoError(t, vector.AppendAny(initial.Vecs[2], []byte{byte(key)}, false, initial.Vecs[2].MPool()))
		require.NoError(t, vector.AppendAny(initial.Vecs[3], nil, true, initial.Vecs[3].MPool()))
	}
	_, err := appender.ApplyAppend(initial, nil)
	require.NoError(t, err)
	flag := node.data.GetVectorByName("flag")
	require.Zero(t, flag.GetNulls().GetBitmap().Len())
	require.Equal(t, 3, flag.Length())

	idType := types.T_int64.ToType()
	oldSchema := catalog.NewEmptySchema(schema.Name)
	require.NoError(t, oldSchema.AppendPKCol("id", idType, 0))
	require.NoError(t, oldSchema.Finalize(false))
	old := containers.BuildBatch([]string{"id"}, []types.Type{idType}, containers.Options{Allocator: inputPool})
	defer old.Close()
	require.NoError(t, vector.AppendAny(old.Vecs[0], int64(99), false, old.Vecs[0].MPool()))
	const offset = 1
	require.LessOrEqual(t, offset+old.Length(), flag.Length())

	// Leave enough room for the clone, with zero bytes left for its bitmap.
	unpadded := mp.CurrNB()
	clone, err := flag.CloneWindow(0, flag.Length(), mp)
	require.NoError(t, err)
	cloneBytes := mp.CurrNB() - unpadded
	require.Zero(t, clone.GetNulls().GetBitmap().Len())
	clone.Free(mp)
	require.Equal(t, unpadded, mp.CurrNB())
	padding, err := mp.Alloc(int(mp.Cap()-unpadded-cloneBytes), true)
	require.NoError(t, err)
	defer func() {
		if padding != nil {
			mp.Free(padding)
		}
	}()
	before := mp.CurrNB()
	require.Equal(t, cloneBytes, mp.Cap()-before)
	err = appender.ApplyAppendAt(old, offset, oldSchema)
	require.True(t, mpool.IsMPoolCapacityFailure(err), "expected bitmap allocation to exceed the pool: %v", err)
	require.Equal(t, before, mp.CurrNB())
	for i, key := range []int64{7, 8, 9} {
		require.Equal(t, key, node.data.Vecs[0].GetValue(i))
		require.Equal(t, i != 2, node.data.Vecs[1].GetValue(i))
		require.Equal(t, []byte{byte(key)}, node.data.Vecs[2].GetValue(i))
		positions, err := node.pkIndex.GetActiveRow(key)
		require.NoError(t, err)
		require.Equal(t, []uint32{uint32(i)}, positions)
	}
	_, err = node.pkIndex.GetActiveRow(int64(99))
	require.Error(t, err)
	require.Same(t, flag, node.data.Vecs[1])
	require.Zero(t, flag.GetNulls().GetBitmap().Len())

	mp.Free(padding)
	padding = nil
	require.NoError(t, appender.ApplyAppendAt(old, offset, oldSchema))
	require.Equal(t, 3, node.data.Length())
	require.Equal(t, int64(99), node.data.Vecs[0].GetValue(offset))
	require.True(t, node.data.Vecs[1].IsNull(offset))
	require.True(t, node.data.Vecs[2].IsNull(offset))
	for _, pos := range []int{0, 2} {
		require.Equal(t, initial.Vecs[0].GetValue(pos), node.data.Vecs[0].GetValue(pos))
		require.Equal(t, initial.Vecs[1].GetValue(pos), node.data.Vecs[1].GetValue(pos))
		require.Equal(t, initial.Vecs[2].GetValue(pos), node.data.Vecs[2].GetValue(pos))
	}
	_, err = node.pkIndex.GetActiveRow(int64(8))
	require.Error(t, err)
	positions, err := node.pkIndex.GetActiveRow(int64(99))
	require.NoError(t, err)
	require.Equal(t, []uint32{offset}, positions)
}

func TestReplayAppendLegacyCatalogWALTextType(t *testing.T) {
	schema := catalog.SystemDBSchema
	name := pkgcatalog.SystemDBAttr_Name
	for _, tc := range []struct {
		name    string
		tableID uint64
		width   int32
		accept  bool
	}{
		{"released catalog text", pkgcatalog.MO_DATABASE_ID, 5000, true},
		{"wrong catalog width", pkgcatalog.MO_DATABASE_ID, 4999, false},
		{"user table text", 1000, 5000, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			inputTypes := schema.Types()
			inputTypes[schema.GetColIdx(name)].Charset = types.CharsetLegacy
			inputTypes[schema.GetColIdx(name)].Width = tc.width
			payload := containers.BuildBatch(schema.Attrs(), inputTypes, containers.Options{Allocator: mp})
			defer payload.Close()
			for i, attr := range payload.Attrs {
				if attr == name {
					require.NoError(t, vector.AppendAny(payload.Vecs[i], []byte("release_db"), false, payload.Vecs[i].MPool()))
				} else {
					require.NoError(t, vector.AppendAny(payload.Vecs[i], nil, true, payload.Vecs[i].MPool()))
				}
			}
			node := &memoryNode{writeSchema: schema, data: containers.BuildBatch(
				schema.AllNames(), schema.AllTypes(), containers.Options{Allocator: mp},
			)}
			defer node.data.Close()
			normalizeLegacyCatalogWALTypes(tc.tableID, payload, schema)
			err := node.ApplyAppendAtLocked(payload, 0, schema, nil)
			if !tc.accept {
				require.ErrorContains(t, err, "invalid append vector")
				require.Zero(t, node.data.Length())
				require.Equal(t, types.CharsetLegacy, payload.GetVectorByName(name).GetType().Charset)
				return
			}
			require.NoError(t, err)
			require.Equal(t, []byte("release_db"), node.data.GetVectorByName(name).Get(0))
			require.Equal(t, types.CharsetUTF8, node.data.GetVectorByName(name).GetType().Charset)
		})
	}
}

func TestSchemaEvolutionPersistedNodeOracle(t *testing.T) {
	_, schema := evolutionSchemas(t)
	ctx := t.Context()
	fs, err := fileservice.NewLocalFS(ctx, "oracle", t.TempDir(), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer fs.Close(ctx)
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	rt := dbutils.NewRuntime(dbutils.WithRuntimeObjectFS(fs), dbutils.WithRuntimeTransientPool(
		containers.NewVectorPool("oracle", 0, containers.WithMPool(mp))))
	cat := catalog.MockCatalog(nil)
	defer cat.Close()
	db, err := cat.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	tbl, err := db.CreateTableEntry(schema, nil, nil)
	require.NoError(t, err)
	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	defer input.Clean(mp)
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(1), false, mp))
	input.SetRowCount(1)
	writer := ioutil.ConstructWriter(0, []uint16{0}, -1, false, false, fs)
	_, err = writer.WriteBatch(input)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	obj, err := tbl.CreateObject(nil, &objectio.CreateObjOpt{Stats: &stats}, nil)
	require.NoError(t, err)
	base := &baseObject{rt: rt}
	base.meta.Store(obj)
	node := newPersistedNode(base)
	for _, mode := range []struct {
		name     string
		borrowed bool
	}{{"scan", false}, {"merge scan", true}} {
		t.Run(mode.name, func(t *testing.T) {
			readCtx := ctx
			if mode.borrowed {
				readCtx = WithScanNoCopy(ctx)
			}
			var out *containers.Batch
			require.NoError(t, node.Scan(readCtx, &out, nil, schema, 0, []int{0, 1}, mp))
			defer out.Close()
			require.Equal(t, 1, out.Length())
			require.Equal(t, int64(1), out.Vecs[0].GetValue(0))
			require.True(t, out.Vecs[1].IsNull(0))
		})
	}
}
