// Copyright 2024 Matrix Origin
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

package cdc

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

var (
	builderFromTS = types.BuildTS(100, 0)
	builderToTS   = types.BuildTS(200, 0)
)

func newBuilderTestMPool(t *testing.T) *mpool.MPool {
	t.Helper()
	mp, err := mpool.NewMPool(t.Name(), 0, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.Zero(t, mp.CurrNB()+mp.OnHeapCurrNB(), "fixture must release all mpool memory")
		mpool.DeleteMPool(mp)
	})
	return mp
}

func standardBuilderTableDef() *plan.TableDef {
	return &plan.TableDef{
		Name: "users",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: "name", Typ: plan.Type{Id: int32(types.T_varchar)}},
		},
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}},
		Name2ColIndex: map[string]int32{"id": 0, "name": 1},
	}
}

func directInsertBatch(t *testing.T, mp *mpool.MPool, ids []int32, names []string) *batch.Batch {
	t.Helper()
	require.Len(t, names, len(ids))
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	t.Cleanup(func() { bat.Clean(mp) })
	for i, id := range ids {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], id, false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte(names[i]), false, mp))
	}
	bat.SetRowCount(len(ids))
	return bat
}

func atomicStringBatch(t *testing.T, mp *mpool.MPool, values []string) *AtomicBatch {
	t.Helper()
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	owned := true
	t.Cleanup(func() {
		if owned {
			bat.Clean(mp)
		}
	})
	for _, value := range values {
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte(value), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], builderFromTS, false, mp))
	}
	bat.SetRowCount(len(values))
	atomic := NewAtomicBatch(mp)
	packer := types.NewPacker()
	atomic.Append(packer, bat, 1, 0)
	owned = false
	packer.Close()
	t.Cleanup(atomic.Close)
	return atomic
}

func unpaddedSQL(sql []byte) string { return string(sql[v2SQLBufReserved:]) }

func TestNewCDCStatementBuilder_ValidationAndStrategy(t *testing.T) {
	t.Run("pk only uses upsert and quotes once", func(t *testing.T) {
		def := &plan.TableDef{
			Cols: []*plan.ColDef{
				{Name: "select", Typ: plan.Type{Id: int32(types.T_int32)}},
				{Name: "a`b``c", Typ: plan.Type{Id: int32(types.T_varchar)}},
			},
			Pkey:          &plan.PrimaryKeyDef{Names: []string{"select"}},
			Name2ColIndex: map[string]int32{"select": 0, "a`b``c": 1},
		}
		builder, err := NewCDCStatementBuilder("库`名", "order", def, 1024, false)
		require.NoError(t, err)
		require.Equal(t, "INSERT INTO `库``名`.`order` VALUES ", string(builder.insertStem))
		require.Equal(t, " ON DUPLICATE KEY UPDATE `select`=VALUES(`select`),`a``b````c`=VALUES(`a``b````c`);", string(builder.insertSuffix))
		require.Equal(t, "DELETE FROM `库``名`.`order` WHERE `select` IN (", string(builder.deleteStem))
	})

	for _, tc := range []struct {
		name    string
		indexes []*plan.IndexDef
		replace bool
	}{
		{name: "nonunique", indexes: []*plan.IndexDef{{IndexName: "idx"}}},
		{name: "single unique", indexes: []*plan.IndexDef{{IndexName: "uk", Unique: true}}, replace: true},
		{name: "composite nullable unique", indexes: []*plan.IndexDef{{IndexName: "uk", Parts: []string{"id", "name"}, Unique: true}}, replace: true},
		{name: "any of two unique", indexes: []*plan.IndexDef{{IndexName: "idx"}, {IndexName: "uk", Unique: true}}, replace: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			def := standardBuilderTableDef()
			def.Indexes = tc.indexes
			builder, err := NewCDCStatementBuilder("db", "users", def, 1024, true)
			require.NoError(t, err)
			if tc.replace {
				require.Equal(t, "REPLACE INTO `db`.`users` VALUES ", string(builder.insertStem))
				require.Equal(t, ";", string(builder.insertSuffix))
			} else {
				require.Equal(t, "INSERT INTO `db`.`users` VALUES ", string(builder.insertStem))
				require.Contains(t, string(builder.insertSuffix), "ON DUPLICATE KEY UPDATE")
			}
		})
	}

	t.Run("composite primary key is fully quoted", func(t *testing.T) {
		def := standardBuilderTableDef()
		def.Cols[0].Name, def.Cols[1].Name = "a`b", "名"
		def.Pkey.Names = []string{"a`b", "名"}
		def.Name2ColIndex = map[string]int32{"a`b": 0, "名": 1}
		builder, err := NewCDCStatementBuilder("db", "t", def, 1024, false)
		require.NoError(t, err)
		require.Equal(t, "DELETE FROM `db`.`t` WHERE (`a``b`,`名`) IN (", string(builder.deleteStem))
		require.Equal(t, 100, builder.EstimateDeleteRowSize())
	})

	base := standardBuilderTableDef()
	badMapping := standardBuilderTableDef()
	badMapping.Name2ColIndex["id"] = 1
	badIndex := standardBuilderTableDef()
	badIndex.Name2ColIndex["id"] = 3
	internalOnly := &plan.TableDef{
		Cols: []*plan.ColDef{{Name: catalog.Row_ID, Typ: plan.Type{Id: int32(types.T_Rowid)}}},
		Pkey: &plan.PrimaryKeyDef{Names: []string{catalog.Row_ID}}, Name2ColIndex: map[string]int32{catalog.Row_ID: 0},
	}
	for _, tc := range []struct {
		name string
		def  *plan.TableDef
	}{
		{name: "nil table"},
		{name: "nil column", def: &plan.TableDef{Cols: []*plan.ColDef{nil}}},
		{name: "empty column name", def: &plan.TableDef{Cols: []*plan.ColDef{{Name: ""}}}},
		{name: "no visible columns", def: internalOnly},
		{name: "nil primary key", def: &plan.TableDef{Cols: base.Cols, Name2ColIndex: base.Name2ColIndex}},
		{name: "empty primary key", def: &plan.TableDef{Cols: base.Cols, Pkey: &plan.PrimaryKeyDef{}, Name2ColIndex: base.Name2ColIndex}},
		{name: "missing mapping", def: &plan.TableDef{Cols: base.Cols, Pkey: base.Pkey}},
		{name: "mismatched mapping", def: badMapping},
		{name: "invalid mapping", def: badIndex},
		{name: "nil index", def: &plan.TableDef{Cols: base.Cols, Pkey: base.Pkey, Name2ColIndex: base.Name2ColIndex, Indexes: []*plan.IndexDef{nil}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder, err := NewCDCStatementBuilder("db", "t", tc.def, 1024, false)
			require.Error(t, err)
			require.Nil(t, builder)
		})
	}
}

func TestCDCStatementBuilder_InsertExactSQLAndOwnership(t *testing.T) {
	mp := newBuilderTestMPool(t)
	def := standardBuilderTableDef()
	def.Cols = append(def.Cols, &plan.ColDef{Name: catalog.Row_ID, Typ: plan.Type{Id: int32(types.T_Rowid)}})
	builder, err := NewCDCStatementBuilder("test_db", "users", def, ^uint64(0), false)
	require.NoError(t, err)
	bat := directInsertBatch(t, mp, []int32{1, 2}, []string{"Alice", ""})
	sqls, err := builder.BuildInsertSQL(context.Background(), bat, builderFromTS, builderToTS)
	require.NoError(t, err)
	require.Len(t, sqls, 1)
	require.Equal(t, "/* [100-0, 200-0) */ INSERT INTO `test_db`.`users` VALUES (1,'Alice'),(2,'') ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`name`=VALUES(`name`);", unpaddedSQL(sqls[0]))
	require.Less(t, cap(sqls[0]), 1024, "capacity must follow payload, not an unused uint64 limit")

	before := append([]byte(nil), sqls[0]...)
	other := directInsertBatch(t, mp, []int32{3}, []string{"other"})
	second, err := builder.BuildInsertSQL(context.Background(), other, builderFromTS, builderToTS)
	require.NoError(t, err)
	second[0][v2SQLBufReserved] = 'X'
	require.Equal(t, before, sqls[0], "results from separate builds must not share mutable storage")
	require.Equal(t, []string{"id"}, def.Pkey.Names, "construction/building must not mutate source metadata")
}

func TestCDCStatementBuilder_InsertNullTypesAndEscaping(t *testing.T) {
	mp := newBuilderTestMPool(t)
	def := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "u", Typ: plan.Type{Id: int32(types.T_uint64)}}, {Name: "s", Typ: plan.Type{Id: int32(types.T_varchar)}},
			{Name: "b", Typ: plan.Type{Id: int32(types.T_bool)}}, {Name: "f", Typ: plan.Type{Id: int32(types.T_float64)}},
		},
		Pkey: &plan.PrimaryKeyDef{Names: []string{"u"}}, Name2ColIndex: map[string]int32{"u": 0, "s": 1, "b": 2, "f": 3},
	}
	builder, err := NewCDCStatementBuilder("db", "t", def, 4096, false)
	require.NoError(t, err)
	bat := batch.NewWithSize(4)
	bat.Vecs[0], bat.Vecs[1] = vector.NewVec(types.T_uint64.ToType()), vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[2], bat.Vecs[3] = vector.NewVec(types.T_bool.ToType()), vector.NewVec(types.T_float64.ToType())
	t.Cleanup(func() { bat.Clean(mp) })
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], ^uint64(0), false, mp))
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("path\\'测试"), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[2], true, false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[3], 3.14, false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], uint64(1), false, mp))
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], nil, true, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[2], false, true, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[3], 0.0, true, mp))
	bat.SetRowCount(2)
	sqls, err := builder.BuildInsertSQL(context.Background(), bat, builderFromTS, builderToTS)
	require.NoError(t, err)
	require.Len(t, sqls, 1)
	require.Contains(t, unpaddedSQL(sqls[0]), "(18446744073709551615,'path\\\\\\'测试',true,3.14),(1,NULL,NULL,NULL)")
}

func TestCDCStatementBuilder_InsertBoundsAndPostFlush(t *testing.T) {
	for _, useReplace := range []bool{false, true} {
		name := "upsert"
		if useReplace {
			name = "replace"
		}
		t.Run(name, func(t *testing.T) { testInsertBoundsAndPostFlush(t, useReplace) })
	}
}

func testInsertBoundsAndPostFlush(t *testing.T, useReplace bool) {
	t.Helper()
	mp := newBuilderTestMPool(t)
	def := standardBuilderTableDef()
	if useReplace {
		def.Indexes = []*plan.IndexDef{{IndexName: "uk_name", Parts: []string{"name"}, Unique: true}}
	}
	probe, err := NewCDCStatementBuilder("db", "t", def, ^uint64(0), false)
	require.NoError(t, err)
	one := directInsertBatch(t, mp, []int32{1}, []string{"small"})
	oneSQL, err := probe.BuildInsertSQL(context.Background(), one, builderFromTS, builderToTS)
	require.NoError(t, err)
	exact := uint64(len(oneSQL[0]))
	for _, tc := range []struct {
		name    string
		limit   uint64
		wantErr bool
	}{
		{name: "exact", limit: exact}, {name: "one below", limit: exact - 1, wantErr: true},
		{name: "zero", wantErr: true}, {name: "tiny", limit: v2SQLBufReserved, wantErr: true}, {name: "huge uint", limit: ^uint64(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder, buildErr := NewCDCStatementBuilder("db", "t", def, tc.limit, false)
			require.NoError(t, buildErr)
			sqls, buildErr := builder.BuildInsertSQL(context.Background(), one, builderFromTS, builderToTS)
			if tc.wantErr {
				require.Error(t, buildErr)
				require.Nil(t, sqls)
				return
			}
			require.NoError(t, buildErr)
			require.Len(t, sqls, 1)
			require.LessOrEqual(t, uint64(len(sqls[0])), tc.limit)
		})
	}

	largeOnly := directInsertBatch(t, mp, []int32{2}, []string{strings.Repeat("L", 120)})
	largeSQL, err := probe.BuildInsertSQL(context.Background(), largeOnly, builderFromTS, builderToTS)
	require.NoError(t, err)
	splitLimit := uint64(len(largeSQL[0]))
	builder, err := NewCDCStatementBuilder("db", "t", def, splitLimit, false)
	require.NoError(t, err)
	for _, tc := range []struct {
		name  string
		ids   []int32
		names []string
	}{
		{name: "small large", ids: []int32{1, 2}, names: []string{"s", strings.Repeat("L", 120)}},
		{name: "large small", ids: []int32{2, 1}, names: []string{strings.Repeat("L", 120), "s"}},
		{name: "small large small", ids: []int32{1, 2, 3}, names: []string{"s", strings.Repeat("L", 120), "s"}},
		{name: "three split rows", ids: []int32{1, 2, 3}, names: []string{strings.Repeat("L", 120), strings.Repeat("L", 120), strings.Repeat("L", 120)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bat := directInsertBatch(t, mp, tc.ids, tc.names)
			sqls, buildErr := builder.BuildInsertSQL(context.Background(), bat, builderFromTS, builderToTS)
			require.NoError(t, buildErr)
			require.Len(t, sqls, len(tc.ids))
			for i, sql := range sqls {
				require.LessOrEqual(t, uint64(len(sql)), splitLimit)
				verb, suffix := "INSERT", " ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`name`=VALUES(`name`);"
				if useReplace {
					verb, suffix = "REPLACE", ";"
				}
				require.Equal(t, fmt.Sprintf("/* [100-0, 200-0) */ %s INTO `db`.`t` VALUES (%d,'%s')%s", verb, tc.ids[i], tc.names[i], suffix), unpaddedSQL(sql))
			}
			first := append([]byte(nil), sqls[0]...)
			sqls[1][v2SQLBufReserved] = 'X'
			require.Equal(t, first, sqls[0], "split statements must own independent buffers")
		})
	}
	oversized := directInsertBatch(t, mp, []int32{1, 2}, []string{"s", strings.Repeat("X", 500)})
	sqls, err := builder.BuildInsertSQL(context.Background(), oversized, builderFromTS, builderToTS)
	require.Error(t, err)
	require.Nil(t, sqls, "a size error after a valid flushed row must discard completed statements")
}

type failingBuilderIterator struct {
	rows   [][]any
	offset int
	closed bool
}

func (i *failingBuilderIterator) Next() bool { i.offset++; return i.offset <= len(i.rows) }
func (i *failingBuilderIterator) Row(_ context.Context, row []any) error {
	if i.offset == len(i.rows) {
		return context.Canceled
	}
	copy(row, i.rows[i.offset])
	return nil
}
func (i *failingBuilderIterator) Close() { i.closed = true }

func TestCDCStatementBuilder_IteratorErrorAfterFlushIsAtomic(t *testing.T) {
	def := standardBuilderTableDef()
	probe, err := NewCDCStatementBuilder("db", "t", def, ^uint64(0), false)
	require.NoError(t, err)
	row, err := probe.formatInsertRow(context.Background(), []any{int32(1), []byte(strings.Repeat("x", 80))})
	require.NoError(t, err)
	limit := uint64(v2SQLBufReserved + len(probe.buildInsertPrefix(builderFromTS, builderToTS)) + len(row) + len(probe.insertSuffix))
	builder, err := NewCDCStatementBuilder("db", "t", def, limit, false)
	require.NoError(t, err)
	iter := &failingBuilderIterator{offset: -1, rows: [][]any{{int32(1), []byte(strings.Repeat("x", 80))}, {int32(2), []byte(strings.Repeat("y", 80))}}}
	sqls, err := builder.buildInsertSQL(context.Background(), iter, builderFromTS, builderToTS)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, sqls)
	require.True(t, iter.closed)
}

func TestCDCStatementBuilder_DeleteExactSQLCompositeAndBounds(t *testing.T) {
	mp := newBuilderTestMPool(t)
	singleDef := &plan.TableDef{
		Cols: []*plan.ColDef{{Name: "key`word", Typ: plan.Type{Id: int32(types.T_varchar)}}},
		Pkey: &plan.PrimaryKeyDef{Names: []string{"key`word"}}, Name2ColIndex: map[string]int32{"key`word": 0},
	}
	probe, err := NewCDCStatementBuilder("d`b", "表", singleDef, ^uint64(0), false)
	require.NoError(t, err)
	one := atomicStringBatch(t, mp, []string{"small"})
	oneSQL, err := probe.BuildDeleteSQL(context.Background(), one, builderFromTS, builderToTS)
	require.NoError(t, err)
	require.Equal(t, "/* [100-0, 200-0) */ DELETE FROM `d``b`.`表` WHERE `key``word` IN (('small'));", unpaddedSQL(oneSQL[0]))
	exact := uint64(len(oneSQL[0]))
	for _, delta := range []uint64{0, 1} {
		builder, buildErr := NewCDCStatementBuilder("d`b", "表", singleDef, exact-delta, false)
		require.NoError(t, buildErr)
		sqls, buildErr := builder.BuildDeleteSQL(context.Background(), one, builderFromTS, builderToTS)
		if delta == 0 {
			require.NoError(t, buildErr)
			require.Len(t, sqls, 1)
		} else {
			require.Error(t, buildErr)
			require.Nil(t, sqls)
		}
	}

	largeOnly := atomicStringBatch(t, mp, []string{strings.Repeat("L", 100)})
	largeSQL, err := probe.BuildDeleteSQL(context.Background(), largeOnly, builderFromTS, builderToTS)
	require.NoError(t, err)
	limit := uint64(len(largeSQL[0]))
	builder, err := NewCDCStatementBuilder("d`b", "表", singleDef, limit, false)
	require.NoError(t, err)
	mixed := atomicStringBatch(t, mp, []string{"a", strings.Repeat("L", 100), "z"})
	sqls, err := builder.BuildDeleteSQL(context.Background(), mixed, builderFromTS, builderToTS)
	require.NoError(t, err)
	require.Len(t, sqls, 2)
	require.Contains(t, unpaddedSQL(sqls[0]), strings.Repeat("L", 100))
	require.Equal(t, "/* [100-0, 200-0) */ DELETE FROM `d``b`.`表` WHERE `key``word` IN (('a'),('z'));", unpaddedSQL(sqls[1]))
	for _, sql := range sqls {
		require.LessOrEqual(t, uint64(len(sql)), limit)
	}
	tooLarge := atomicStringBatch(t, mp, []string{"a", strings.Repeat("X", 300)})
	sqls, err = builder.BuildDeleteSQL(context.Background(), tooLarge, builderFromTS, builderToTS)
	require.Error(t, err)
	require.Nil(t, sqls)

	compositeDef := &plan.TableDef{
		Cols: []*plan.ColDef{{Name: "order", Typ: plan.Type{Id: int32(types.T_int32)}}, {Name: "客户", Typ: plan.Type{Id: int32(types.T_int32)}}},
		Pkey: &plan.PrimaryKeyDef{Names: []string{"order", "客户"}}, Name2ColIndex: map[string]int32{"order": 0, "客户": 1},
	}
	composite, err := NewCDCStatementBuilder("db", "t", compositeDef, 1024, true)
	require.NoError(t, err)
	packer := types.NewPacker()
	packer.EncodeInt32(1)
	packer.EncodeInt32(100)
	row, err := composite.formatDeleteRow(context.Background(), append([]byte(nil), packer.GetBuf()...))
	packer.Close()
	require.NoError(t, err)
	require.Equal(t, "(1,100)", string(row))
	keys := types.NewPacker()
	defer keys.Close()
	keys.EncodeInt32(1)
	keys.EncodeInt32(100)
	firstKey := string(keys.GetBuf())
	keys.Reset()
	keys.EncodeInt32(2)
	keys.EncodeInt32(200)
	packedBatch := atomicStringBatch(t, mp, []string{firstKey, string(keys.GetBuf())})
	compositeSQL, err := composite.BuildDeleteSQL(context.Background(), packedBatch, builderFromTS, builderToTS)
	require.NoError(t, err)
	require.Len(t, compositeSQL, 1)
	require.Equal(t, "/* [100-0, 200-0) */ DELETE FROM `db`.`t` WHERE (`order`,`客户`) IN ((1,100),(2,200));", unpaddedSQL(compositeSQL[0]))
	_, err = composite.formatDeleteRow(context.Background(), int32(1))
	require.ErrorContains(t, err, "composite PK must be []byte")
	badPacker := types.NewPacker()
	badPacker.EncodeInt32(1)
	_, err = composite.formatDeleteRow(context.Background(), append([]byte(nil), badPacker.GetBuf()...))
	badPacker.Close()
	require.ErrorContains(t, err, "PK tuple length mismatch")
}

func TestCDCStatementBuilder_AtomicInsertOrderAndDedup(t *testing.T) {
	mp := newBuilderTestMPool(t)
	builder, err := NewCDCStatementBuilder("db", "t", standardBuilderTableDef(), 1024, false)
	require.NoError(t, err)
	atomic := NewAtomicBatch(mp)
	t.Cleanup(atomic.Close)
	packer := types.NewPacker()
	defer packer.Close()
	appendTestBatchToAtomic(t, atomic, packer, mp, types.BuildTS(3, 0), []int32{3})
	appendTestBatchToAtomic(t, atomic, packer, mp, types.BuildTS(2, 0), []int32{1, 2})
	appendTestBatchToAtomic(t, atomic, packer, mp, types.BuildTS(2, 0), []int32{1})
	sqls, err := builder.buildAtomicInsertSQL(context.Background(), atomic, types.BuildTS(1, 0), types.BuildTS(4, 0))
	require.NoError(t, err)
	require.Len(t, sqls, 1)
	require.Equal(t, 1, atomic.DuplicateRows())
	require.Equal(t, "/* [1-0, 4-0) */ INSERT INTO `db`.`t` VALUES (1,'test'),(2,'test'),(3,'test') ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`name`=VALUES(`name`);", unpaddedSQL(sqls[0]))
}

func TestCDCStatementBuilder_EmptyInputs(t *testing.T) {
	mp := newBuilderTestMPool(t)
	builder, err := NewCDCStatementBuilder("db", "t", standardBuilderTableDef(), 0, false)
	require.NoError(t, err)
	sqls, err := builder.BuildInsertSQL(context.Background(), nil, builderFromTS, builderToTS)
	require.NoError(t, err)
	require.Nil(t, sqls)
	sqls, err = builder.BuildDeleteSQL(context.Background(), nil, builderFromTS, builderToTS)
	require.NoError(t, err)
	require.Nil(t, sqls)
	empty := directInsertBatch(t, mp, nil, nil)
	sqls, err = builder.BuildInsertSQL(context.Background(), empty, builderFromTS, builderToTS)
	require.NoError(t, err)
	require.Nil(t, sqls)
	atomic := NewAtomicBatch(mp)
	t.Cleanup(atomic.Close)
	sqls, err = builder.buildAtomicInsertSQL(context.Background(), atomic, builderFromTS, builderToTS)
	require.NoError(t, err)
	require.Nil(t, sqls)
	sqls, err = builder.BuildDeleteSQL(context.Background(), atomic, builderFromTS, builderToTS)
	require.NoError(t, err)
	require.Nil(t, sqls)
}
