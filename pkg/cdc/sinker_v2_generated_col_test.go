// Copyright 2026 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// generatedMiddleTableDef mirrors a source table whose visible columns are
// [id, g generated, a, h generated]: two generated columns, one stored between
// replicated columns so any slot-shifting bug moves base values.
func generatedMiddleTableDef() *plan.TableDef {
	return &plan.TableDef{
		Name: "gen_t",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: "g", Typ: plan.Type{Id: int32(types.T_int32)},
				GeneratedCol: &plan.GeneratedCol{OriginString: "a + 1", IsStored: true}},
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: "h", Typ: plan.Type{Id: int32(types.T_varchar)},
				GeneratedCol: &plan.GeneratedCol{OriginString: "concat('x', a)", IsStored: true}},
		},
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}},
		Name2ColIndex: map[string]int32{"id": 0, "g": 1, "a": 2, "h": 3},
	}
}

// generatedSourceBatch builds the user-defined-column prefix plus a commit-ts
// vector, the snapshot data-batch layout CDC reads: user defined cols | ts.
// Every column carries a distinguishable value so a misaligned projection
// cannot pass the assertions by coincidence. The batch cleanup is registered
// before the appends so a failed append still releases the fixture memory.
func generatedSourceBatch(t *testing.T, mp *mpool.MPool, ids, gens, bases []int32) *batch.Batch {
	t.Helper()
	require.Equal(t, len(ids), len(gens))
	require.Equal(t, len(ids), len(bases))
	bat := batch.NewWithSize(5)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType(), mp)
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType(), mp)
	bat.Vecs[2] = vector.NewVec(types.T_int32.ToType(), mp)
	bat.Vecs[3] = vector.NewVec(types.T_varchar.ToType(), mp)
	bat.Vecs[4] = vector.NewVec(types.T_TS.ToType(), mp)
	t.Cleanup(func() { bat.Clean(mp) })
	for i := range ids {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], ids[i], false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], gens[i], false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], bases[i], false, mp))
		str := fmt.Sprintf("gen%d", gens[i])
		require.NoError(t, vector.AppendBytes(bat.Vecs[3], []byte(str), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[4], builderFromTS, false, mp))
	}
	bat.SetRowCount(len(ids))
	return bat
}

// omitGeneratedContractFor preserves the committed builder expectations: a
// generated target accepted for recompute omits every source-generated slot,
// so the admission-captured copy mask marks exactly the non-generated
// ordinals for value copying.
func omitGeneratedContractFor(tableDef *plan.TableDef) *CDCTargetGenerationContract {
	contract := &CDCTargetGenerationContract{}
	if tableDef == nil {
		return contract
	}
	for _, col := range tableDef.Cols {
		if col == nil {
			continue
		}
		if _, internal := catalog.InternalColumns[col.Name]; internal || col.Hidden {
			continue
		}
		contract.Copy = append(contract.Copy, col.GeneratedCol == nil)
	}
	return contract
}

func TestNewCDCStatementBuilderGeneratedColumnsProjection(t *testing.T) {
	builder, err := NewCDCStatementBuilder("gen_db", "gen_t", generatedMiddleTableDef(), 1024*1024, false, omitGeneratedContractFor(generatedMiddleTableDef()))
	require.NoError(t, err)
	// Visible batch slots are [0:id, 1:g, 2:a, 3:h]; only id and a replicate,
	// and both must survive at their batch positions 0 and 2.
	require.Equal(t, []byte("INSERT INTO `gen_db`.`gen_t` (`id`,`a`) VALUES "), builder.insertStem)

	wantSuffix := []byte(" ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`a`=VALUES(`a`);")
	require.Equal(t, wantSuffix, builder.insertSuffix)
}

func TestBuildInsertSQLOmitsGeneratedColumnsMiddleAlignment(t *testing.T) {
	mp := newBuilderTestMPool(t)
	builder, err := NewCDCStatementBuilder("gen_db", "gen_t", generatedMiddleTableDef(), 1024*1024, false, omitGeneratedContractFor(generatedMiddleTableDef()))
	require.NoError(t, err)

	// g and h values are far from any base value; a correct projection must
	// pick a's values (10,20) for the second INSERT column, not g's (1000,2000).
	bat := generatedSourceBatch(t, mp,
		[]int32{1, 2},
		[]int32{1000, 2000},
		[]int32{10, 20})

	sqls, err := builder.BuildInsertSQL(context.Background(), bat, builderFromTS, builderToTS)
	require.NoError(t, err)
	require.Len(t, sqls, 1)

	want := fmt.Sprintf("/* [%s, %s) */ ", builderFromTS.ToString(), builderToTS.ToString()) +
		"INSERT INTO `gen_db`.`gen_t` (`id`,`a`) VALUES (1,10),(2,20)" +
		" ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`a`=VALUES(`a`);"
	require.Equal(t, want, unpaddedSQL(sqls[0]))
}

// generatedTailBatch builds a tail data batch (user defined cols | commit-ts)
// and transfers its ownership to an AtomicBatch exactly once, mirroring the
// ownership pattern of atomicStringBatch. The ts column is the last vector and
// the single pk column id is the second-to-consume index after the commit-ts.
func generatedTailBatch(
	t *testing.T, mp *mpool.MPool, atomic *AtomicBatch, ids, gens, bases []int32,
) {
	t.Helper()
	require.Equal(t, len(ids), len(gens))
	require.Equal(t, len(ids), len(bases))
	bat := batch.NewWithSize(5)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType(), mp)
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType(), mp)
	bat.Vecs[2] = vector.NewVec(types.T_int32.ToType(), mp)
	bat.Vecs[3] = vector.NewVec(types.T_varchar.ToType(), mp)
	bat.Vecs[4] = vector.NewVec(types.T_TS.ToType(), mp)
	// Register both fixture cleanups before any operation below can fail, so a
	// mid-build abort still releases the batch and packer memory.
	owned := true
	t.Cleanup(func() {
		if owned {
			bat.Clean(mp)
		}
	})
	packer := types.NewPacker()
	packerOpen := true
	t.Cleanup(func() {
		if packerOpen {
			packer.Close()
		}
	})
	for i := range ids {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], ids[i], false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], gens[i], false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], bases[i], false, mp))
		str := fmt.Sprintf("gen%d", gens[i])
		require.NoError(t, vector.AppendBytes(bat.Vecs[3], []byte(str), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[4], builderFromTS, false, mp))
	}
	bat.SetRowCount(len(ids))
	atomic.Append(packer, bat, 4, 0)
	// Explicit ownership transfer: the AtomicBatch owns the batch and the
	// packer is closed inline; both conditional cleanups disarm here.
	owned = false
	packerOpen = false
	packer.Close()
}

func TestBuildAtomicInsertSQLOmitsGeneratedColumnsMiddleAlignment(t *testing.T) {
	mp := newBuilderTestMPool(t)
	builder, err := NewCDCStatementBuilder("gen_db", "gen_t", generatedMiddleTableDef(), 1024*1024, false, omitGeneratedContractFor(generatedMiddleTableDef()))
	require.NoError(t, err)

	// Tail batches carry user defined cols | cpk | commit-ts. The single pk id
	// is the pk column, so the layout is [id, g, a, h, ts]: pk col idx 0, ts idx 4.
	atomic := NewAtomicBatch(mp)
	// Register the AtomicBatch cleanup before the batch can be appended so a
	// failed build still releases its memory.
	t.Cleanup(atomic.Close)
	generatedTailBatch(t, mp, atomic,
		[]int32{3, 4},
		[]int32{3000, 4000},
		[]int32{30, 40})

	sqls, err := builder.buildAtomicInsertSQL(context.Background(), atomic, builderFromTS, builderToTS)
	require.NoError(t, err)
	require.Len(t, sqls, 1)

	want := fmt.Sprintf("/* [%s, %s) */ ", builderFromTS.ToString(), builderToTS.ToString()) +
		"INSERT INTO `gen_db`.`gen_t` (`id`,`a`) VALUES (3,30),(4,40)" +
		" ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`a`=VALUES(`a`);"
	require.Equal(t, want, unpaddedSQL(sqls[0]))
}

func TestNewCDCStatementBuilderReplaceOmitsGeneratedColumns(t *testing.T) {
	def := generatedMiddleTableDef()
	def.Indexes = []*plan.IndexDef{{IndexName: "uk_a", Parts: []string{"a"}, Unique: true}}
	builder, err := NewCDCStatementBuilder("gen_db", "gen_t", def, 1024*1024, false, omitGeneratedContractFor(def))
	require.NoError(t, err)
	require.Equal(t, []byte("REPLACE INTO `gen_db`.`gen_t` (`id`,`a`) VALUES "), builder.insertStem)
	require.Equal(t, []byte(";"), builder.insertSuffix)
}

func TestNewCDCStatementBuilderAllVisibleColumnsGenerated(t *testing.T) {
	def := &plan.TableDef{
		Name: "all_gen",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: "g", Typ: plan.Type{Id: int32(types.T_int32)},
				GeneratedCol: &plan.GeneratedCol{IsStored: true}},
		},
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}},
		Name2ColIndex: map[string]int32{"id": 0, "g": 1},
	}
	// Insertable columns exist (id); the generated one is simply skipped.
	builder, err := NewCDCStatementBuilder("gen_db", "all_gen", def, 1024, false, omitGeneratedContractFor(def))
	require.NoError(t, err)
	require.Equal(t, []byte("INSERT INTO `gen_db`.`all_gen` (`id`) VALUES "), builder.insertStem)

	onlyGenerated := &plan.TableDef{
		Name: "only_gen",
		Cols: []*plan.ColDef{
			{Name: "g", Typ: plan.Type{Id: int32(types.T_int32)},
				GeneratedCol: &plan.GeneratedCol{IsStored: true}},
		},
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"g"}},
		Name2ColIndex: map[string]int32{"g": 0},
	}
	_, err = NewCDCStatementBuilder("gen_db", "only_gen", onlyGenerated, 1024, false, omitGeneratedContractFor(onlyGenerated))
	require.ErrorContains(t, err, "no insertable columns")
}

func TestBuildInsertSQLGeneratedFreeTableUnchanged(t *testing.T) {
	mp := newBuilderTestMPool(t)
	builder, err := NewCDCStatementBuilder("test_db", "users", standardBuilderTableDef(), 1024*1024, false, omitGeneratedContractFor(standardBuilderTableDef()))
	require.NoError(t, err)

	bat := directInsertBatch(t, mp, []int32{1, 2}, []string{"alice", "bob"})
	sqls, err := builder.BuildInsertSQL(context.Background(), bat, builderFromTS, builderToTS)
	require.NoError(t, err)
	require.Len(t, sqls, 1)

	want := fmt.Sprintf("/* [%s, %s) */ ", builderFromTS.ToString(), builderToTS.ToString()) +
		"INSERT INTO `test_db`.`users` (`id`,`name`) VALUES (1,'alice'),(2,'bob')" +
		" ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`name`=VALUES(`name`);"
	require.Equal(t, want, unpaddedSQL(sqls[0]))
}

// generatedGeometryTableDef mirrors a source table whose generated column has
// a type the generic row extractor cannot decode: geometry. Generated column
// values, stored or virtual, are physically materialized by the write path, so
// the CDC changes handle delivers the vector in the source batch prefix.
func generatedGeometryTableDef() *plan.TableDef {
	return &plan.TableDef{
		Name: "geo_gen_t",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: "g", Typ: plan.Type{Id: int32(types.T_geometry)},
				GeneratedCol: &plan.GeneratedCol{OriginString: "ST_GeomFromText('POINT(1 2)')", IsStored: true}},
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int32)}},
		},
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}},
		Name2ColIndex: map[string]int32{"id": 0, "g": 1, "a": 2},
	}
}

// geometryGeneratedSourceBatch builds [id, g(T_geometry), a, ts] with opaque
// WKB bytes in the generated slot. extractRowFromVector has no T_geometry
// case and fails on the first decoded row, so building the insert SQL proves
// the builder never decodes the dropped generated slots. The guarded cleanup
// is registered before the appends so a failed append still releases the
// batch; the returned release func disarms it for an explicit ownership
// transfer (the caller's AtomicBatch.Close then cleans the batch once).
func geometryGeneratedSourceBatch(t *testing.T, mp *mpool.MPool, ids, bases []int32) (bat *batch.Batch, release func()) {
	t.Helper()
	require.Equal(t, len(ids), len(bases))
	bat = batch.NewWithSize(4)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType(), mp)
	bat.Vecs[1] = vector.NewVec(types.T_geometry.ToType(), mp)
	bat.Vecs[2] = vector.NewVec(types.T_int32.ToType(), mp)
	bat.Vecs[3] = vector.NewVec(types.T_TS.ToType(), mp)
	owned := true
	t.Cleanup(func() {
		if owned {
			bat.Clean(mp)
		}
	})
	for i := range ids {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], ids[i], false, mp))
		// WKB for POINT(1 2); the value is opaque to the sink SQL.
		wkb := []byte{0x01, 0x01, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40}
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], wkb, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], bases[i], false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[3], builderFromTS, false, mp))
	}
	bat.SetRowCount(len(ids))
	return bat, func() { owned = false }
}

// TestBuildInsertSQLSkipsDecodingDroppedGeneratedSlots is the decisive
// focused witness for the slot projection: a generated column whose type the
// generic extractor rejects (geometry) must never be decoded even though its
// value is omitted from the SQL, and the replicated columns must stay
// aligned, on both the snapshot and the atomic tail path.
func TestBuildInsertSQLSkipsDecodingDroppedGeneratedSlots(t *testing.T) {
	mp := newBuilderTestMPool(t)

	want := func(values string) string {
		return fmt.Sprintf("/* [%s, %s) */ ", builderFromTS.ToString(), builderToTS.ToString()) +
			"INSERT INTO `gen_db`.`geo_gen_t` (`id`,`a`) VALUES " + values +
			" ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`a`=VALUES(`a`);"
	}

	t.Run("snapshot", func(t *testing.T) {
		builder, err := NewCDCStatementBuilder("gen_db", "geo_gen_t", generatedGeometryTableDef(), 1024*1024, false, omitGeneratedContractFor(generatedGeometryTableDef()))
		require.NoError(t, err)
		bat, _ := geometryGeneratedSourceBatch(t, mp, []int32{5, 6}, []int32{50, 60})
		sqls, err := builder.BuildInsertSQL(context.Background(), bat, builderFromTS, builderToTS)
		require.NoError(t, err)
		require.Len(t, sqls, 1)
		require.Equal(t, want("(5,50),(6,60)"), unpaddedSQL(sqls[0]))
	})

	t.Run("tail", func(t *testing.T) {
		builder, err := NewCDCStatementBuilder("gen_db", "geo_gen_t", generatedGeometryTableDef(), 1024*1024, false, omitGeneratedContractFor(generatedGeometryTableDef()))
		require.NoError(t, err)
		// The guarded acquisition cleanup covers failed appends; release()
		// transfers ownership explicitly after Append so the AtomicBatch is
		// the single owner and its Close cleans the batch exactly once.
		bat, release := geometryGeneratedSourceBatch(t, mp, []int32{7, 8}, []int32{70, 80})
		atomic := NewAtomicBatch(mp)
		t.Cleanup(atomic.Close)
		packer := types.NewPacker()
		packerOpen := true
		t.Cleanup(func() {
			if packerOpen {
				packer.Close()
			}
		})
		// layout [id, g, a, ts]: single pk id is column 0, commit-ts is column 3.
		atomic.Append(packer, bat, 3, 0)
		release()
		packerOpen = false
		packer.Close()
		sqls, err := builder.buildAtomicInsertSQL(context.Background(), atomic, builderFromTS, builderToTS)
		require.NoError(t, err)
		require.Len(t, sqls, 1)
		require.Equal(t, want("(7,70),(8,80)"), unpaddedSQL(sqls[0]))
	})
}
