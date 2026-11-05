//go:build mo_m0

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package m0catalog

import (
	"context"
	"fmt"
	"testing"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/stretchr/testify/require"
)

type m0CatalogValues struct {
	handle.Relation
	values map[uint16]any
	calls  int
}

func TestM0BindSchemaRejectsMissingInputs(t *testing.T) {
	_, err := BindSchema(context.Background(), nil, nil, nil, nil)
	require.Error(t, err)
	_, err = BindSchema(nil, nil, nil, nil, nil)
	require.Error(t, err)
}

func (r *m0CatalogValues) GetValue(_ *common.ID, _ uint32, column uint16, _ bool) (any, bool, error) {
	r.calls++
	value, ok := r.values[column]
	return value, !ok, nil
}

func TestM0MergeColumnCatalogBinding(t *testing.T) {
	table := catalog.MockTableEntryWithDB(catalog.MockDBEntryWithAccInfo(7, 41), 42)
	schema := catalog.NewEmptySchema("target")
	schema.AcInfo.TenantID = 7
	typ := types.T_decimal64.ToType()
	typ.Width, typ.Scale = 12, 2
	col := &catalog.ColDef{Name: "amount", Idx: 0, SeqNum: 0, Type: typ, NullAbility: true}
	typeBytes, err := types.Encode(&typ)
	require.NoError(t, err)
	expression, err := types.Encode(&plan.GeneratedCol{IsStored: true, OriginString: "a + 1"})
	require.NoError(t, err)
	makeValues := func() *m0CatalogValues {
		return &m0CatalogValues{values: map[uint16]any{
			pkgcatalog.MO_COLUMNS_ACCOUNT_ID_IDX: uint32(7), pkgcatalog.MO_COLUMNS_ATT_DATABASE_ID_IDX: uint64(41),
			pkgcatalog.MO_COLUMNS_ATT_DATABASE_IDX: []byte(""), pkgcatalog.MO_COLUMNS_ATT_RELNAME_IDX: []byte("target"),
			pkgcatalog.MO_COLUMNS_ATT_RELNAME_ID_IDX: uint64(42), pkgcatalog.MO_COLUMNS_ATTNAME_IDX: []byte("amount"),
			pkgcatalog.MO_COLUMNS_ATT_SEQNUM_IDX: uint16(0), pkgcatalog.MO_COLUMNS_ATTNUM_IDX: int32(1),
			pkgcatalog.MO_COLUMNS_ATTTYP_IDX: typeBytes, pkgcatalog.MO_COLUMNS_ATTNOTNULL_IDX: int8(0),
			pkgcatalog.MO_COLUMNS_ATT_IS_HIDDEN_IDX: int8(0), pkgcatalog.MO_COLUMNS_ATT_IS_CLUSTERBY: int8(0),
			pkgcatalog.MO_COLUMNS_ATT_IS_AUTO_INCREMENT_IDX: int8(0),
			pkgcatalog.MO_COLUMNS_ATT_CONSTRAINT_TYPE_IDX:   []byte(pkgcatalog.SystemColNoConstraint),
			pkgcatalog.MO_COLUMNS_ATT_DEFAULT_IDX:           []byte(nil), pkgcatalog.MO_COLUMNS_ATT_UPDATE_IDX: []byte(nil),
			pkgcatalog.MO_COLUMNS_ATTHASDEF_IDX: int8(0), pkgcatalog.MO_COLUMNS_ATT_HAS_UPDATE_IDX: int8(0), pkgcatalog.MO_COLUMNS_ATTISDROPPED_IDX: int8(0),
			pkgcatalog.MO_COLUMNS_ATT_ENUM_IDX: []byte(nil), pkgcatalog.MO_COLUMNS_ATT_HAS_GENERATED_IDX: int8(1),
			pkgcatalog.MO_COLUMNS_ATT_GENERATED_IDX: bytesCopy(expression),
		}}
	}
	values := makeValues()
	require.NoError(t, checkM0MergeColumn(context.Background(), values, nil, 0, table, schema, col))
	require.Equal(t, expression, col.GeneratedCol)
	values.values[pkgcatalog.MO_COLUMNS_ATT_GENERATED_IDX].([]byte)[0] ^= 0xff
	require.Equal(t, expression, col.GeneratedCol, "catalog value must be copied")
	for _, tc := range []struct {
		name   string
		column uint16
		value  any
	}{
		{"wrong account", pkgcatalog.MO_COLUMNS_ACCOUNT_ID_IDX, uint32(8)},
		{"wrong relation", pkgcatalog.MO_COLUMNS_ATT_RELNAME_ID_IDX, uint64(43)},
		{"wrong relation name", pkgcatalog.MO_COLUMNS_ATT_RELNAME_IDX, []byte("other")},
		{"wrong sequence", pkgcatalog.MO_COLUMNS_ATT_SEQNUM_IDX, uint16(1)},
		{"same oid wrong width", pkgcatalog.MO_COLUMNS_ATTTYP_IDX, func() []byte { t2 := typ; t2.Width++; b, _ := types.Encode(&t2); return b }()},
		{"invalid flag", pkgcatalog.MO_COLUMNS_ATTNOTNULL_IDX, int8(2)},
		{"default marker mismatch", pkgcatalog.MO_COLUMNS_ATTHASDEF_IDX, int8(1)},
		{"dropped column", pkgcatalog.MO_COLUMNS_ATTISDROPPED_IDX, int8(1)},
		{"invalid marker", pkgcatalog.MO_COLUMNS_ATT_HAS_GENERATED_IDX, int8(2)},
		{"invalid expression", pkgcatalog.MO_COLUMNS_ATT_GENERATED_IDX, []byte{0xff}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			values := makeValues()
			values.values[tc.column] = tc.value
			require.Error(t, checkM0MergeColumn(context.Background(), values, nil, 0, table, schema, col))
		})
	}
	t.Run("missing marker", func(t *testing.T) {
		values := makeValues()
		delete(values.values, pkgcatalog.MO_COLUMNS_ATT_HAS_GENERATED_IDX)
		require.Error(t, checkM0MergeColumn(context.Background(), values, nil, 0, table, schema, col))
	})
	t.Run("existing generated semantics conflict", func(t *testing.T) {
		values := makeValues()
		other, err := types.Encode(&plan.GeneratedCol{IsStored: false, OriginString: "a + 2"})
		require.NoError(t, err)
		values.values[pkgcatalog.MO_COLUMNS_ATT_GENERATED_IDX] = other
		require.Error(t, checkM0MergeColumn(context.Background(), values, nil, 0, table, schema, col))
		values.values[pkgcatalog.MO_COLUMNS_ATT_HAS_GENERATED_IDX] = int8(0)
		values.values[pkgcatalog.MO_COLUMNS_ATT_GENERATED_IDX] = []byte(nil)
		require.Error(t, checkM0MergeColumn(context.Background(), values, nil, 0, table, schema, col))
	})
	t.Run("restores absent field", func(t *testing.T) {
		copyCol := *col
		copyCol.GeneratedCol = nil
		require.NoError(t, checkM0MergeColumn(context.Background(), makeValues(), nil, 0, table, schema, &copyCol))
		require.Equal(t, expression, copyCol.GeneratedCol)
	})
	t.Run("generated source conflict is rejected", func(t *testing.T) {
		values := makeValues()
		other, err := types.Encode(&plan.GeneratedCol{IsStored: true, OriginString: "different spelling"})
		require.NoError(t, err)
		values.values[pkgcatalog.MO_COLUMNS_ATT_GENERATED_IDX] = other
		copyCol := *col
		require.Error(t, checkM0MergeColumn(context.Background(), values, nil, 0, table, schema, &copyCol))
	})
	t.Run("canceled before catalog read", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		values := makeValues()
		copyCol := *col
		err := checkM0MergeColumn(ctx, values, nil, 0, table, schema, &copyCol)
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, values.calls)
		require.Equal(t, col.GeneratedCol, copyCol.GeneratedCol)
	})
}

func bytesCopy(v []byte) []byte { return append([]byte(nil), v...) }

type m0CatalogScanObject struct {
	handle.Object
	batch   *containers.Batch
	scanErr error
	closed  bool
	scanned bool
}

func (o *m0CatalogScanObject) HybridScan(ctx context.Context, bat **containers.Batch, _ uint16, idxs []int, _ *mpool.MPool) error {
	o.scanned = true
	if len(idxs) != pkgcatalog.MO_COLUMNS_MAXIDX+1 {
		return moerr.NewInternalErrorNoCtx("unexpected scan width")
	}
	*bat = o.batch
	if o.scanErr != nil {
		return o.scanErr
	}
	return ctx.Err()
}

func (o *m0CatalogScanObject) Close() error { o.closed = true; return nil }

type m0CatalogScanRelation struct {
	handle.Relation
	object    *m0CatalogScanObject
	workspace bool
}

func (r *m0CatalogScanRelation) GetObject(_ *types.Objectid, _ bool) (handle.Object, error) {
	return r.object, nil
}

func (r *m0CatalogScanRelation) FillInWorkspaceDeletes(_ types.Blockid, deleted **nulls.Nulls, _ uint64) error {
	if r.workspace {
		if *deleted == nil {
			*deleted = nulls.NewWithSize(1)
		}
		(*deleted).Add(0)
	}
	return nil
}

func TestM0MergeCatalogRowScanCleanup(t *testing.T) {
	t.Run("partial scan cancellation", func(t *testing.T) {
		released := 0
		batch := containers.NewBatch()
		batch.DataRelease = func() { released++ }
		object := &m0CatalogScanObject{batch: batch, scanErr: context.Canceled}
		rel := &m0CatalogScanRelation{object: object}
		err := checkM0MergeCatalogRow(context.Background(), rel, &common.ID{}, 0, nil, nil, nil)
		require.ErrorIs(t, err, context.Canceled)
		require.True(t, object.scanned)
		require.True(t, object.closed)
		require.Equal(t, 1, released)
	})
	t.Run("short vector", func(t *testing.T) {
		batch := containers.NewBatch()
		for i := 0; i <= pkgcatalog.MO_COLUMNS_MAXIDX; i++ {
			v := containers.MakeVector(types.T_int8.ToType(), common.DefaultAllocator)
			if i != pkgcatalog.MO_COLUMNS_ATT_GENERATED_IDX {
				v.Append(int8(0), false)
			}
			batch.AddVector(fmt.Sprintf("col%d", i), v)
		}
		released := 0
		batch.DataRelease = func() { released++ }
		object := &m0CatalogScanObject{batch: batch}
		err := checkM0MergeCatalogRow(context.Background(), &m0CatalogScanRelation{object: object}, &common.ID{}, 0, nil, nil, nil)
		require.ErrorContains(t, err, "catalog column unavailable")
		require.True(t, object.closed)
		require.Equal(t, 1, released)
		for _, vec := range batch.Vecs {
			require.Nil(t, vec)
		}
	})
	for _, tc := range []struct {
		name      string
		committed bool
		workspace bool
	}{
		{"committed deletion", true, false},
		{"workspace deletion", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch := containers.NewBatch()
			for i := 0; i <= pkgcatalog.MO_COLUMNS_MAXIDX; i++ {
				v := containers.MakeVector(types.T_int8.ToType(), common.DefaultAllocator)
				v.Append(int8(0), false)
				batch.AddVector(fmt.Sprintf("col%d", i), v)
			}
			if tc.committed {
				batch.Delete(0)
			}
			released := 0
			batch.DataRelease = func() { released++ }
			object := &m0CatalogScanObject{batch: batch}
			rel := &m0CatalogScanRelation{object: object, workspace: tc.workspace}
			err := checkM0MergeCatalogRow(context.Background(), rel, &common.ID{}, 0, nil, nil, nil)
			require.ErrorContains(t, err, "catalog row is deleted")
			require.True(t, object.closed)
			require.Equal(t, 1, released)
			for _, vec := range batch.Vecs {
				require.Nil(t, vec)
			}
		})
	}
}
