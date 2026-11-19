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

package cache

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

// decimalColumnUpdateFixture builds a published table whose attributed
// DECIMAL columns carry the UNSIGNED/ZEROFILL mask in att_is_unsigned: c0 is
// a primary uint64 (mirrors the shared fixture's primary), c1 is plain
// DECIMAL(10,2) mask 0, c2 is unsigned DECIMAL mask 1, c3 is zerofill
// DECIMAL mask 3, and the trailing hidden ROWID.
func decimalColumnUpdateFixture(t *testing.T) (*CatalogCache, *TableItem, Columns) {
	t.Helper()
	pkType := types.T_uint64.ToType()
	pkEncoded, err := pkType.Marshal()
	require.NoError(t, err)
	decimalType := types.New(types.T_decimal64, 10, 2)
	decimalEncoded, err := decimalType.Marshal()
	require.NoError(t, err)
	newCol := func(num int32, seq uint16, name string, typ []byte) catalog.Column {
		return catalog.Column{AccountId: 7, DatabaseId: 8, TableId: 9, DatabaseName: "db", TableName: "t",
			Name: name, Num: num, Seqnum: seq, Typ: typ, ConstraintType: catalog.SystemColNoConstraint}
	}
	rowType := types.T_Rowid.ToType()
	rowEncoded, err := rowType.Marshal()
	require.NoError(t, err)
	cols := Columns{
		newCol(1, 0, "c0", pkEncoded),
		newCol(2, 1, "c1", decimalEncoded),
		newCol(3, 2, "c2", decimalEncoded),
		newCol(4, 3, "c3", decimalEncoded),
		{AccountId: 7, DatabaseId: 8, TableId: 9, DatabaseName: "db", TableName: "t",
			Name: catalog.Row_ID, Num: 5, Seqnum: 4, Typ: rowEncoded, IsHidden: 1},
	}
	cols[0].ConstraintType = catalog.SystemColPKConstraint
	cols[0].HasDef = 1
	cols[0].DefaultExpr, err = (&plan.Default{OriginString: "7"}).Marshal()
	require.NoError(t, err)
	cols[2].IsUnsigned = 1 // unsigned decimal
	cols[3].IsUnsigned = 3 // unsigned zerofill decimal
	item := &TableItem{AccountId: 7, DatabaseId: 8, Id: 9, Name: "t", Ts: timestamp.Timestamp{PhysicalTime: 100},
		PrimaryIdx: -1, PrimarySeqnum: -1, ClusterByIdx: -1}
	InitTableItemWithColumns(item, cols)
	cc := NewCatalog()
	cc.setTableItem(item, true)
	return cc, item, cols
}

// TestColumnOnlyUpdateNeverDropsNumericAttributes pins the acceptance rule
// against the attributed-DECIMAL mask: the rule never accepts a column-only
// update whose accepted projection would lose the column's
// UNSIGNED/ZEROFILL mask, and the mapping from an accepted column row to the
// projected plan type is deterministic.
func TestColumnOnlyUpdateNeverDropsNumericAttributes(t *testing.T) {
	t.Run("identical masks replay instead of merging", func(t *testing.T) {
		misses := captureColumnMisses(t)
		cc, item, cols := decimalColumnUpdateFixture(t)
		before := proto.Clone(item.TableDef)
		// The unsigned (mask 1) and zerofill (mask 3) columns alone form a
		// column-only update: the rule must refuse them rather than accept a
		// projection that could drop the mask. Mask 1 fails the integer-only
		// unsigned guard; mask 3 fails the flag validity check.
		for _, subset := range []Columns{cols[2:3], cols[3:4], cols[1:4]} {
			updated := append(Columns(nil), subset...)
			require.NotPanics(t, func() { cc.InsertColumns(columnUpdateBatch(t, updated, 200)) })
		}
		require.Equal(t, int64(3), misses.Load(),
			"every attributed-decimal column-only update must fall back to raw catalog replay")
		require.True(t, proto.Equal(before, item.TableDef))
		require.Equal(t, int64(100), item.Ts.PhysicalTime)
	})

	t.Run("mask drift replays instead of accepting", func(t *testing.T) {
		misses := captureColumnMisses(t)
		cc, item, cols := decimalColumnUpdateFixture(t)
		before := proto.Clone(item.TableDef)
		updated := append(Columns(nil), cols[2:3]...)
		updated[0].IsUnsigned = 0 // published mask 1, incoming mask 0
		cc.InsertColumns(columnUpdateBatch(t, updated, 200))
		require.Equal(t, int64(1), misses.Load(),
			"a mask change must go through the raw replay, never a silent projection merge")
		require.True(t, proto.Equal(before, item.TableDef))
	})

	t.Run("plain decimal acceptance preserves the zero mask", func(t *testing.T) {
		misses := captureColumnMisses(t)
		cc, item, cols := decimalColumnUpdateFixture(t)
		before := proto.Clone(item.TableDef)
		updated := append(Columns(nil), cols[1:2]...)
		cc.InsertColumns(columnUpdateBatch(t, updated, 200))
		require.Zero(t, misses.Load())
		require.True(t, proto.Equal(before, item.TableDef))
		// The accepted projection keeps the plain column's zero mask.
		require.Zero(t, item.TableDef.Cols[1].Typ.NumericAttributes)
	})

	t.Run("accepted mapping is deterministic from the column row", func(t *testing.T) {
		_, _, cols := decimalColumnUpdateFixture(t)
		def, err := decodeColumnAttribute(cols[2])
		require.NoError(t, err)
		require.Equal(t, uint8(1), def.Attr.NumericAttributes)
		def, err = decodeColumnAttribute(cols[3])
		require.NoError(t, err)
		require.Equal(t, uint8(3), def.Attr.NumericAttributes)
		def, err = decodeColumnAttribute(cols[1])
		require.NoError(t, err)
		require.Zero(t, def.Attr.NumericAttributes)
		// Legacy unsigned integers keep the mask at zero: att_is_unsigned
		// remains a boolean for them and never feeds NumericAttributes.
		uintCol := cols[0]
		uintCol.IsUnsigned = 1
		def, err = decodeColumnAttribute(uintCol)
		require.NoError(t, err)
		require.Zero(t, def.Attr.NumericAttributes, "unsigned integer columns carry no DECIMAL mask")
	})
}
