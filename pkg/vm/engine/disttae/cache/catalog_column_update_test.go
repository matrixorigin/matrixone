// Copyright 2022 Matrix Origin
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

package cache

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

func columnUpdateFixture(t testing.TB, count int) (*CatalogCache, *TableItem, Columns) {
	t.Helper()
	cols := make(Columns, count+1)
	typ := types.T_uint64.ToType()
	encoded, err := typ.Marshal()
	require.NoError(t, err)
	for i := range count {
		cols[i] = catalog.Column{AccountId: 7, DatabaseId: 8, TableId: 9, DatabaseName: "db", TableName: "t",
			Name: fmt.Sprintf("c%d", i), Num: int32(i + 1), Seqnum: uint16(i), Typ: encoded,
			ConstraintType: catalog.SystemColNoConstraint}
	}
	cols[0].ConstraintType = catalog.SystemColPKConstraint
	cols[0].HasDef = 1
	cols[0].DefaultExpr, err = (&plan.Default{OriginString: "7"}).Marshal()
	require.NoError(t, err)
	if count > 1 {
		cols[1].IsClusterBy = 1
		cols[1].HasUpdate = 1
		cols[1].UpdateExpr, err = (&plan.OnUpdate{OriginString: "c0 + 1"}).Marshal()
		require.NoError(t, err)
	}
	if count > 2 {
		cols[2].HasGenerated = 1
		cols[2].GeneratedExpr, err = (&plan.GeneratedCol{OriginString: "c0 + c1"}).Marshal()
		require.NoError(t, err)
	}
	rowType := types.T_Rowid.ToType()
	rowEncoded, err := rowType.Marshal()
	require.NoError(t, err)
	cols[count] = catalog.Column{AccountId: 7, DatabaseId: 8, TableId: 9, DatabaseName: "db", TableName: "t",
		Name: catalog.Row_ID, Num: int32(count + 1), Seqnum: uint16(count), Typ: rowEncoded, IsHidden: 1}
	item := &TableItem{AccountId: 7, DatabaseId: 8, Id: 9, Name: "t", Ts: timestamp.Timestamp{PhysicalTime: 100},
		PrimaryIdx: -1, PrimarySeqnum: -1, ClusterByIdx: -1}
	InitTableItemWithColumns(item, cols)
	cc := NewCatalog()
	cc.setTableItem(item, true)
	return cc, item, cols
}

func columnUpdateBatch(t testing.TB, cols Columns, at int64) *batch.Batch {
	t.Helper()
	mp := mpool.MustNewZero()
	packer := types.NewPacker()
	defer packer.Close()
	data, err := catalog.GenCreateColumnTuples(cols, mp, packer)
	require.NoError(t, err)
	bat := batch.NewWithSize(len(data.Vecs) + MO_OFF)
	copy(bat.Vecs[MO_OFF:], data.Vecs)
	bat.Vecs[MO_ROWID_IDX] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[MO_TIMESTAMP_IDX] = vector.NewVec(types.T_TS.ToType())
	for range cols {
		require.NoError(t, vector.AppendFixed(bat.Vecs[MO_ROWID_IDX], types.Rowid{}, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[MO_TIMESTAMP_IDX], types.BuildTS(at, 0), false, mp))
	}
	bat.SetRowCount(len(cols))
	t.Cleanup(func() { bat.Clean(mp); require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	return bat
}

func captureColumnMisses(t *testing.T) *atomic.Int64 {
	old := logutil.GetReportZapFunc()
	var misses atomic.Int64
	logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: func(enc zapcore.Encoder, entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
		if entry.Level == zapcore.ErrorLevel && strings.Contains(entry.Message, "not found when inserting columns") {
			misses.Add(1)
		}
		return old(enc, entry, fields)
	}})
	t.Cleanup(func() { logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: old}) })
	return &misses
}

func TestColumnOnlyUpdatePreservesPublishedSchema(t *testing.T) {
	misses := captureColumnMisses(t)
	cc, item, cols := columnUpdateFixture(t, 3)
	before := proto.Clone(item.TableDef)
	for _, subset := range []Columns{cols[:1], cols[:3]} {
		updated := append(Columns(nil), subset...)
		for i := range updated {
			updated[i].IsUnsigned = 1
		}
		cc.InsertColumns(columnUpdateBatch(t, updated, 200))
	}
	require.Zero(t, misses.Load())
	require.Equal(t, 1, cc.tables.data.Len())
	require.True(t, proto.Equal(before, item.TableDef))
	require.Equal(t, int64(100), item.Ts.PhysicalTime)
	require.Equal(t, 0, item.PrimaryIdx)
	require.Equal(t, 1, item.ClusterByIdx)
	lookup := &TableItem{AccountId: 7, DatabaseId: 8, Name: "t", Ts: timestamp.Timestamp{PhysicalTime: 300}}
	require.True(t, cc.GetTable(lookup))
	require.Same(t, item.TableDef, lookup.TableDef)
	// A real DDL still initializes the new table version, rather than taking the no-op path.
	next := &TableItem{AccountId: 7, DatabaseId: 8, Id: 9, Name: "t", Ts: timestamp.Timestamp{PhysicalTime: 300}, PrimaryIdx: -1, PrimarySeqnum: -1, ClusterByIdx: -1}
	cc.setTableItem(next, true)
	cols[0].Comment = "DDL change"
	cc.InsertColumns(columnUpdateBatch(t, cols, 300))
	require.Equal(t, "DDL change", next.TableDef.Cols[0].Comment)
	require.True(t, proto.Equal(before, item.TableDef))
	require.Zero(t, misses.Load())
}

func TestColumnOnlyUpdateRejectsUnprovenProjection(t *testing.T) {
	mutations := map[string]func(*TableItem, *Columns){
		"type":              func(_ *TableItem, c *Columns) { typ := types.T_uint32.ToType(); (*c)[0].Typ, _ = typ.Marshal() },
		"malformed type":    func(_ *TableItem, c *Columns) { (*c)[0].Typ = []byte{1} },
		"default":           func(_ *TableItem, c *Columns) { (*c)[0].DefaultExpr, _ = (&plan.Default{OriginString: "8"}).Marshal() },
		"malformed default": func(_ *TableItem, c *Columns) { (*c)[0].DefaultExpr = []byte{255} },
		"update": func(_ *TableItem, c *Columns) {
			(*c)[1].UpdateExpr, _ = (&plan.OnUpdate{OriginString: "c0 + 2"}).Marshal()
		},
		"malformed update": func(_ *TableItem, c *Columns) { (*c)[1].UpdateExpr = []byte{255} },
		"generated": func(_ *TableItem, c *Columns) {
			(*c)[2].GeneratedExpr, _ = (&plan.GeneratedCol{OriginString: "c0 - c1"}).Marshal()
		},
		"malformed generated": func(_ *TableItem, c *Columns) { (*c)[2].GeneratedExpr = []byte{255} },
		"name":                func(_ *TableItem, c *Columns) { (*c)[0].Name = "other" },
		"ordinal":             func(_ *TableItem, c *Columns) { (*c)[0].Num = 0 },
		"duplicate":           func(_ *TableItem, c *Columns) { *c = append(*c, (*c)[0]) },
		"comment":             func(_ *TableItem, c *Columns) { (*c)[0].Comment = "new" },
		"hidden":              func(_ *TableItem, c *Columns) { (*c)[0].IsHidden = 1 },
		"primary":             func(_ *TableItem, c *Columns) { (*c)[0].ConstraintType = catalog.SystemColNoConstraint },
		"constraint":          func(_ *TableItem, c *Columns) { (*c)[0].ConstraintType = "unknown" },
		"cluster":             func(_ *TableItem, c *Columns) { (*c)[0].IsClusterBy = 1 },
		"auto increment":      func(_ *TableItem, c *Columns) { (*c)[0].IsAutoIncrement = 1 },
		"seqnum":              func(_ *TableItem, c *Columns) { (*c)[0].Seqnum = 10 },
		"enum":                func(_ *TableItem, c *Columns) { (*c)[0].EnumValues = "a,b" },
		"invalid flag":        func(_ *TableItem, c *Columns) { (*c)[0].IsUnsigned = 2 },
		"no table definition": func(i *TableItem, _ *Columns) { i.TableDef = nil },
		"partial schema":      func(i *TableItem, _ *Columns) { i.Defs = i.Defs[:len(i.Defs)-1] },
		"plan mismatch":       func(i *TableItem, _ *Columns) { i.TableDef.Cols[0].Comment = "inconsistent" },
		"name map":            func(i *TableItem, _ *Columns) { i.TableDef.Name2ColIndex["c0"] = 2 },
		"key index":           func(i *TableItem, _ *Columns) { i.PrimaryIdx = -1 },
		"key seqnum":          func(i *TableItem, _ *Columns) { i.PrimarySeqnum = -1 },
		"cluster index":       func(i *TableItem, _ *Columns) { i.ClusterByIdx = -1 },
		"duplicate cached name": func(i *TableItem, _ *Columns) {
			for _, d := range i.Defs {
				if a, ok := d.(*engine.AttributeDef); ok && a.Attr.ID == 2 {
					a.Attr.Name = "C0"
				}
			}
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			misses := captureColumnMisses(t)
			cc, item, cols := columnUpdateFixture(t, 3)
			cols = cols[:3]
			for i := range cols {
				cols[i].IsUnsigned = 1
			}
			mutate(item, &cols)
			var before proto.Message
			if item.TableDef != nil {
				before = proto.Clone(item.TableDef)
			}
			require.NotPanics(t, func() { cc.InsertColumns(columnUpdateBatch(t, cols, 200)) })
			require.Equal(t, int64(1), misses.Load())
			require.Equal(t, 1, cc.tables.data.Len())
			if before != nil {
				require.True(t, proto.Equal(before, item.TableDef))
			}
		})
	}
}

func TestColumnOnlyUpdateRespectsTableIncarnation(t *testing.T) {
	for _, name := range []string{"missing", "tombstone", "recreated", "uninitialized", "same timestamp wrong id", "earlier timestamp"} {
		t.Run(name, func(t *testing.T) {
			misses := captureColumnMisses(t)
			cc, item, cols := columnUpdateFixture(t, 3)
			before := proto.Clone(item.TableDef)
			at := int64(200)
			switch name {
			case "missing":
				cc = NewCatalog()
			case "earlier timestamp":
				at = 50
			default:
				newer := *item
				newer.Ts.PhysicalTime = 150
				switch name {
				case "tombstone":
					newer.deleted = true
				case "recreated":
					newer.Id = 10
				case "uninitialized":
					newer.TableDef = nil
					newer.Defs = nil
				case "same timestamp wrong id":
					newer.Id = 10
					newer.Ts.PhysicalTime = 200
				}
				cc.setTableItem(&newer, true)
			}
			n := cc.tables.data.Len()
			cc.InsertColumns(columnUpdateBatch(t, cols[:1], at))
			require.Equal(t, int64(1), misses.Load())
			require.Equal(t, n, cc.tables.data.Len())
			require.True(t, proto.Equal(before, item.TableDef))
		})
	}
}

func TestColumnOnlyUpdateWithGCAndReaders(t *testing.T) {
	misses := captureColumnMisses(t)
	cc, item, cols := columnUpdateFixture(t, 3)
	older := *item
	older.Ts.PhysicalTime = 50
	cc.setTableItem(&older, true)
	bat := columnUpdateBatch(t, cols[:1], 200)
	before := proto.Clone(item.TableDef)
	start := make(chan struct{})
	var found bool
	var observed *plan.TableDef
	var wg sync.WaitGroup
	for _, action := range []func(){
		func() { cc.GC(timestamp.Timestamp{PhysicalTime: 150}) },
		func() { cc.InsertColumns(bat) },
		func() {
			q := &TableItem{AccountId: 7, DatabaseId: 8, Name: "t", Ts: timestamp.Timestamp{PhysicalTime: 200}}
			found = cc.GetTable(q)
			observed = q.TableDef
		},
	} {
		wg.Add(1)
		go func(f func()) { defer wg.Done(); <-start; f() }(action)
	}
	close(start)
	wg.Wait()
	require.True(t, found)
	require.Same(t, item.TableDef, observed)
	require.Equal(t, 1, cc.tables.data.Len())
	require.True(t, proto.Equal(before, item.TableDef))
	require.Zero(t, misses.Load())
}

func BenchmarkColumnOnlyUpdate(b *testing.B) {
	for _, width := range []int{16, 128, 1024} {
		b.Run(fmt.Sprint(width), func(b *testing.B) {
			cc, _, cols := columnUpdateFixture(b, width)
			page := min(width, 128)
			bat := columnUpdateBatch(b, cols[:page], 200)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				cc.InsertColumns(bat)
			}
		})
	}
}

func TestColumnOnlyUpdateComparesStringCollation(t *testing.T) {
	misses := captureColumnMisses(t)
	cc, item, cols := columnUpdateFixture(t, 3)
	typ := types.T_varchar.ToType()
	typ.Charset = 0
	typ.CollationVersion = 0
	cols[2].Typ, _ = typ.Marshal()
	InitTableItemWithColumns(item, cols)
	cc.InsertColumns(columnUpdateBatch(t, cols[2:3], 200))
	require.Zero(t, misses.Load())
	typ.Charset = 1
	cols[2].Typ, _ = typ.Marshal()
	cc.InsertColumns(columnUpdateBatch(t, cols[2:3], 300))
	require.Equal(t, int64(1), misses.Load())
	require.Equal(t, uint32(0), item.TableDef.Cols[2].Typ.Charset)
}

func BenchmarkColumnAttributeDecode(b *testing.B) {
	_, _, cols := columnUpdateFixture(b, 3)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = genTableDefOfColumn(cols[0])
	}
}
