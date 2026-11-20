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
	"fmt"

	"testing"

	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestCandidateSchemaDefaultsAndResidual(t *testing.T) {
	typ := types.T_int64.ToType()
	unusual := typ
	unusual.Size++
	unusual.SetNotNull(true)
	attrs := []engine.Attribute{
		{Name: "a", Type: typ, Alg: compress.Lz4, Default: &plan.Default{}},
		{Name: "b", Type: typ, Alg: compress.Lz4, Default: &plan.Default{}},
		{Name: "c", Type: unusual, Alg: compress.Lz4, IsRowId: true, Default: &plan.Default{NullAbility: true}},
		{Name: "d", Type: typ, Alg: compress.Lz4Chunked, IsHidden: true, Default: &plan.Default{XXX_unrecognized: []byte{}}},
		{Name: "e", Type: typ},
		{Name: "f", Type: typ, Default: &plan.Default{XXX_sizecache: 7}},
		{Name: "g", Type: typ, Default: &plan.Default{NullAbility: true}},
		{Name: "h", Type: typ, Default: &plan.Default{XXX_unrecognized: []byte{0xa0, 0x06, 0x01}}},
	}
	item := &TableItem{Name: "t"}
	for _, a := range attrs {
		item.Defs = append(item.Defs, &engine.AttributeDef{Attr: a})
	}
	s := newCandidateSchema(item)
	require.NotNil(t, s)
	require.Equal(t, 6, len(s.residual)) // Lz4 defaults are omitted; None remains explicit.
	require.Same(t, s.planner.Cols[0].Default, s.planner.Cols[1].Default)
	require.NotSame(t, s.planner.Cols[0].Default, s.planner.Cols[2].Default)
	require.NotNil(t, s.planner.Cols[3].Default.XXX_unrecognized)
	require.Nil(t, s.planner.Cols[4].Default)
	require.Equal(t, int32(7), s.planner.Cols[5].Default.XXX_sizecache)
	require.Same(t, s.planner.Cols[2].Default, s.planner.Cols[6].Default)
	require.Equal(t, []byte{0xa0, 0x06, 0x01}, s.planner.Cols[7].Default.XXX_unrecognized)
	s.planner.Cols[7].Default.XXX_unrecognized[2] = 2
	require.Equal(t, byte(1), attrs[7].Default.XXX_unrecognized[2])
	reconstructed := (SchemaSource{schema: s}).Columns(false)
	for i, a := range attrs {
		require.Equal(t, a.Type, reconstructed[i].Type)
		require.Equal(t, a.Alg, reconstructed[i].Alg)
		require.Equal(t, a.IsRowId, reconstructed[i].IsRowId)
	}
}

func TestCandidateUnsupportedTypeIsResidual(t *testing.T) {
	for _, id := range []int32{-256, 256} {
		_, ok := candidateType(plan.Type{Id: id})
		require.False(t, ok, "planner ID %d must not wrap to T_any", id)
	}
	unknown := plan.Type{Id: 255}
	_, ok := candidateType(unknown)
	require.False(t, ok)
	original := types.Type{Oid: types.T(255), Size: 17}
	s := &candidateSchema{
		planner:  &plan.TableDef{Cols: []*plan.ColDef{{Typ: unknown}}},
		residual: []candidateResidual{{index: 0, typ: original, alg: compress.None, rowID: true}},
	}
	attr := (SchemaSource{schema: s}).Columns(false)[0]
	require.Equal(t, original, attr.Type)
	require.Equal(t, compress.T(compress.None), attr.Alg)
	require.True(t, attr.IsRowId)
}

func TestCandidatePrimaryColumnsPreserveSelectedMetadata(t *testing.T) {
	typ := types.T_int64.ToType()
	original := types.Type{Oid: types.T(255), Size: 17}
	s := SchemaSource{schema: &candidateSchema{
		planner: &plan.TableDef{Cols: []*plan.ColDef{
			{OriginName: "payload", Typ: plan.Type{Id: int32(typ.Oid)}},
			{OriginName: "first", Typ: plan.Type{Id: int32(typ.Oid)}, Primary: true, Default: &plan.Default{OriginString: "7"}},
			{OriginName: "hidden_key", Typ: plan.Type{Id: int32(original.Oid)}, Primary: true, Hidden: true,
				OnUpdate: &plan.OnUpdate{OriginString: "8"}, GeneratedCol: &plan.GeneratedCol{OriginString: "9"}},
		}},
		residual: []candidateResidual{
			{index: 0, typ: typ, alg: compress.None},
			{index: 2, typ: original, alg: compress.None, rowID: true},
		},
	}}
	keys := s.Columns(true)
	require.Len(t, keys, 2)
	require.Equal(t, "first", keys[0].Name)
	require.Equal(t, typ, keys[0].Type)
	require.Equal(t, "hidden_key", keys[1].Name)
	require.Equal(t, original, keys[1].Type)
	require.Equal(t, compress.T(compress.None), keys[1].Alg)
	require.True(t, keys[1].IsHidden)
	require.True(t, keys[1].IsRowId)
	keys[0].Default.OriginString = "changed"
	keys[1].OnUpdate.OriginString = "changed"
	keys[1].GeneratedCol.OriginString = "changed"
	again := s.Columns(true)
	require.Equal(t, "7", again[0].Default.OriginString)
	require.Equal(t, "8", again[1].OnUpdate.OriginString)
	require.Equal(t, "9", again[1].GeneratedCol.OriginString)
}

// Exercise every representable OID, including values outside the known type set.
// The engine value is the oracle: reconstructing from the planner must never
// silently discard fields that the planner does not represent.
func TestCandidateAllTypeIDsAndWireOwnership(t *testing.T) {
	raw := []byte{0xa0, 0x06, 0x07, 0xa0, 0x06, 0x08}
	item := &TableItem{Name: "types", Constraint: append([]byte(nil), raw...)}
	var expected []engine.Attribute
	for oid := 0; oid < 256; oid++ {
		typ := types.Type{Oid: types.T(oid), Size: int32(oid + 1), Width: int32(oid + 2), Scale: int32(oid + 3), Charset: uint8(oid), CollationVersion: uint8(255 - oid)}
		typ.SetNotNull(oid%2 == 0)
		a := engine.Attribute{Name: fmt.Sprintf("c%d", oid), Type: typ, Alg: compress.T(oid % 3), IsRowId: oid%2 == 1}
		expected = append(expected, a)
		item.Defs = append(item.Defs, &engine.AttributeDef{Attr: a})
	}
	s := newCandidateSchema(item)
	require.NotNil(t, s)
	for i, attr := range (SchemaSource{schema: s}).Columns(false) {
		require.Equal(t, expected[i], *attr, "oid %d", i)
	}
	require.Equal(t, raw, s.constraint.raw)
	item.Constraint[0] = 0
	require.Equal(t, raw, s.constraint.raw)
}

// Encoding shared defaults must remain read-only while a private planner copy
// is modified. The start barrier makes the operations overlap under -race.
func TestCandidateConcurrentEncodingAndPrivateWrite(t *testing.T) {
	item := &TableItem{Name: "t", Defs: []engine.TableDef{
		&engine.AttributeDef{Attr: engine.Attribute{Name: "a", Type: types.T_int64.ToType(), Default: &plan.Default{NullAbility: true}}},
		&engine.AttributeDef{Attr: engine.Attribute{Name: "b", Type: types.T_int64.ToType(), Default: &plan.Default{NullAbility: true}}},
	}}
	s := newCandidateSchema(item)
	require.Same(t, s.planner.Cols[0].Default, s.planner.Cols[1].Default)
	start := make(chan struct{})
	done := make(chan error, 2)
	go func() {
		<-start
		for i := 0; i < 100; i++ {
			if _, err := s.planner.Marshal(); err != nil {
				done <- err
				return
			}
			if _, err := s.planner.Cols[0].Default.XXX_Marshal(nil, true); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	go func() {
		<-start
		for i := 0; i < 100; i++ {
			private := s.projection()
			private.Cols[0].Default.NullAbility = false
		}
		done <- nil
	}()
	close(start)
	require.NoError(t, <-done)
	require.NoError(t, <-done)
	require.True(t, s.planner.Cols[0].Default.NullAbility)
	require.True(t, s.planner.Cols[1].Default.NullAbility)
}

// A real catalog source remains usable by an old reader after a new version
// is published, including when the reader asks for fresh private projections.
func TestPublishedSchemaSourceMVCCReader(t *testing.T) {
	cc := NewCatalog()
	typ := types.T_int64.ToType()
	wire, err := typ.Marshal()
	require.NoError(t, err)
	publish := func(version uint32, ts int64, name string) {
		item := &TableItem{AccountId: 1, DatabaseId: 2, Name: "t", Id: 12, Version: version, Ts: timestamp.Timestamp{PhysicalTime: ts}}
		InitTableItemWithColumns(item, Columns{{Name: name, Num: 1, Typ: wire}})
		cc.tableChange.Lock()
		cc.tables.data.Set(item)
		cc.tableChange.Unlock()
	}
	lookup := func(ts int64) *TableItem {
		item := &TableItem{AccountId: 1, DatabaseId: 2, Name: "t", Ts: timestamp.Timestamp{PhysicalTime: ts}}
		require.True(t, cc.GetTable(item))
		return item
	}
	publish(1, 10, "old")
	old := lookup(15)
	reader := old.SchemaSource()
	require.True(t, reader.Valid())
	before := reader.PlanTableDef()
	publish(2, 20, "new")
	current := lookup(25)
	require.Equal(t, "new", current.SchemaSource().PlanTableDef().Cols[0].Name)
	require.Equal(t, "old", reader.PlanTableDef().Cols[0].Name)
	require.Equal(t, "old", lookup(15).SchemaSource().PlanTableDef().Cols[0].Name)
	private := reader.PlanTableDef()
	private.Cols[0].Name = "changed"
	require.Equal(t, before, reader.PlanTableDef())
	defs, err := reader.EngineDefs(old)
	require.NoError(t, err)
	var names []string
	for _, def := range defs {
		if a, ok := def.(*engine.AttributeDef); ok {
			names = append(names, a.Attr.Name)
		}
	}
	require.Equal(t, []string{"old"}, names)
}
