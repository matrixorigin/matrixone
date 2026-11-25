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

package catalog

import (
	"slices"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/stretchr/testify/require"
)

func TestGenCreateTableTuplePreservesExplicitCreatedTime(t *testing.T) {
	m := mpool.MustNew("created-time")
	packer := types.NewPacker()
	defer packer.Close()
	want := types.Timestamp(123456789)
	bat, err := GenCreateTableTuple(Table{TableId: 10001, CreatedTime: want}, m, packer)
	require.NoError(t, err)
	defer bat.Clean(m)
	require.Equal(t, want,
		vector.MustFixedColNoTypeCheck[types.Timestamp](bat.Vecs[MO_TABLES_CREATED_TIME_IDX])[0])
}

func mustMarshalBatch(bat *batch.Batch) []byte {
	data, err := batch.MarshalVectors(bat.Vecs)
	if err != nil {
		panic(err)
	}
	return data
}

// wireVector is one vector of a batch.MarshalBinary encoding, split into its
// fields so tests can corrupt them one at a time.
type wireVector struct {
	class  byte
	typ    []byte
	length uint32
	data   []byte
	area   []byte
	nulls  []byte
	sorted byte
}

// wireBatch is a batch.MarshalBinary encoding split into its fields.
type wireBatch struct {
	rows int64
	vecs []wireVector
	tail []byte // extra buffer, Recursive and ShuffleIDX
}

type wireCursor struct{ data []byte }

func (c *wireCursor) next(n int) []byte {
	if n > len(c.data) {
		panic("short batch encoding")
	}
	v := slices.Clone(c.data[:n])
	c.data = c.data[n:]
	return v
}

func (c *wireCursor) u32() uint32 { return types.DecodeUint32(c.next(4)) }

func (c *wireCursor) sized() []byte { return c.next(int(c.u32())) }

func decodeWireVector(data []byte) wireVector {
	c := wireCursor{data: data}
	var v wireVector
	v.class = c.next(1)[0]
	v.typ = c.next(types.TSize)
	v.length = c.u32()
	v.data = c.sized()
	v.area = c.sized()
	v.nulls = c.sized()
	v.sorted = c.next(1)[0]
	if len(c.data) != 0 {
		panic("trailing vector encoding")
	}
	return v
}

func decodeWireBatch(data []byte) *wireBatch {
	c := wireCursor{data: data}
	w := &wireBatch{rows: types.DecodeInt64(c.next(8))}
	n := int(c.u32())
	for i := 0; i < n; i++ {
		w.vecs = append(w.vecs, decodeWireVector(c.sized()))
	}
	w.tail = c.data
	return w
}

func (v wireVector) encode() []byte {
	out := append(append([]byte{v.class}, v.typ...), types.EncodeUint32(&v.length)...)
	for _, part := range [][]byte{v.data, v.area, v.nulls} {
		n := uint32(len(part))
		out = append(out, types.EncodeUint32(&n)...)
		out = append(out, part...)
	}
	return append(out, v.sorted)
}

func (w *wireBatch) encode() []byte {
	out := slices.Clone(types.EncodeInt64(&w.rows))
	n := int32(len(w.vecs))
	out = append(out, types.EncodeInt32(&n)...)
	for _, v := range w.vecs {
		enc := v.encode()
		size := uint32(len(enc))
		out = append(out, types.EncodeUint32(&size)...)
		out = append(out, enc...)
	}
	return append(out, w.tail...)
}

// editWire decodes e.Bat, applies edit and re-encodes it.
func editWire(e *api.Entry, edit func(*wireBatch)) {
	w := decodeWireBatch(e.Bat)
	edit(w)
	e.Bat = w.encode()
}

func mustWireVector(vec *vector.Vector) wireVector {
	data, err := vec.MarshalBinary()
	if err != nil {
		panic(err)
	}
	return decodeWireVector(data)
}

// improve test coverage
func TestParseError(t *testing.T) {
	m := mpool.MustNew("test")
	packer := types.NewPacker()
	defer packer.Close()

	{
		bat1, _ := GenCreateTableTuple(Table{TableId: 10001}, m, packer)
		bat2, _ := GenCreateTableTuple(Table{TableId: 10002}, m, packer)
		require.NoError(t, bat1.UnionOne(bat2, 0, m))

		e := &api.Entry{
			EntryType:  api.Entry_Alter,
			DatabaseId: MO_CATALOG_ID,
			TableId:    MO_TABLES_ID,
			Attrs:      slices.Clone(MoTablesSchema),
			Bat:        mustMarshalBatch(bat1),
		}
		_, _, err := ParseEntryList([]*api.Entry{e})
		t.Log(err)
		require.Error(t, err) // bad mo_tables entry type '3'

		e.TableName = "alter"
		_, _, err = ParseEntryList([]*api.Entry{e})
		require.Error(t, err) // bad alter table batch size (2 rows)
	}

	{
		bat2, _ := GenCreateColumnTuples([]Column{
			{
				DatabaseId: 2000,
				TableId:    10001,
				Name:       "col1",
			},
		}, m, packer)

		e2 := &api.Entry{
			EntryType:  api.Entry_Insert,
			DatabaseId: MO_CATALOG_ID,
			TableId:    MO_COLUMNS_ID,
			Attrs:      slices.Clone(MoColumnsSchema),
			Bat:        mustMarshalBatch(bat2),
		}

		_, _, err := ParseEntryList([]*api.Entry{e2})
		require.Error(t, err) // dangling column entry
	}

	{
		bat1, _ := GenCreateTableTuple(Table{TableId: 10001}, m, packer)
		e1 := &api.Entry{
			EntryType:  api.Entry_Insert,
			DatabaseId: MO_CATALOG_ID,
			TableId:    MO_TABLES_ID,
			Attrs:      slices.Clone(MoTablesSchema),
			Bat:        mustMarshalBatch(bat1),
		}
		typ := types.T_int64.ToType()
		typBytes, _ := types.Encode(&typ)
		bat2, _ := GenCreateColumnTuples([]Column{
			{
				TableId: 10001,
				Name:    "col1",
				Typ:     typBytes,
			},
			{
				TableId: 10001,
				Name:    "col2",
				Typ:     typBytes,
			},
		}, m, packer)

		e2 := &api.Entry{
			EntryType:  api.Entry_Insert,
			DatabaseId: MO_CATALOG_ID,
			TableId:    100000, // bad target table id
			Attrs:      slices.Clone(MoColumnsSchema),
			Bat:        mustMarshalBatch(bat2),
		}
		_, _, err := ParseEntryList([]*api.Entry{e1, e2})
		require.Error(t, err) // mismatched column batch

		bat3, _ := GenCreateColumnTuples([]Column{
			{
				TableId: 10002, // bad target table id
				Name:    "col3",
				Typ:     typBytes,
			},
		}, m, packer)
		require.NoError(t, bat2.UnionOne(bat3, 0, m))

		e3 := &api.Entry{
			EntryType:  api.Entry_Insert,
			DatabaseId: MO_CATALOG_ID,
			TableId:    MO_COLUMNS_ID,
			Attrs:      slices.Clone(MoColumnsSchema),
			Bat:        mustMarshalBatch(bat2),
		}
		_, _, err = ParseEntryList([]*api.Entry{e1, e3})
		t.Log(err)
		require.Error(t, err) // mismatched column id
	}
}

func TestShowReqs(t *testing.T) {
	reqs := []any{
		&CreateDatabaseReq{Cmds: []CreateDatabase{{DatabaseId: 10000, Name: "db0"}}},
		&DropDatabaseReq{Cmds: []DropDatabase{{Id: 10000, Name: "db9"}}},
		&CreateTableReq{Cmds: []CreateTable{{TableId: 10001, DatabaseId: 10000, Name: "tbl0"}}},
		&DropTableReq{Cmds: []DropTable{{Id: 10001, DatabaseId: 10000, Name: "tbl9"}}},
		&api.AlterTableReq{DbId: 100, TableId: 101, Kind: api.AlterKind_UpdateComment},
		"write req",
	}
	t.Log(ShowReqs(reqs))
}

func TestParseMoColumnsUpdateEntry(t *testing.T) {
	e := &api.Entry{
		EntryType:  api.Entry_Insert,
		DatabaseId: MO_CATALOG_ID,
		TableId:    MO_COLUMNS_ID,
		TableName:  MO_COLUMNS_UPDATE,
		Bat:        []byte{},
	}

	req, remaining, err := ParseEntryList([]*api.Entry{e})
	require.NoError(t, err)
	require.Same(t, e, req)
	require.Empty(t, remaining)
}

func TestGenRowsGeometry(t *testing.T) {
	mp := mpool.MustNewZero()

	bat := batch.NewWithSize(1)
	vec := vector.NewVec(types.T_geometry.ToType(), mp)
	require.NoError(t, vector.AppendBytesList(vec, [][]byte{
		[]byte("POINT(1 1)"),
		[]byte("POINT(2 2)"),
	}, nil, mp))
	bat.SetVector(0, vec)
	bat.SetRowCount(2)
	defer bat.Clean(mp)

	rows := GenRows(bat)
	require.Equal(t, []byte("POINT(1 1)"), rows[0][0])
	require.Equal(t, []byte("POINT(2 2)"), rows[1][0])
}

func TestParseDatabaseEntryRejectsMalformedTuple(t *testing.T) {
	m := mpool.MustNew("test")
	packer := types.NewPacker()
	defer packer.Close()

	newEntry := func(t *testing.T, kind api.Entry_EntryType) *api.Entry {
		t.Helper()
		var bat *batch.Batch
		var err error
		attrs := DropDatabaseTupleAttrs
		if kind == api.Entry_Insert {
			attrs = MoDatabaseSchema
			bat, err = GenCreateDatabaseTuple("", 0, 0, 0, "catalog_guard_database_long_name", 123, "", m, packer)
		} else {
			bat, err = GenDropDatabaseTuple(types.Rowid{}, 0, 123, "catalog_guard_database_long_name", m, packer)
		}
		require.NoError(t, err)
		defer bat.Clean(m)
		return &api.Entry{
			EntryType:  kind,
			DatabaseId: MO_CATALOG_ID,
			TableId:    MO_DATABASE_ID,
			// Tests mutate the names; never alias the shared schema slices.
			Attrs: slices.Clone(attrs),
			Bat:   mustMarshalBatch(bat),
		}
	}
	// nameIdx is the datname column, a varchar longer than the inline limit.
	nameIdx := func(e *api.Entry) int {
		if e.EntryType == api.Entry_Delete {
			return 3
		}
		return MO_DATABASE_DAT_NAME_IDX
	}
	// replaceVector swaps vector i of e's encoding for a well-formed vec.
	replaceVector := func(e *api.Entry, i int, vec *vector.Vector) {
		defer vec.Free(m)
		editWire(e, func(w *wireBatch) { w.vecs[i] = mustWireVector(vec) })
	}
	nullsShow := func(n *nulls.Nulls) []byte {
		data, err := n.Show()
		if err != nil {
			panic(err)
		}
		return data
	}

	for _, kind := range []api.Entry_EntryType{api.Entry_Insert, api.Entry_Delete} {
		e := newEntry(t, kind)
		req, rest, err := ParseEntryList([]*api.Entry{e})
		require.NoError(t, err)
		require.Empty(t, rest)
		if kind == api.Entry_Insert {
			require.IsType(t, &CreateDatabaseReq{}, req)
		} else {
			require.IsType(t, &DropDatabaseReq{}, req)
		}

		// The wire helpers below round-trip an untouched encoding exactly,
		// so each rejection comes from its mutation alone.
		original := slices.Clone(e.Bat)
		editWire(e, func(*wireBatch) {})
		require.Equal(t, original, e.Bat)

		// Two valid rows parse; "invalid later row" differs only by its null.
		editWire(e, func(w *wireBatch) {
			w.rows = 2
			for i := range w.vecs {
				v := &w.vecs[i]
				v.data = append(v.data, v.data...)
				v.length = 2
			}
		})
		_, _, err = ParseEntryList([]*api.Entry{e})
		require.NoError(t, err)
	}

	for _, tc := range []struct {
		name   string
		mutate func(*api.Entry)
	}{
		{"nil batch", func(e *api.Entry) { e.Bat = nil }},
		{"truncated batch", func(e *api.Entry) { e.Bat = e.Bat[:len(e.Bat)-1] }},
		{"generic DML delete", func(e *api.Entry) {
			editWire(e, func(w *wireBatch) { w.vecs = w.vecs[:2] })
			e.Attrs = e.Attrs[:2]
		}},
		{"extra column", func(e *api.Entry) {
			editWire(e, func(w *wireBatch) { w.vecs = append(w.vecs, w.vecs[0]) })
			e.Attrs = append(e.Attrs, "extra")
		}},
		{"missing vector", func(e *api.Entry) {
			editWire(e, func(w *wireBatch) { w.vecs = w.vecs[:len(w.vecs)-1] })
		}},
		{"missing attr", func(e *api.Entry) { e.Attrs = e.Attrs[:len(e.Attrs)-1] }},
		{"wrong column order", func(e *api.Entry) { e.Attrs[2] = "datname" }},
		{"wrong type", func(e *api.Entry) {
			vec := vector.NewVec(types.T_int64.ToType(), m)
			require.NoError(t, vector.AppendFixed(vec, int64(123), false, m))
			replaceVector(e, 2, vec)
		}},
		{"wrong type, same width", func(e *api.Entry) {
			editWire(e, func(w *wireBatch) {
				typ := types.DecodeType(w.vecs[2].typ)
				if typ.Oid == types.T_uint64 {
					typ.Oid = types.T_int64
				} else {
					typ.Oid = types.T_text
				}
				w.vecs[2].typ = slices.Clone(types.EncodeType(&typ))
			})
		}},
		{"unknown type", func(e *api.Entry) {
			editWire(e, func(w *wireBatch) { w.vecs[2].typ[0] = 255 })
		}},
		{"malformed null bitmap", func(e *api.Entry) {
			editWire(e, func(w *wireBatch) { w.vecs[2].nulls = []byte{1} })
		}},
		{"short data", func(e *api.Entry) {
			editWire(e, func(w *wireBatch) { w.vecs[2].data = nil })
		}},
		{"uneven vectors", func(e *api.Entry) {
			// A well-formed two-row vector in a one-row batch.
			vec := vector.NewVec(types.T_varchar.ToType(), m)
			require.NoError(t, vector.AppendBytes(vec, []byte("a"), false, m))
			require.NoError(t, vector.AppendBytes(vec, []byte("b"), false, m))
			replaceVector(e, 3, vec)
		}},
		{"uneven vector length", func(e *api.Entry) {
			editWire(e, func(w *wireBatch) { w.vecs[3].length = 2 })
		}},
		{"row count mismatch", func(e *api.Entry) {
			editWire(e, func(w *wireBatch) { w.rows = 2 })
		}},
		{"constant vector", func(e *api.Entry) {
			editWire(e, func(w *wireBatch) { w.vecs[2].class = vector.CONSTANT })
		}},
		{"null database id", func(e *api.Entry) {
			editWire(e, func(w *wireBatch) {
				w.vecs[2].nulls = nullsShow(mustNullsOK(nulls.Build(1, bitmapTestMP, 0)))
			})
		}},
		{"invalid later row", func(e *api.Entry) {
			editWire(e, func(w *wireBatch) {
				w.rows = 2
				for i := range w.vecs {
					v := &w.vecs[i]
					v.data = append(v.data, v.data...)
					v.length = 2
				}
				w.vecs[3].nulls = nullsShow(mustNullsOK(nulls.Build(2, bitmapTestMP, 1)))
			})
		}},
		{"invalid varlen area", func(e *api.Entry) {
			idx := nameIdx(e)
			editWire(e, func(w *wireBatch) { w.vecs[idx].area = nil })
		}},
		{"unsupported entry type", func(e *api.Entry) { e.EntryType = api.Entry_Alter }},
	} {
		for _, kind := range []api.Entry_EntryType{api.Entry_Insert, api.Entry_Delete} {
			t.Run(kind.String()+"/"+tc.name, func(t *testing.T) {
				e := newEntry(t, kind)
				tc.mutate(e)
				require.NotPanics(t, func() {
					req, _, err := ParseEntryList([]*api.Entry{e})
					require.Error(t, err)
					require.Nil(t, req)
				})
			})
		}
	}
}
