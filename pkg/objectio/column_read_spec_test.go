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

package objectio_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

func specTestPool(t *testing.T) *mpool.MPool {
	t.Helper()
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})
	return mp
}

func specColumn(seq uint32, typ types.Type, literal *plan.Literal) *plan.ColDef {
	return &plan.ColDef{Seqnum: seq, Typ: typ.PlanType(), Default: &plan.Default{
		HasOriginDefault: true, OriginDefault: literal,
	}}
}

func TestColumnReadSpecTypes(t *testing.T) {
	uuid, err := types.ParseUuid("7264c648-0859-4cc6-ad2c-98527a57b747")
	require.NoError(t, err)
	doc, err := types.ParseStringToByteJson(`{"a":1}`)
	require.NoError(t, err)
	encodedDoc, err := types.EncodeJson(doc)
	require.NoError(t, err)
	largeDecimal, err := types.ParseDecimal256("123.456789", 60, 6)
	require.NoError(t, err)
	textLiteral := func(s string) *plan.Literal {
		return &plan.Literal{Value: &plan.Literal_Sval{Sval: s}}
	}
	arrayLiteral := func(data []byte) *plan.Literal {
		return &plan.Literal{Value: &plan.Literal_VecVal{VecVal: string(data)}}
	}
	tests := []struct {
		typ  types.Type
		lit  *plan.Literal
		want any
	}{
		{types.T_bool.ToType(), &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}, false},
		{types.T_bit.ToType(), &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 2}}, uint64(2)},
		{types.T_int8.ToType(), &plan.Literal{Value: &plan.Literal_I8Val{I8Val: -8}}, int8(-8)},
		{types.T_int16.ToType(), &plan.Literal{Value: &plan.Literal_I16Val{I16Val: -16}}, int16(-16)},
		{types.T_int32.ToType(), &plan.Literal{Value: &plan.Literal_I32Val{I32Val: -32}}, int32(-32)},
		{types.T_int64.ToType(), &plan.Literal{Value: &plan.Literal_I64Val{I64Val: -64}}, int64(-64)},
		{types.T_uint8.ToType(), &plan.Literal{Value: &plan.Literal_U8Val{U8Val: 8}}, uint8(8)},
		{types.T_uint16.ToType(), &plan.Literal{Value: &plan.Literal_U16Val{U16Val: 16}}, uint16(16)},
		{types.T_uint32.ToType(), &plan.Literal{Value: &plan.Literal_U32Val{U32Val: 32}}, uint32(32)},
		{types.T_uint64.ToType(), &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 64}}, uint64(64)},
		{types.T_year.ToType(), &plan.Literal{Value: &plan.Literal_I32Val{I32Val: 2026}}, types.MoYear(2026)},
		{types.T_float32.ToType(), &plan.Literal{Value: &plan.Literal_Fval{Fval: 1.25}}, float32(1.25)},
		{types.T_float64.ToType(), &plan.Literal{Value: &plan.Literal_Dval{Dval: 2.5}}, float64(2.5)},
		{types.New(types.T_decimal64, 10, 2), &plan.Literal{Value: &plan.Literal_Decimal64Val{Decimal64Val: &plan.Decimal64{A: 1234}}}, types.Decimal64(1234)},
		{types.New(types.T_decimal128, 30, 6), &plan.Literal{Value: &plan.Literal_Decimal128Val{Decimal128Val: &plan.Decimal128{A: 123456789}}}, types.Decimal128{B0_63: 123456789}},
		{types.New(types.T_decimal256, 60, 6), textLiteral("123.456789"), largeDecimal},
		{types.T_date.ToType(), &plan.Literal{Value: &plan.Literal_Dateval{Dateval: 800000}}, types.Date(800000)},
		{types.New(types.T_time, 0, 3), &plan.Literal{Value: &plan.Literal_Timeval{Timeval: 1123000}}, types.Time(1123000)},
		{types.New(types.T_datetime, 0, 6), &plan.Literal{Value: &plan.Literal_Datetimeval{Datetimeval: 1234567}}, types.Datetime(1234567)},
		{types.New(types.T_timestamp, 0, 6), &plan.Literal{Value: &plan.Literal_Timestampval{Timestampval: 1234567}}, types.Timestamp(1234567)},
		{types.T_char.ToType(), textLiteral("hello"), []byte("hello")},
		{types.T_varchar.ToType(), textLiteral(strings.Repeat("a", 45)), []byte(strings.Repeat("a", 45))},
		{types.T_binary.ToType(), textLiteral("\x00\x01"), []byte{0, 1}},
		{types.T_varbinary.ToType(), textLiteral("\x00\x01"), []byte{0, 1}},
		{types.T_text.ToType(), textLiteral("text"), []byte("text")},
		{types.T_blob.ToType(), textLiteral("\x00\x02"), []byte{0, 2}},
		{types.T_datalink.ToType(), textLiteral("file:///sample"), []byte("file:///sample")},
		{types.T_geometry.ToType(), textLiteral("\x01\x01\x00\x00\x00" + strings.Repeat("\x00", 16)), append([]byte{1, 1, 0, 0, 0}, make([]byte, 16)...)},
		{types.T_json.ToType(), &plan.Literal{Value: &plan.Literal_Jsonval{Jsonval: `{"a":1}`}}, encodedDoc},
		{types.T_json.ToType(), textLiteral(string(encodedDoc)), encodedDoc},
		{types.T_enum.ToType(), &plan.Literal{Value: &plan.Literal_EnumVal{EnumVal: 2}}, types.Enum(2)},
		{types.T_uuid.ToType(), textLiteral(uuid.String()), uuid},
		{types.T_uuid.ToType(), textLiteral(string(uuid[:])), uuid},
		{types.New(types.T_array_float32, 2, 0), textLiteral("[1,2]"), types.ArrayToBytes([]float32{1, 2})},
		{types.New(types.T_array_float64, 2, 0), arrayLiteral(types.ArrayToBytes([]float64{1, 2})), types.ArrayToBytes([]float64{1, 2})},
		{types.New(types.T_array_float16, 2, 0), textLiteral("[0,0]"), types.ArrayToBytes([]types.Float16{0, 0})},
		{types.New(types.T_array_bf16, 2, 0), textLiteral("[0,0]"), types.ArrayToBytes([]types.BF16{0, 0})},
		{types.New(types.T_array_int8, 2, 0), arrayLiteral([]byte{3, 4}), []byte{3, 4}},
		{types.New(types.T_array_uint8, 2, 0), arrayLiteral([]byte{3, 4}), []byte{3, 4}},
	}

	for _, tc := range tests {
		t.Run(tc.typ.Oid.String(), func(t *testing.T) {
			mp := specTestPool(t)
			// The literal also activates the TableDef path before a caller sets the table bit.
			spec := objectio.NewColumnReadSpecFromTableDef(&plan.TableDef{Cols: []*plan.ColDef{specColumn(2, tc.typ, tc.lit)}})
			require.NotNil(t, spec)
			constant, err := spec.MaterializeMissingVector(2, tc.typ, 3, mp)
			require.NoError(t, err)
			defer constant.Free(mp)
			require.True(t, constant.IsConst())
			require.False(t, constant.IsConstNull())
			require.Equal(t, tc.typ, *constant.GetType())
			require.Equal(t, 3, constant.Length())
			dst := vector.NewVec(tc.typ)
			defer dst.Free(mp)
			require.NoError(t, spec.AppendMissingRows(2, dst, 3, mp))
			require.Equal(t, 3, dst.Length())
			expected := types.EncodeValue(tc.want, tc.typ.Oid)
			raw, isNull, err := spec.MissingDefaultRawBytes(2)
			require.NoError(t, err)
			require.False(t, isNull)
			require.Equal(t, expected, raw)
			for i := range 3 {
				require.False(t, dst.IsNull(uint64(i)))
				require.Equal(t, expected, constant.GetRawBytesAt(i))
				require.Equal(t, expected, dst.GetRawBytesAt(i))
			}
			meta, err := spec.VirtualColumnMeta(2, 3)
			require.NoError(t, err)
			require.Zero(t, meta.NullCnt())
			require.Equal(t, uint32(1), meta.Ndv())
			require.Zero(t, meta.Location().OriginSize())
			zm := meta.ZoneMap()
			require.Equal(t, tc.typ.Scale, zm.GetScale())
			if tc.typ.Oid == types.T_decimal256 || tc.typ.Oid.IsArray() {
				require.False(t, zm.IsInited())
			} else if len(expected) > 30 {
				require.Equal(t, expected[:30], zm.GetMinBuf())
				require.Equal(t, append(slices.Clone(expected[:29]), expected[29]+1), zm.GetMaxBuf())
			} else {
				require.True(t, zm.IsInited())
				require.Equal(t, expected, zm.GetMinBuf())
				require.Equal(t, expected, zm.GetMaxBuf())
			}
		})
	}
}

func TestColumnReadSpecArrayDimensions(t *testing.T) {
	array := types.New(types.T_array_float32, 2, 0)
	packed := types.New(types.T_array_float8, 2, 0)
	ofSizeOne, err := types.AppendBlockScaled(nil, types.BlockScaledMXFP8, []float32{1})
	require.NoError(t, err)
	ofSizeTwo, err := types.AppendBlockScaled(nil, types.BlockScaledMXFP8, []float32{1, 2})
	require.NoError(t, err)
	raw := func(b []byte) *plan.Literal { return &plan.Literal{Value: &plan.Literal_VecVal{VecVal: string(b)}} }
	for _, tc := range []struct {
		name string
		typ  types.Type
		lit  *plan.Literal
		err  string
	}{
		{"short fixed array", array, raw(types.ArrayToBytes([]float32{1})), "dimension 1 does not match column dimension 2"},
		{"unaligned fixed array", array, raw(make([]byte, 7)), "7 bytes for 4-byte elements"},
		{"long text array", array, &plan.Literal{Value: &plan.Literal_Sval{Sval: "[1,2,3]"}}, "dimension 3 does not match column dimension 2"},
		{"short block-scaled array", packed, raw(ofSizeOne), "dimension 1 does not match column dimension 2"},
		{"matching block-scaled array", packed, raw(ofSizeTwo), ""},
		{"unspecified width", types.New(types.T_array_float32, 0, 0), raw(types.ArrayToBytes([]float32{1})), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := objectio.NewColumnReadSpecFromTableDef(&plan.TableDef{Cols: []*plan.ColDef{specColumn(2, tc.typ, tc.lit)}})
			_, _, err := spec.MissingDefaultRawBytes(2)
			if tc.err == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.err)
			}
		})
	}
}

func TestColumnReadSpecEmptyMetadata(t *testing.T) {
	typ := types.T_bool.ToType()
	spec := objectio.NewColumnReadSpecFromTableDef(&plan.TableDef{Cols: []*plan.ColDef{
		specColumn(0, typ, &plan.Literal{Value: &plan.Literal_Bval{Bval: true}}),
		specColumn(2, typ, &plan.Literal{Isnull: true}),
	}})
	checkEmpty := func(col objectio.ColumnMeta, err error) {
		t.Helper()
		require.NoError(t, err)
		require.False(t, col.ZoneMap().IsInited())
		require.Equal(t, types.T_bool, col.ZoneMap().GetType())
		require.Zero(t, col.Ndv())
		require.Zero(t, col.NullCnt())
	}
	meta := objectio.BuildObjectMeta(1)
	meta.BlockHeader().SetMetaColumnCount(1)
	require.Zero(t, meta.BlockCount())
	for _, seq := range []uint16{0, 2} {
		checkEmpty(spec.VirtualColumnMeta(seq, 0))
		checkEmpty(spec.ResolveColumnMeta(meta, seq))
		checkEmpty(spec.ResolveColumnMeta(objectio.ObjectDataMeta(nil), seq))
	}
}

func TestColumnReadSpecResolution(t *testing.T) {
	typ := types.T_bool.ToType()
	withNull := specColumn(2, typ, &plan.Literal{Isnull: true})
	legacy := &plan.ColDef{Seqnum: 3, Typ: typ.PlanType()}
	unsupported := specColumn(4, typ, &plan.Literal{Value: &plan.Literal_Defaultval{Defaultval: true}})
	require.Nil(t, objectio.NewColumnReadSpecFromTableDef(&plan.TableDef{Cols: []*plan.ColDef{legacy}}))
	encoded, err := objectio.NewColumnReadSpecFromEncoded(false, []objectio.RawColumnSpecInput{{Default: []byte{0xff}}})
	require.NoError(t, err)
	require.Nil(t, encoded)

	spec := objectio.NewColumnReadSpecFromTableDef(&plan.TableDef{FeatureFlag: features.SchemaEvolved, Cols: []*plan.ColDef{withNull, legacy, unsupported}})
	require.Equal(t, objectio.ColPhysical, spec.Classify(2, 2, true))
	require.Equal(t, objectio.ColVirtualDefault, spec.Classify(2, 1, false))
	require.Equal(t, objectio.ColCorruptedMissing, spec.Classify(2, 2, false))
	block := objectio.BuildBlockMeta(3)
	header := block.BlockHeader()
	header.SetMaxSeqnum(1)
	header.SetMetaColumnCount(3)
	header.SetRows(3)
	mp := specTestPool(t)
	for _, seq := range []uint16{2, 3} {
		col, err := spec.ResolveColumnMeta(block, seq)
		require.NoError(t, err)
		require.Equal(t, uint32(3), col.NullCnt())
		require.Zero(t, col.Ndv())
		require.False(t, col.ZoneMap().IsInited())
		vec, err := spec.MaterializeMissingVector(seq, typ, 3, mp)
		require.NoError(t, err)
		defer vec.Free(mp)
		require.True(t, vec.IsConstNull())
		raw, isNull, err := spec.MissingDefaultRawBytes(seq)
		require.NoError(t, err)
		require.Nil(t, raw)
		require.True(t, isNull)
	}
	_, _, err = spec.MissingDefaultRawBytes(4)
	require.ErrorContains(t, err, "unsupported origin default")
	_, err = spec.ResolveColumnMeta(block, 4)
	require.ErrorContains(t, err, "unsupported origin default")
	_, err = spec.ResolveColumnMeta(block, 5)
	require.ErrorContains(t, err, "absent from the read schema")
	header.SetMaxSeqnum(2)
	_, err = spec.ResolveColumnMeta(block, 2)
	require.ErrorContains(t, err, "missing within block layout")
	fetcher := spec.WrapColumnMetaFetcher(block)
	_ = fetcher.MustGetColumn(2)
	require.ErrorContains(t, fetcher.Err(), "missing within block layout")
}

func TestColumnReadSpecLocalFS(t *testing.T) {
	ctx := t.Context()
	fs, err := fileservice.NewLocalFS(ctx, "column-read-spec", t.TempDir(), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	mp := specTestPool(t)
	intType, strType := types.T_int64.ToType(), types.T_varchar.ToType()
	arrayType := types.New(types.T_array_float32, 2, 0)
	bat := batch.NewWithSize(2)
	bat.Vecs[0], bat.Vecs[1] = vector.NewVec(intType), vector.NewVec(intType)
	defer bat.Clean(mp)
	for row := range 4 {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(row+1), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(row+10), false, mp))
	}
	bat.SetRowCount(4)
	id := objectio.NewObjectid()
	name := objectio.BuildObjectNameWithObjectID(&id)
	writer, err := objectio.NewObjectWriter(name, fs, 0, []uint16{0, 1}, nil)
	require.NoError(t, err)
	_, err = writer.Write(bat)
	require.NoError(t, err)
	blocks, err := writer.WriteEnd(ctx)
	require.NoError(t, err)
	loc := objectio.BuildLocation(name, blocks[0].GetExtent(), 4, 0)
	meta, err := objectio.FastLoadObjectMeta(ctx, &loc, false, fs)
	require.NoError(t, err)
	dataMeta := meta.MustDataMeta()

	longText := strings.Repeat("frozen ", 12)
	pb := &plan.TableDef{FeatureFlag: features.SchemaEvolved, Cols: []*plan.ColDef{
		{Seqnum: 0, Typ: intType.PlanType()}, {Seqnum: 1, Typ: intType.PlanType()},
		specColumn(2, intType, &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 42}}),
		specColumn(3, strType, &plan.Literal{Value: &plan.Literal_Sval{Sval: longText}}),
		specColumn(4, intType, &plan.Literal{Isnull: true}),
		specColumn(5, arrayType, &plan.Literal{Value: &plan.Literal_Sval{Sval: "[2,0]"}}),
	}}
	spec := objectio.NewColumnReadSpecFromTableDef(pb)
	ctx = objectio.WithColumnReadSpec(ctx, spec)
	cols := []uint16{0, 2, 3, 4}
	typs := []types.Type{intType, intType, strType, intType}
	assertRows := func(t *testing.T, vecs []*vector.Vector, ids []int64) {
		t.Helper()
		for i, id := range ids {
			require.Equal(t, id, vector.GetFixedAtNoTypeCheck[int64](vecs[0], i))
			require.Equal(t, int64(42), vector.GetFixedAtNoTypeCheck[int64](vecs[1], i))
			require.Equal(t, longText, string(vecs[2].GetBytesAt(i)))
			require.True(t, vecs[3].IsNull(uint64(i)))
		}
		for _, vec := range vecs {
			require.Equal(t, len(ids), vec.Length())
		}
	}
	t.Run("ReadOneBlock", func(t *testing.T) {
		before := mp.CurrNB()
		io, err := objectio.ReadOneBlock(ctx, &dataMeta, name.String(), 0, cols, typs, mp, fs, 0)
		require.NoError(t, err)
		defer io.Release()
		require.Equal(t, before, mp.CurrNB(), "serialization keeps no query-pool bytes")
		vecs := make([]*vector.Vector, len(cols))
		for i := range vecs {
			vecs[i] = vector.NewVec(typs[i])
			defer vecs[i].Free(mp)
			require.NoError(t, objectio.MustVectorToCached(vecs[i], io.Entries[i].CachedData))
		}
		require.True(t, vecs[1].IsConst())
		require.True(t, vecs[2].IsConst())
		assertRows(t, vecs, []int64{1, 2, 3, 4})
	})
	t.Run("LoadColumns", func(t *testing.T) {
		cache := containers.NewVectors(len(cols))
		release, _, err := ioutil.LoadColumns(ctx, cols, typs, fs, loc, cache, mp, 0)
		require.NoError(t, err)
		defer release()
		vecs := make([]*vector.Vector, len(cols))
		for i := range vecs {
			vecs[i] = &cache[i]
		}
		assertRows(t, vecs, []int64{1, 2, 3, 4})
	})
	t.Run("LoadColumnsData2 without vector pool", func(t *testing.T) {
		wrapped, release, err := ioutil.LoadColumnsData2(ctx, cols, typs, fs, loc, 0, false, nil)
		require.NoError(t, err)
		defer release()
		vecs := make([]*vector.Vector, len(wrapped))
		for i, vec := range wrapped {
			vecs[i] = vec.GetDownstreamVector()
		}
		assertRows(t, vecs, []int64{1, 2, 3, 4})
	})
	for _, selected := range [][]int64{nil, {3, 1, 1}} {
		t.Run("LoadColumnsDataInto", func(t *testing.T) {
			vecs := make([]*vector.Vector, len(cols))
			for i, typ := range typs {
				vecs[i] = vector.NewVec(typ)
				defer vecs[i].Free(mp)
			}
			mask, _, err := ioutil.LoadColumnsDataInto(ctx, cols, typs, fs, loc, vecs, selected, nil, mp, 0)
			require.NoError(t, err)
			defer mask.Release()
			ids := []int64{1, 2, 3, 4}
			if selected != nil {
				ids = []int64{4, 2, 2}
			}
			assertRows(t, vecs, ids)
		})
	}
	t.Run("TopN", func(t *testing.T) {
		dst := vector.NewVec(strType)
		defer dst.Free(mp)
		top := &objectio.IndexReaderTopOp{Typ: types.T_array_float32, MetricType: metric.Metric_L2sqDistance,
			NumVec: types.ArrayToBytes([]float32{0, 0}), Limit: 2}
		rows, distances, _, err := objectio.ReadBlockBySearchAndTopN(ctx, []uint16{2}, []types.Type{intType}, []uint16{3}, []types.Type{strType}, []*vector.Vector{dst},
			5, arrayType, func(filters []vector.Vector) ([]int64, error) {
				require.Equal(t, 4, filters[0].Length())
				for i := range 4 {
					require.Equal(t, int64(42), vector.GetFixedAtNoTypeCheck[int64](&filters[0], i))
				}
				return []int64{1, 2, 3}, nil
			}, top, fs, loc, mp, 0)
		require.NoError(t, err)
		require.Equal(t, []int64{1, 2}, rows)
		require.Equal(t, []float64{4, 4}, distances)
		require.Equal(t, []string{longText, longText}, []string{dst.GetStringAt(0), dst.GetStringAt(1)})
		tags := slices.Clone(top.NumVec)
		rows, _, _, err = objectio.ReadColumnTopN(ctx, 5, arrayType, fs, loc, nil, &objectio.IndexReaderTopOp{
			Typ: types.T_array_float32, MetricType: metric.Metric_L2sqDistance, NumVec: tags, Limit: 2}, mp, 0)
		require.NoError(t, err)
		require.Equal(t, []int64{0, 1}, rows)
	})
	t.Run("without-spec", func(t *testing.T) {
		ctx := objectio.WithoutColumnReadSpec(ctx)
		require.Nil(t, objectio.ColumnReadSpecFromContext(ctx))
		cache := containers.NewVectors(1)
		_, release, err := ioutil.LoadTombstoneColumns(ctx, []uint16{2}, []types.Type{intType}, fs, loc, cache, mp, 0)
		require.NoError(t, err)
		defer release()
		require.True(t, cache[0].IsConstNull())
	})
	t.Run("ReadDeletes strips the data spec", func(t *testing.T) {
		tombstone := batch.NewWithSize(2)
		tombstone.Vecs[0], tombstone.Vecs[1] = vector.NewVec(objectio.RowidType), vector.NewVec(intType)
		defer tombstone.Clean(mp)
		rowID := types.RandomRowid()
		require.NoError(t, vector.AppendFixed(tombstone.Vecs[0], rowID, false, mp))
		require.NoError(t, vector.AppendFixed(tombstone.Vecs[1], int64(2), false, mp))
		tombstone.SetRowCount(1)
		writer := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_None, fs)
		_, err := writer.WriteBatch(tombstone)
		require.NoError(t, err)
		_, _, err = writer.Sync(ctx)
		require.NoError(t, err)
		stats := writer.GetObjectStats()
		cache := containers.NewVectors(2)
		assertFS := &specAssertingFS{FileService: fs, t: t}
		_, release, err := ioutil.ReadDeletes(ctx, stats.ObjectLocation(), assertFS, true, cache, &intType)
		require.NoError(t, err)
		defer release()
		require.True(t, assertFS.read)
		require.Equal(t, rowID, vector.GetFixedAtNoTypeCheck[types.Rowid](&cache[0], 0))
		require.Equal(t, int64(2), vector.GetFixedAtNoTypeCheck[int64](&cache[1], 0))
	})
}

type specAssertingFS struct {
	fileservice.FileService
	t    *testing.T
	read bool
}

func (fs *specAssertingFS) Read(ctx context.Context, io *fileservice.IOVector) error {
	fs.read = true
	require.Nil(fs.t, objectio.ColumnReadSpecFromContext(ctx))
	return fs.FileService.Read(ctx, io)
}
