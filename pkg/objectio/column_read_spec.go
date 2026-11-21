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

package objectio

import (
	"context"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type ColResolution uint8

const (
	ColPhysical ColResolution = iota
	ColVirtualDefault
	ColCorruptedMissing
)

type BlockHeaderMeta interface {
	ColumnMetaFetcher
	BlockHeader() BlockHeader
}

// ColumnReadSpec is an immutable snapshot of a table's column layout and
// frozen defaults. A nil spec selects the legacy read behavior.
type ColumnReadSpec struct {
	bySeqnum map[uint16]columnReadEntry
}

type columnReadEntry struct {
	typ   types.Type
	zm    index.ZM
	value readDefault
	err   error
}

type RawColumnSpecInput struct {
	SeqNum  uint16
	Type    types.Type
	Default []byte
}

func NewColumnReadSpecFromTableDef(def *plan.TableDef) *ColumnReadSpec {
	if def == nil {
		return nil
	}
	// A TableDef can carry frozen defaults before its feature bit is set.
	if !features.IsSchemaEvolved(def.FeatureFlag) && !slices.ContainsFunc(def.Cols, func(col *plan.ColDef) bool {
		return col.GetDefault().GetHasOriginDefault()
	}) {
		return nil
	}
	spec := &ColumnReadSpec{bySeqnum: make(map[uint16]columnReadEntry, len(def.Cols))}
	for _, col := range def.Cols {
		if col == nil || col.Seqnum >= SEQNUM_UPPER || col.Typ.Id == int32(types.T_Rowid) {
			continue
		}
		typ, err := types.TypeFromPlan(col.Typ)
		if err != nil {
			spec.bySeqnum[uint16(col.Seqnum)] = columnReadEntry{err: err}
			continue
		}
		spec.bySeqnum[uint16(col.Seqnum)] = newColumnReadEntry(typ, col.Default)
	}
	return spec
}

func NewColumnReadSpecFromEncoded(evolved bool, cols []RawColumnSpecInput) (*ColumnReadSpec, error) {
	if !evolved {
		return nil, nil
	}
	spec := &ColumnReadSpec{bySeqnum: make(map[uint16]columnReadEntry, len(cols))}
	for _, col := range cols {
		var def plan.Default
		if err := def.Unmarshal(col.Default); err != nil {
			return nil, err
		}
		spec.bySeqnum[col.SeqNum] = newColumnReadEntry(col.Type, &def)
	}
	return spec, nil
}

func newColumnReadEntry(typ types.Type, def *plan.Default) columnReadEntry {
	entry := columnReadEntry{typ: typ}
	if def.GetHasOriginDefault() && def.OriginDefault != nil && !def.OriginDefault.Isnull {
		// Defer literal errors until a read actually needs the frozen value.
		entry.value, entry.err = decodeReadDefault(typ, def.OriginDefault)
	}
	entry.zm = index.NewZM(typ.Oid, typ.Scale)
	if entry.err == nil && entry.value.apply != nil {
		index.UpdateZM(entry.zm, entry.value.encoded)
	}
	return entry
}

func (s *ColumnReadSpec) Classify(seqnum, maxSeqnum uint16, present bool) ColResolution {
	if present {
		return ColPhysical
	}
	if s == nil || seqnum > maxSeqnum {
		return ColVirtualDefault
	}
	return ColCorruptedMissing
}

func (s *ColumnReadSpec) missingEntry(seqnum uint16) (columnReadEntry, error) {
	if s == nil {
		return columnReadEntry{}, nil
	}
	entry, ok := s.bySeqnum[seqnum]
	if !ok {
		return entry, moerr.NewInternalErrorNoCtxf("column sequence %d is absent from the read schema", seqnum)
	}
	return entry, entry.err
}

func (s *ColumnReadSpec) MaterializeMissingVector(seqnum uint16, typ types.Type, rows int, mp *mpool.MPool) (*vector.Vector, error) {
	return s.fill(seqnum, typ, nil, rows, mp)
}

func (s *ColumnReadSpec) AppendMissingRows(seqnum uint16, dst *vector.Vector, rows int, mp *mpool.MPool) error {
	_, err := s.fill(seqnum, *dst.GetType(), dst, rows, mp)
	return err
}

// MissingDefaultRawBytes returns the cached read-only bytes of a frozen default.
func (s *ColumnReadSpec) MissingDefaultRawBytes(seqnum uint16) (raw []byte, isNull bool, err error) {
	entry, err := s.missingEntry(seqnum)
	if err != nil {
		return nil, false, err
	}
	return entry.value.encoded, entry.value.apply == nil, nil
}

func (s *ColumnReadSpec) fill(seqnum uint16, typ types.Type, dst *vector.Vector, rows int, mp *mpool.MPool) (*vector.Vector, error) {
	entry, err := s.missingEntry(seqnum)
	if err != nil {
		return nil, err
	}
	if entry.value.apply != nil {
		if entry.typ.Oid != typ.Oid || entry.typ.Scale != typ.Scale {
			return nil, moerr.NewInternalErrorNoCtxf("default type %v does not match read type %v for column %d", entry.typ, typ, seqnum)
		}
		vec, err := entry.value.apply(typ, dst, rows, mp)
		if err != nil && vec != nil {
			vec.Free(mp)
			vec = nil
		}
		return vec, err
	}
	if dst == nil {
		return vector.NewConstNull(typ, rows, mp), nil
	}
	return nil, vector.AppendMultiFixed(dst, byte(0), true, rows, mp)
}

func (s *ColumnReadSpec) VirtualColumnMeta(seqnum uint16, rows uint32) (ColumnMeta, error) {
	entry, err := s.missingEntry(seqnum)
	if err != nil {
		return nil, err
	}
	col := make(ColumnMeta, virtualColumnMetaLen)
	col.setDataType(uint8(entry.typ.Oid))
	if rows == 0 {
		col.SetZoneMap(index.NewZM(entry.typ.Oid, entry.typ.Scale))
	} else {
		col.SetZoneMap(entry.zm)
		if entry.value.apply == nil {
			col.SetNullCnt(rows)
		} else {
			col.SetNdv(1)
		}
	}
	return col, nil
}

// ResolveColumnMeta keeps concrete metadata unboxed on the legacy path.
func ResolveColumnMeta[M BlockHeaderMeta](spec *ColumnReadSpec, meta M, seqnum uint16) (ColumnMeta, error) {
	if spec == nil {
		return meta.MustGetColumn(seqnum), nil
	}
	return spec.ResolveColumnMeta(meta, seqnum)
}

func (s *ColumnReadSpec) ResolveColumnMeta(meta BlockHeaderMeta, seqnum uint16) (ColumnMeta, error) {
	if s == nil || seqnum >= SEQNUM_UPPER {
		return meta.MustGetColumn(seqnum), nil
	}
	data, isObject := meta.(ObjectDataMeta)
	if isObject && (data.IsEmpty() || data.BlockCount() == 0) {
		return s.VirtualColumnMeta(seqnum, 0)
	}
	hdr := meta.BlockHeader()
	var col ColumnMeta
	if seqnum <= hdr.MaxSeqnum() && seqnum < hdr.MetaColumnCount() {
		col = meta.MustGetColumn(seqnum)
	}
	present := len(col) != 0 && col.DataType() != 0
	if !present && len(col) != 0 {
		// Object summaries written by ObjectColumnMetasBuilder omit DataType.
		// All blocks share the object's sequence layout. The block is the
		// authority for physical presence and the summary supplies statistics.
		if isObject {
			block := data.GetBlockMeta(uint32(hdr.StartID()))
			present = seqnum <= block.GetMaxSeqnum() && seqnum < block.GetMetaColumnCount() &&
				block.ColumnMeta(seqnum).DataType() != 0
		}
	}
	switch s.Classify(seqnum, hdr.MaxSeqnum(), present) {
	case ColPhysical:
		return col, nil
	case ColVirtualDefault:
		return s.VirtualColumnMeta(seqnum, hdr.Rows())
	default:
		return nil, moerr.NewInternalErrorNoCtxf("column sequence %d is missing within block layout ending at %d", seqnum, hdr.MaxSeqnum())
	}
}

type EvolvedColumnMetaFetcher struct {
	BlockHeaderMeta
	spec *ColumnReadSpec
	err  error
}

func (s *ColumnReadSpec) WrapColumnMetaFetcher(meta BlockHeaderMeta) *EvolvedColumnMetaFetcher {
	return &EvolvedColumnMetaFetcher{BlockHeaderMeta: meta, spec: s}
}

func (f *EvolvedColumnMetaFetcher) MustGetColumn(seqnum uint16) ColumnMeta {
	if f.err == nil {
		col, err := f.spec.ResolveColumnMeta(f.BlockHeaderMeta, seqnum)
		if err == nil {
			return col
		}
		f.err = err
	}
	return BuildColumnMeta()
}

func (f *EvolvedColumnMetaFetcher) Err() error { return f.err }

type columnReadSpecKey struct{}

func WithColumnReadSpec(ctx context.Context, spec *ColumnReadSpec) context.Context {
	if spec == nil {
		return ctx
	}
	return context.WithValue(ctx, columnReadSpecKey{}, spec)
}

func WithoutColumnReadSpec(ctx context.Context) context.Context {
	if ColumnReadSpecFromContext(ctx) == nil {
		return ctx
	}
	return context.WithValue(ctx, columnReadSpecKey{}, (*ColumnReadSpec)(nil))
}

func ColumnReadSpecFromContext(ctx context.Context) *ColumnReadSpec {
	spec, _ := ctx.Value(columnReadSpecKey{}).(*ColumnReadSpec)
	return spec
}

type readDefault struct {
	encoded []byte
	apply   func(types.Type, *vector.Vector, int, *mpool.MPool) (*vector.Vector, error)
}

func fixedReadDefault[T types.FixedSizeT](v T) readDefault {
	return readDefault{types.EncodeFixed(v), func(typ types.Type, dst *vector.Vector, rows int, mp *mpool.MPool) (*vector.Vector, error) {
		if dst == nil {
			return vector.NewConstFixed(typ, v, rows, mp)
		}
		return nil, vector.AppendMultiFixed(dst, v, false, rows, mp)
	}}
}

func bytesReadDefault(v []byte) readDefault {
	return readDefault{v, func(typ types.Type, dst *vector.Vector, rows int, mp *mpool.MPool) (*vector.Vector, error) {
		if dst == nil {
			return vector.NewConstBytes(typ, v, rows, mp)
		}
		return nil, vector.AppendMultiBytes(dst, v, false, rows, mp)
	}}
}

func decodeReadDefault(typ types.Type, lit *plan.Literal) (readDefault, error) {
	oid := typ.Oid
	switch v := lit.Value.(type) {
	case *plan.Literal_Bval:
		if oid == types.T_bool {
			return fixedReadDefault(v.Bval), nil
		}
	case *plan.Literal_I8Val:
		if oid == types.T_int8 {
			return fixedReadDefault(int8(v.I8Val)), nil
		}
	case *plan.Literal_I16Val:
		if oid == types.T_int16 {
			return fixedReadDefault(int16(v.I16Val)), nil
		}
	case *plan.Literal_I32Val:
		if oid == types.T_int32 {
			return fixedReadDefault(v.I32Val), nil
		}
		if oid == types.T_year {
			return fixedReadDefault(types.MoYear(v.I32Val)), nil
		}
	case *plan.Literal_I64Val:
		if oid == types.T_int64 {
			return fixedReadDefault(v.I64Val), nil
		}
	case *plan.Literal_U8Val:
		if oid == types.T_uint8 {
			return fixedReadDefault(uint8(v.U8Val)), nil
		}
	case *plan.Literal_U16Val:
		if oid == types.T_uint16 {
			return fixedReadDefault(uint16(v.U16Val)), nil
		}
	case *plan.Literal_U32Val:
		if oid == types.T_uint32 {
			return fixedReadDefault(v.U32Val), nil
		}
	case *plan.Literal_U64Val:
		if oid == types.T_uint64 || oid == types.T_bit {
			return fixedReadDefault(v.U64Val), nil
		}
	case *plan.Literal_Fval:
		if oid == types.T_float32 {
			return fixedReadDefault(v.Fval), nil
		}
	case *plan.Literal_Dval:
		if oid == types.T_float64 {
			return fixedReadDefault(v.Dval), nil
		}
	case *plan.Literal_Dateval:
		if oid == types.T_date {
			return fixedReadDefault(types.Date(v.Dateval)), nil
		}
	case *plan.Literal_Timeval:
		if oid == types.T_time {
			return fixedReadDefault(types.Time(v.Timeval)), nil
		}
	case *plan.Literal_Datetimeval:
		if oid == types.T_datetime {
			return fixedReadDefault(types.Datetime(v.Datetimeval)), nil
		}
	case *plan.Literal_Timestampval:
		if oid == types.T_timestamp {
			return fixedReadDefault(types.Timestamp(v.Timestampval)), nil
		}
	case *plan.Literal_Decimal64Val:
		if oid == types.T_decimal64 && v.Decimal64Val != nil {
			return fixedReadDefault(types.Decimal64(v.Decimal64Val.A)), nil
		}
	case *plan.Literal_Decimal128Val:
		if oid == types.T_decimal128 && v.Decimal128Val != nil {
			return fixedReadDefault(types.Decimal128{B0_63: uint64(v.Decimal128Val.A), B64_127: uint64(v.Decimal128Val.B)}), nil
		}
	case *plan.Literal_EnumVal:
		if oid == types.T_enum {
			return fixedReadDefault(types.Enum(v.EnumVal)), nil
		}
	case *plan.Literal_Sval:
		return decodeStringReadDefault(typ, v.Sval)
	case *plan.Literal_Jsonval:
		if oid == types.T_json {
			bj, err := types.ParseStringToByteJson(v.Jsonval)
			if err != nil {
				return readDefault{}, err
			}
			data, err := types.EncodeJson(bj)
			return bytesReadDefault(data), err
		}
	case *plan.Literal_VecVal:
		if oid.IsArrayRelate() {
			return fixedArrayReadDefault(typ, []byte(v.VecVal))
		}
		if format, ok := oid.BlockScaledFormat(); ok {
			data := []byte(v.VecVal)
			cell, err := types.ParseBlockScaledCell(data)
			if err != nil {
				return readDefault{}, err
			}
			if cell.Format == format {
				if err := checkReadDefaultArrayDimension(typ, int64(cell.Dim)); err != nil {
					return readDefault{}, err
				}
				return bytesReadDefault(data), nil
			}
		}
	}
	// Scalar bf16, float16, float8 and float4 await plan.Literal support.
	return readDefault{}, moerr.NewInternalErrorNoCtxf("unsupported origin default %T for %s", lit.Value, typ)
}

func checkReadDefaultArrayDimension(typ types.Type, dim int64) error {
	if typ.Width > 0 && dim != int64(typ.Width) {
		return moerr.NewInternalErrorNoCtxf("array origin default dimension %d does not match column dimension %d", dim, typ.Width)
	}
	return nil
}

func fixedArrayReadDefault(typ types.Type, data []byte) (readDefault, error) {
	size := typ.GetArrayElementSize()
	if len(data)%size != 0 {
		return readDefault{}, moerr.NewInternalErrorNoCtxf("array origin default has %d bytes for %d-byte elements", len(data), size)
	}
	if err := checkReadDefaultArrayDimension(typ, int64(len(data)/size)); err != nil {
		return readDefault{}, err
	}
	return bytesReadDefault(data), nil
}

func decodeStringReadDefault(typ types.Type, s string) (readDefault, error) {
	var data []byte
	var err error
	switch typ.Oid {
	case types.T_decimal256:
		v, err := types.ParseDecimal256(s, typ.Width, typ.Scale)
		return fixedReadDefault(v), err
	case types.T_uuid:
		var v types.Uuid
		if len(s) == len(v) {
			copy(v[:], s)
		} else {
			v, err = types.ParseUuid(s)
		}
		return fixedReadDefault(v), err
	case types.T_json:
		data = []byte(s) // folded JSON constants carry the stored binary value
		if len(data) == 0 {
			return readDefault{}, moerr.NewInternalErrorNoCtx("empty JSON origin default")
		}
		err = bytejson.ValidateStoredJSONDocument(types.DecodeJson(data))
	case types.T_array_float32:
		data, err = types.StringToArrayToBytes[float32](s)
	case types.T_array_float64:
		data, err = types.StringToArrayToBytes[float64](s)
	case types.T_array_float16:
		data, err = types.StringToArrayToBytes[types.Float16](s)
	case types.T_array_bf16:
		data, err = types.StringToArrayToBytes[types.BF16](s)
	case types.T_array_int8:
		data, err = types.StringToArrayToBytes[int8](s)
	case types.T_array_uint8:
		data, err = types.StringToArrayToBytes[uint8](s)
	case types.T_array_float8, types.T_array_float4:
		return readDefault{}, moerr.NewInternalErrorNoCtx("block-scaled array origin default requires a stored cell")
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_text, types.T_blob,
		types.T_datalink, types.T_geometry, types.T_geometry32:
		data = []byte(s)
	default:
		return readDefault{}, moerr.NewInternalErrorNoCtxf("unsupported string origin default for %s", typ)
	}
	if err != nil {
		return readDefault{}, err
	}
	if typ.Oid.IsArrayRelate() {
		return fixedArrayReadDefault(typ, data)
	}
	return bytesReadDefault(data), nil
}
