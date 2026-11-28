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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
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

// ColumnReadSpec is the immutable column layout of an evolved table.
// In v1 an eligible historical missing column always reads as typed NULL.
// A nil spec selects the legacy read behavior.
type ColumnReadSpec struct {
	bySeqnum map[uint16]columnReadEntry
}

type columnReadEntry struct {
	typ types.Type
	err error
}

type ColumnReadSpecInput struct {
	SeqNum uint16
	Type   types.Type
}

func NewColumnReadSpecFromTableDef(def *plan.TableDef) *ColumnReadSpec {
	if def == nil || !features.IsSchemaEvolved(def.FeatureFlag) {
		return nil
	}
	spec := &ColumnReadSpec{bySeqnum: make(map[uint16]columnReadEntry, len(def.Cols))}
	for _, col := range def.Cols {
		if col == nil || col.Seqnum >= SEQNUM_UPPER || col.Typ.Id == int32(types.T_Rowid) {
			continue
		}
		typ, err := types.TypeFromPlan(col.Typ)
		spec.bySeqnum[uint16(col.Seqnum)] = columnReadEntry{typ: typ, err: err}
	}
	return spec
}

// NewColumnReadSpec builds the spec for an already activated schema.
func NewColumnReadSpec(cols []ColumnReadSpecInput) *ColumnReadSpec {
	spec := &ColumnReadSpec{bySeqnum: make(map[uint16]columnReadEntry, len(cols))}
	for _, col := range cols {
		spec.bySeqnum[col.SeqNum] = columnReadEntry{typ: col.Type}
	}
	return spec
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

// ColumnType returns the read schema's type for a projected sequence.
func (s *ColumnReadSpec) ColumnType(seqnum uint16) (types.Type, error) {
	entry, err := s.missingEntry(seqnum)
	return entry.typ, err
}

// ValidateMissingColumn checks schema membership after the caller establishes
// that the column is absent from an older layout.
func (s *ColumnReadSpec) ValidateMissingColumn(seqnum uint16) error {
	_, err := s.missingEntry(seqnum)
	return err
}

func (s *ColumnReadSpec) MaterializeMissingVector(seqnum uint16, typ types.Type, rows int, mp *mpool.MPool) (*vector.Vector, error) {
	if err := s.ValidateMissingColumn(seqnum); err != nil {
		return nil, err
	}
	return vector.NewConstNull(typ, rows, mp), nil
}

func (s *ColumnReadSpec) AppendMissingRows(seqnum uint16, dst *vector.Vector, rows int, mp *mpool.MPool) error {
	if err := s.ValidateMissingColumn(seqnum); err != nil {
		return err
	}
	return vector.AppendMultiFixed(dst, byte(0), true, rows, mp)
}

func (s *ColumnReadSpec) VirtualColumnMeta(seqnum uint16, rows uint32) (ColumnMeta, error) {
	entry, err := s.missingEntry(seqnum)
	if err != nil {
		return nil, err
	}
	col := make(ColumnMeta, virtualColumnMetaLen)
	col.setDataType(uint8(entry.typ.Oid))
	col.SetZoneMap(index.NewZM(entry.typ.Oid, entry.typ.Scale))
	col.SetNullCnt(rows)
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
