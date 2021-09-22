package memEngine

import (
	"fmt"
	"matrixone/pkg/compress"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/memEngine/segment"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/process"

	"github.com/pierrec/lz4"
)

func (r *relation) Close() {}

func (r *relation) ID() string {
	return r.id
}

func (r *relation) Rows() int64 {
	return r.md.Rows
}

func (r *relation) Size(_ string) int64 {
	return 0
}

func (r *relation) Segments() []engine.SegmentInfo {
	segs := make([]engine.SegmentInfo, r.md.Segs)
	for i := range segs {
		segs[i].Node = r.n
		segs[i].Id = r.sKey(i)
	}
	return segs
}

func (r *relation) Segment(si engine.SegmentInfo, _ *process.Process) engine.Segment {
	return segment.New(si.Id, r.db, r.md.Attrs)
}

func (r *relation) Write(_ uint64, bat *batch.Batch) error {
	key := r.sKey(int(r.md.Segs))
	for i, attr := range bat.Attrs {
		v, err := bat.Vecs[i].Show()
		if err != nil {
			return err
		}
		if r.md.Attrs[i].Alg == compress.Lz4 {
			data := make([]byte, lz4.CompressBlockBound(len(v)))
			if data, err = compress.Compress(v, data, compress.Lz4); err != nil {
				return err
			}
			data = append(data, encoding.EncodeInt32(int32(len(v)))...)
			v = data
		}
		if err := r.db.Set(key+"."+attr, v); err != nil {
			return err
		}
	}
	{
		r.md.Segs++
		data, err := encoding.Encode(r.md)
		if err != nil {
			return err
		}
		if err := r.db.Set(r.key(), data); err != nil {
			return err
		}
	}
	return nil
}

func (r *relation) sKey(num int) string {
	return fmt.Sprintf("%v.%v.%v", r.rid, r.id, num)
}

func (r *relation) key() string {
	return fmt.Sprintf("%v.%v", r.rid, r.id)
}

func (r *relation) Index() []*engine.IndexTableDef {
	panic("implement me")
}

func (r *relation) Attribute() []metadata.Attribute {
	defs := make([]metadata.Attribute, len(r.md.Attrs))
	for i, attr := range r.md.Attrs {
		defs[i] = attr
	}
	return defs
}

func (r *relation) AddAttribute(u uint64, def engine.TableDef) error {
	return nil
}

func (r *relation) DelAttribute(u uint64, def engine.TableDef) error {
	return nil
}
