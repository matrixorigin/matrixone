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

func (r *relation) ID() string {
	return r.id
}

func (r *relation) Rows() int64 {
	return r.md.Rows
}

func (r *relation) Segment(id string, proc *process.Process) engine.Segment {
	return segment.New(id, r.db, proc, r.md.Attrs)
}

func (r *relation) Segments() []string {
	segs := make([]string, r.md.Segs)
	for i := range segs {
		segs[i] = sKey(i, r.id)
	}
	return segs
}

func (r *relation) Attribute() []metadata.Attribute {
	return r.md.Attrs
}

func (r *relation) Scheduling(_ metadata.Nodes) []*engine.Unit {
	return nil
}

func (r *relation) Write(bat *batch.Batch) error {
	key := sKey(int(r.md.Segs), r.id)
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
		if err := r.db.Set(r.id, data); err != nil {
			return err
		}
	}
	return nil
}

func (r *relation) AddAttribute(_ metadata.Attribute) error {
	return nil
}

func (r *relation) DelAttribute(_ metadata.Attribute) error {
	return nil
}

func sKey(num int, id string) string {
	return fmt.Sprintf("%v.%v", id, num)
}
