package memEngine

import (
	"fmt"
	"matrixbase/pkg/compress"
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/encoding"
	"matrixbase/pkg/mempool"
	"matrixbase/pkg/vm/engine"
	"matrixbase/pkg/vm/engine/memEngine/segment"
	"matrixbase/pkg/vm/metadata"
	"matrixbase/pkg/vm/op"
	"matrixbase/pkg/vm/process"

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

func (r *relation) Read(o op.OP, mp *mempool.Mempool) (*batch.Batch, error) {
	var attrs []string
	{
		as := o.Attributes()
		attrs = make([]string, len(as))
		for i, a := range as {
			attrs[i] = a.Name
		}
	}
	return o.Read(attrs, mp)
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
