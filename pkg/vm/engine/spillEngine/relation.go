package spillEngine

import (
	"fmt"
	"hash/crc32"
	"matrixone/pkg/compress"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/spillEngine/segment"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/process"
	"path"

	"github.com/pierrec/lz4"
)

func (r *relation) ID() string {
	return r.id
}

func (r *relation) Rows() int64 {
	return r.md.Rows
}

func (r *relation) Segments() []string {
	segs := make([]string, r.md.Segs)
	for i := range segs {
		segs[i] = key(i, r.id)
	}
	return segs
}

func (r *relation) Attribute() []metadata.Attribute {
	return r.md.Attrs
}

func (r *relation) Scheduling(ns metadata.Nodes) []*engine.Unit {
	return nil
}

func (r *relation) Segment(id string, proc *process.Process) engine.Segment {
	return segment.New(id, r.db, proc, r.mp)
}

func (r *relation) Write(bat *batch.Batch) error {
	seg := key(int(r.md.Segs), r.id)
	if n := len(bat.Sels); n > 0 {
		if err := r.db.Set(seg, bat.SelsData[mempool.CountSize:mempool.CountSize+n*8]); err != nil {
			return err
		}
	}
	for i, attr := range bat.Attrs {
		data, err := bat.Vecs[i].Show()
		if err != nil {
			r.clean(seg, bat.Attrs[:i])
			return err
		}
		switch r.mp[attr].Alg {
		case compress.Lz4:
			buf := make([]byte, lz4.CompressBlockBound(len(data)))
			if buf, err = compress.Compress(data, buf, compress.Lz4); err != nil {
				r.clean(seg, bat.Attrs[:i])
				return err
			}
			buf = append(buf, encoding.EncodeInt32(int32(len(data)))...)
			data = buf
		}
		data = append(data, encoding.EncodeUint32(crc32.Checksum(data, crc32.IEEETable))...)
		if err = r.db.Set(seg+"."+attr, data); err != nil {
			r.clean(seg, bat.Attrs[:i])
			return err
		}
	}
	{
		r.md.Segs++
		data, _ := encoding.Encode(r.md)
		if err := r.db.Set(path.Join(r.id, MetaKey), data); err != nil {
			r.clean(seg, bat.Attrs)
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

func (r *relation) clean(seg string, attrs []string) {
	r.db.Del(seg)
	for _, attr := range attrs {
		r.db.Del(seg + "." + attr)
	}
}

func key(num int, id string) string {
	return fmt.Sprintf("%v/%v", id, num)
}
