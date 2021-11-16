package memEngine

import (
	"bytes"
	"matrixone/pkg/compress"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vm/engine"
)

func (r *reader) NewFilter() engine.Filter {
	return nil
}

func (r *reader) NewSummarizer() engine.Summarizer {
	return nil
}

func (r *reader) NewSparseFilter() engine.SparseFilter {
	return nil
}

func (r *reader) Read(cs []uint64, attrs []string) (*batch.Batch, error) {
	if len(r.segs) == 0 {
		return nil, nil
	}
	{
		if len(r.cds) == 0 {
			r.cds = make([]*bytes.Buffer, len(attrs))
			r.dds = make([]*bytes.Buffer, len(attrs))
			for i := range attrs {
				r.cds[i] = bytes.NewBuffer(make([]byte, 0, 1024))
				r.dds[i] = bytes.NewBuffer(make([]byte, 0, 1024))
			}
		}
	}
	bat := batch.New(true, attrs)
	id := r.segs[0]
	r.segs = r.segs[1:]
	for i, attr := range attrs {
		md := r.attrs[attr]
		if md.Alg == compress.None {
			data, err := r.db.Get(id+"."+attr, r.dds[i])
			if err != nil {
				return nil, err
			}
			bat.Vecs[i] = vector.New(md.Type)
			if err := bat.Vecs[i].Read(data); err != nil {
				return nil, err
			}
			bat.Vecs[i].Or = true
			bat.Vecs[i].Ref = cs[i]
		} else {
			data, err := r.db.Get(id+"."+attr, r.cds[i])
			if err != nil {
				return nil, err
			}
			bat.Vecs[i] = vector.New(md.Type)
			n := int(encoding.DecodeInt32(data[len(data)-4:]))
			r.dds[i].Reset()
			if n > r.dds[i].Cap() {
				r.dds[i].Grow(n)
			}
			buf := r.dds[i].Bytes()[:n]
			_, err = compress.Decompress(data[:len(data)-4], buf, int(md.Alg))
			if err != nil {
				return nil, err
			}
			data = buf[:n]
			if err := bat.Vecs[i].Read(data); err != nil {
				return nil, err
			}
			bat.Vecs[i].Or = true
			bat.Vecs[i].Ref = cs[i]
		}
	}
	n := vector.Length(bat.Vecs[0])
	if n > cap(r.zs) {
		r.zs = make([]int64, n*2)
	}
	bat.Zs = r.zs[:n]
	for i := 0; i < n; i++ {
		bat.Zs[i] = 1
	}
	return bat, nil
}
