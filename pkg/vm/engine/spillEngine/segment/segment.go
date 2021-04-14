package segment

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vm/engine/spillEngine/kv"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/process"
)

func New(id string, db *kv.KV, proc *process.Process, mp map[string]metadata.Attribute) *Segment {
	return &Segment{id, db, proc, mp}
}

func (s *Segment) ID() string {
	return s.id
}

func (s *Segment) Read(cs []uint64, attrs []string, proc *process.Process) (*batch.Batch, error) {
	bat := batch.New(true, attrs)
	bat.Is = make([]batch.Info, len(attrs))
	{
		data, err := s.db.GetCopy(s.id)
		if err != nil {
			return nil, err
		}
		if data != nil {
			buf, err := proc.Alloc(int64(len(data)))
			if err != nil {
				return nil, err
			}
			copy(buf[mempool.CountSize:], data)
			bat.SelsData = buf
			bat.Sels = encoding.DecodeInt64Slice(buf[mempool.CountSize : mempool.CountSize+len(data)])
		}
	}
	for i, attr := range attrs {
		md := s.mp[attr]
		data, ap, id, err := s.db.Get(s.id+"."+attr, proc)
		if err != nil {
			for j := 0; j < i; j++ {
				bat.Is[i].Wg.Wait()
				bat.Vecs[j].Free(s.proc)
			}
			return nil, err
		}
		vec := vector.New(md.Type)
		vec.Data = data
		bat.Vecs[i] = vec
		bat.Is[i] = batch.Info{
			Ref: cs[i],
			Alg: md.Alg,
			Wg:  &batch.WaitGroup{ap, id},
		}
	}
	return bat, nil
}
