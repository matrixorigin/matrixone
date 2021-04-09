package segment

import (
	"matrixone/pkg/compress"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vm/engine/memEngine/kv"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/process"
)

func New(id string, db *kv.KV, proc *process.Process, attrs []metadata.Attribute) *Segment {
	mp := make(map[string]metadata.Attribute)
	for _, attr := range attrs {
		mp[attr.Name] = attr
	}
	return &Segment{id, db, proc, mp}
}

func (s *Segment) ID() string {
	return s.id
}

func (s *Segment) Read(cs []uint64, attrs []string, proc *process.Process) (*batch.Batch, error) {
	bat := batch.New(true, attrs)
	for i, attr := range attrs {
		md := s.mp[attr]
		vec, err := s.read(s.id+"."+attr, md.Alg, md.Type, proc)
		if err != nil {
			for j := 0; j < i; j++ {
				copy(bat.Vecs[j].Data, mempool.OneCount)
				bat.Vecs[j].Free(s.proc)
			}
			return nil, err
		}
		copy(vec.Data, encoding.EncodeUint64(cs[i]))
		bat.Vecs[i] = vec
	}
	return bat, nil
}

func (s *Segment) read(id string, alg int, typ types.Type, proc *process.Process) (*vector.Vector, error) {
	data, err := s.db.Get(id, proc)
	if err != nil {
		return nil, err
	}
	if alg == compress.Lz4 {
		n := int(encoding.DecodeInt32(data[len(data)-4:]))
		buf, err := proc.Alloc(int64(n))
		if err != nil {
			proc.Free(data)
			return nil, err
		}
		if _, err := compress.Decompress(data[mempool.CountSize:len(data)-4], buf[mempool.CountSize:], alg); err != nil {
			proc.Free(data)
			return nil, err
		}
		proc.Free(data)
		data = buf[:mempool.CountSize+n]
	}
	vec := vector.New(typ)
	if err := vec.Read(data); err != nil {
		proc.Free(data)
		return nil, err
	}
	return vec, nil
}
