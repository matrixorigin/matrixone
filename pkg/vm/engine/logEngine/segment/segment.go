package segment

import (
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/vm/engine/logEngine/kv"
	"matrixbase/pkg/vm/metadata"
	"matrixbase/pkg/vm/process"
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

func (s *Segment) Read(attrs []string, proc *process.Process) (*batch.Batch, error) {
	bat := batch.New(attrs)
	bat.Is = make([]batch.Info, len(attrs))
	for i, attr := range attrs {
		md := s.mp[attr]
		data, ap, id, err := s.db.Get(s.id+"."+attr, proc.Mp)
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
			Alg: md.Alg,
			Wg:  &batch.WaitGroup{ap, id},
		}
	}
	return bat, nil
}
