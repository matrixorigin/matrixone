package logEngine

import (
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/metadata"
)

func (r *Relation) ID() string {
	return r.id
}

func (r *Relation) Rows() int64 {
	return r.md.Rows
}

func (r *Relation) Segments() []string {
	segs := make([]string, r.md.Rows/r.md.Limit)
	for i := range segs {
		segs[i] = bKey(i, r.md.Version, r.id)
	}
	if r.md.Rows%r.md.Limit > 0 {
		segs = append(segs, pKey(len(segs), r.id, r.md.Part-1, r.md.Version))
	}
	return segs
}

func (r *Relation) Attribute() []metadata.Attribute {
	return r.md.Attrs
}

func (r *Relation) Scheduling(ns metadata.Nodes) []*engine.Unit {
	n := len(ns)
	if n == 0 {
		return nil
	}
	segs := r.Segments()
	mp := make(map[string]*engine.Unit)
	us := make([]*engine.Unit, 0, len(segs))
	for i, seg := range segs {
		node := ns[i%n]
		if u, ok := mp[node.Id]; ok {
			u.Segs = append(u.Segs, seg)
		} else {
			u := &engine.Unit{N: node, Segs: []string{seg}}
			mp[node.Id] = u
			us = append(us, u)
		}
	}
	return us
}

func (r *Relation) Write(_ *batch.Batch) error {
	return nil
}

func (r *Relation) AddAttribute(_ metadata.Attribute) error {
	return nil
}

func (r *Relation) DelAttribute(_ metadata.Attribute) error {
	return nil
}

func bKey(num int, version int64, id string) string {
	return fmt.Sprintf("%v.%v.%v", id, version, num)
}

func pKey(num int, id string, part int32, version int64) string {
	return fmt.Sprintf("%v.%v.%v.part%v", id, version, num, part)
}
