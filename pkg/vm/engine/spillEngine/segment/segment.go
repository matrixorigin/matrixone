package segment

import (
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/spillEngine/block"
	"matrixone/pkg/vm/engine/spillEngine/kv"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/process"
)

func New(id string, db *kv.KV, proc *process.Process, mp map[string]metadata.Attribute) *Segment {
	return &Segment{id, db, proc, mp}
}

func (s *Segment) ID() string {
	return s.id
}

func (s *Segment) Rows() int64 {
	return 0
}

func (s *Segment) Size(attr string) int64 {
	return block.New(s.id, s.db, s.proc, s.mp).Size(attr)
}

func (s *Segment) Blocks() []string {
	return []string{s.id}
}

func (s *Segment) Block(id string, proc *process.Process) engine.Block {
	return block.New(id, s.db, proc, s.mp)
}

func (s *Segment) NewFilter() engine.Filter {
	return nil
}

func (s *Segment) NewSummarizer() engine.Summarizer {
	return nil
}

func (s *Segment) NewSparseFilter() engine.SparseFilter {
	return nil
}
