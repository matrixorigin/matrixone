package segment

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memEngine/block"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memEngine/kv"
	"github.com/matrixorigin/matrixone/pkg/vm/metadata"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func New(id string, db *kv.KV, attrs []metadata.Attribute) *Segment {
	mp := make(map[string]metadata.Attribute)
	for _, attr := range attrs {
		mp[attr.Name] = attr
	}
	return &Segment{id, db, mp}
}

func (s *Segment) ID() string {
	return s.id
}

func (s *Segment) Rows() int64 {
	return 0
}

func (s *Segment) Size(_ string) int64 {
	return 0
}

func (s *Segment) Blocks() []string {
	return []string{s.id}
}

func (s *Segment) Block(_ string, _ *process.Process) engine.Block {
	return block.New(s.id, s.db, s.mp)
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
