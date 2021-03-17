package unittest

import (
	"log"
	"matrixbase/pkg/vm/engine"
	"matrixbase/pkg/vm/engine/memEngine"
	"matrixbase/pkg/vm/process"
)

func segments(proc *process.Process) []engine.Segment {
	e := memEngine.NewTestEngine()
	r, err := e.Relation("test")
	if err != nil {
		log.Fatal(err)
	}
	ids := r.Segments()
	segs := make([]engine.Segment, len(ids))
	for i, id := range ids {
		segs[i] = r.Segment(id, proc)
	}
	return segs
}
