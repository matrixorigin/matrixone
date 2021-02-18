package batch

import (
	"bytes"
	"fmt"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/mempool"
	"matrixbase/pkg/vm/process"
)

func New(attrs []string) *Batch {
	return &Batch{
		Attrs: attrs,
		Vecs:  make([]*vector.Vector, len(attrs)),
	}
}

func (bat *Batch) Free(p *process.Process, mp *mempool.Mempool) {
	for _, vec := range bat.Vecs {
		vec.Free(p, mp)
	}
}

func (bat *Batch) String() string {
	var buf bytes.Buffer

	for i, attr := range bat.Attrs {
		buf.WriteString(fmt.Sprintf("%s\n", attr))
		buf.WriteString(fmt.Sprintf("\t%s\n", bat.Vecs[i]))
	}
	return buf.String()
}
