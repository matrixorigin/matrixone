package moengine

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
)

var (
	_ engine.Reader = (*txnReader)(nil)
)

func newReader(rel handle.Relation, it handle.BlockIt) *txnReader {
	attrCnt := len(rel.GetMeta().(*catalog.TableEntry).GetSchema().ColDefs)
	cds := make([]*bytes.Buffer, attrCnt)
	dds := make([]*bytes.Buffer, attrCnt)
	for i := 0; i < attrCnt; i++ {
		cds[i] = bytes.NewBuffer(make([]byte, 1<<20))
		dds[i] = bytes.NewBuffer(make([]byte, 1<<20))
	}
	return &txnReader{
		compressed:   cds,
		decompressed: dds,
		handle:       rel,
		it:           it,
	}
}

func (r *txnReader) Read(refCount []uint64, attrs []string) (*batch.Batch, error) {
	r.it.Lock()
	if !r.it.Valid() {
		r.it.Unlock()
		return nil, nil
	}
	h := r.it.GetBlock()
	r.it.Next()
	r.it.Unlock()
	block := newBlock(h)
	return block.Read(refCount, attrs, r.compressed, r.decompressed)
}

func (r *txnReader) NewFilter() engine.Filter {
	return nil
}

func (r *txnReader) NewSummarizer() engine.Summarizer {
	return nil
}

func (r *txnReader) NewSparseFilter() engine.SparseFilter {
	return nil
}
