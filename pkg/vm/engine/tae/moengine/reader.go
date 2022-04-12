package moengine

import (
	"bytes"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
)

var (
	_ engine.Reader = (*txnReader)(nil)
)

func newReaderIt(rel handle.Relation) *txnReaderIt {
	segmentIt := rel.MakeSegmentIt()
	if !segmentIt.Valid() {
		return new(txnReaderIt)
	}
	seg := segmentIt.GetSegment()
	blockIt := seg.MakeBlockIt()
	return &txnReaderIt{
		RWMutex:   new(sync.RWMutex),
		blockIt:   blockIt,
		segmentIt: segmentIt,
	}
}

func newReader(rel handle.Relation, it *txnReaderIt) *txnReader {
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

func (it *txnReaderIt) Valid() bool {
	if it.segmentIt == nil || !it.segmentIt.Valid() {
		return false
	}
	if !it.blockIt.Valid() {
		it.segmentIt.Next()
		if !it.segmentIt.Valid() {
			return false
		}
		seg := it.segmentIt.GetSegment()
		it.blockIt = seg.MakeBlockIt()
		return it.blockIt.Valid()
	}
	return true
}

func (it *txnReaderIt) Next() {
	it.blockIt.Next()
	if it.blockIt.Valid() {
		return
	}
	it.segmentIt.Next()
	if !it.segmentIt.Valid() {
		return
	}
	seg := it.segmentIt.GetSegment()
	it.blockIt = seg.MakeBlockIt()
}

func (it *txnReaderIt) Get() handle.Block {
	return it.blockIt.GetBlock()
}

func (r *txnReader) Read(refCount []uint64, attrs []string) (*batch.Batch, error) {
	r.it.Lock()
	if !r.it.Valid() {
		r.it.Unlock()
		return nil, nil
	}
	h := r.it.Get()
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
