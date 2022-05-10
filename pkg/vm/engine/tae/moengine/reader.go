// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
