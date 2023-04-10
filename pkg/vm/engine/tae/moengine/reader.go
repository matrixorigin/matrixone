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
	"context"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
)

var (
	_ engine.Reader = (*txnReader)(nil)
)

func newReader(rel handle.Relation, it handle.BlockIt) *txnReader {
	return &txnReader{
		handle: rel,
		it:     it,
	}
}

func (r *txnReader) Read(ctx context.Context, attrs []string, _ *plan.Expr, m *mpool.MPool) (*batch.Batch, error) {
	r.it.Lock()
	if !r.it.Valid() {
		r.it.Unlock()
		return nil, nil
	}
	if r.buffer == nil {
		r.buffer = make([]*bytes.Buffer, len(attrs))
		for i := 0; i < len(attrs); i++ {
			r.buffer[i] = new(bytes.Buffer)
		}
	}
	h := r.it.GetBlock()
	r.it.Next()
	r.it.Unlock()
	block := newBlock(h)
	bat, err := block.Read(attrs, nil, r.buffer)
	if err != nil {
		return nil, err
	}
	n := bat.Vecs[0].Length()
	sels := m.GetSels()
	if n > cap(sels) {
		m.PutSels(sels)
		sels = make([]int64, n)
	}
	bat.Zs = sels[:n]
	for i := 0; i < n; i++ {
		bat.Zs[i] = 1
	}
	logutil.Debug(testutil.OperatorCatchBatch("txn reader", bat))
	return bat, nil
}

func (r *txnReader) Close() error {
	return nil
}
