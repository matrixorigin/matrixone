// Copyright 2022 Matrix Origin
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

package disttae

import (
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
)

func (r *emptyReader) Close() error {
	return nil
}

func (r *emptyReader) Read(cols []string, expr *plan.Expr, m *mpool.MPool) (*batch.Batch, error) {
	return nil, nil
}

func (r *blockReader) Close() error {
	return nil
}

func (r *blockReader) Read(cols []string, expr *plan.Expr, m *mpool.MPool) (*batch.Batch, error) {
	if len(r.blks) == 0 {
		return nil, nil
	}
	defer func() { r.blks = r.blks[1:] }()
	return blockio.BlockRead(r.ctx, cols, r.tableDef, r.blks[0].Info.MetaLoc,
		r.blks[0].Info.DeltaLoc, r.ts, r.fs, m)
}

func (r *blockMergeReader) Close() error {
	return nil
}

func (r *blockMergeReader) Read(cols []string, expr *plan.Expr, m *mpool.MPool) (*batch.Batch, error) {
	if len(r.blks) == 0 {
		return nil, nil
	}
	defer func() { r.blks = r.blks[1:] }()
	bat, err := blockio.BlockRead(r.ctx, cols, r.tableDef,
		r.blks[0].meta.Info.MetaLoc, r.blks[0].meta.Info.DeltaLoc, r.ts, r.fs, m)
	if err != nil {
		return nil, err
	}
	r.sels = r.sels[:0]
	sort.Ints(r.blks[0].deletes)
	for i := 0; i < bat.Length(); i++ {
		if len(r.blks[0].deletes) > 0 && i == r.blks[0].deletes[0] {
			r.blks[0].deletes = r.blks[0].deletes[1:]
			continue
		}
		r.sels = append(r.sels, int64(i))
	}
	bat.Shrink(r.sels)
	return bat, nil
}

func (r *mergeReader) Close() error {
	return nil
}

func (r *mergeReader) Read(cols []string, expr *plan.Expr, m *mpool.MPool) (*batch.Batch, error) {
	if len(r.rds) == 0 {
		return nil, nil
	}
	for len(r.rds) > 0 {
		bat, err := r.rds[0].Read(cols, expr, m)
		if err != nil {
			for _, rd := range r.rds {
				rd.Close()
			}
			return nil, err
		}
		if bat == nil {
			r.rds = r.rds[1:]
		}
		if bat != nil {
			return bat, nil
		}
	}
	return nil, nil
}
