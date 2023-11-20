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

package sample

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type poolData struct {
	// validBatch stores the valid rows.
	validBatch *batch.Batch

	// invalidBatch stores the invalid rows.
	// in fact, we only store one invalid row.
	invalidBatch *batch.Batch
}

func (pd *poolData) needAppendInvalid() bool {
	return pd.invalidBatch == nil
}

func (pd *poolData) appendValidRow(proc *process.Process, mp *mpool.MPool, bat *batch.Batch, offset int, length int) error {
	if pd.validBatch == nil {
		pd.validBatch = batch.NewWithSize(len(bat.Vecs))
		for i := range pd.validBatch.Vecs {
			pd.validBatch.Vecs[i] = proc.GetVector(*bat.Vecs[i].GetType())
		}
	}

	for i := range pd.validBatch.Vecs {
		if err := pd.validBatch.Vecs[i].UnionBatch(bat.Vecs[i], int64(offset), length, nil, mp); err != nil {
			return err
		}
	}
	pd.validBatch.AddRowCount(length)
	return nil
}

func (pd *poolData) appendInvalidRow(proc *process.Process, mp *mpool.MPool, bat *batch.Batch, row int) error {
	if pd.invalidBatch != nil {
		return nil
	}
	pd.invalidBatch = batch.NewWithSize(len(bat.Vecs))
	for i := range pd.invalidBatch.Vecs {
		pd.invalidBatch.Vecs[i] = proc.GetVector(*bat.Vecs[i].GetType())
	}

	for i := range pd.invalidBatch.Vecs {
		if err := pd.invalidBatch.Vecs[i].UnionBatch(bat.Vecs[i], int64(row), 1, nil, mp); err != nil {
			return err
		}
	}
	pd.invalidBatch.SetRowCount(1)
	return nil
}

func (pd *poolData) output() *batch.Batch {
	if pd.invalidBatch != nil {
		return pd.invalidBatch
	}
	return pd.validBatch
}

func (pd *poolData) clean(mp *mpool.MPool) {
	if pd.validBatch != nil {
		pd.validBatch.Clean(mp)
	}
	if pd.invalidBatch != nil {
		pd.invalidBatch.Clean(mp)
	}
}
