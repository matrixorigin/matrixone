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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memtable"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

type PartitionReader struct {
	iter        *memtable.TableIter[RowID, DataValue]
	firstCalled bool
	readTime    memtable.Time
	tx          *memtable.Transaction
	expr        *plan.Expr
}

var _ engine.Reader = new(PartitionReader)

func (p *PartitionReader) Close() error {
	p.iter.Close()
	return nil
}

func (p *PartitionReader) Read(colNames []string, expr *plan.Expr, heap *mheap.Mheap) (*batch.Batch, error) {
	if p == nil {
		return nil, nil
	}

	fn := p.iter.Next
	if !p.firstCalled {
		fn = p.iter.First
		p.firstCalled = true
	}

	b := batch.New(false, colNames)
	for i, name := range colNames {
		_ = name
		b.Vecs[i] = vector.New(types.T_any.ToType()) //TODO get type
	}

	maxRows := 4096
	rows := 0
	for ok := fn(); ok; ok = p.iter.Next() {
		_, dataValue, err := p.iter.Read()
		if err != nil {
			return nil, err
		}

		//TODO handle iter.Expr
		_ = p.expr

		for i, name := range b.Attrs {
			value, ok := dataValue[name]
			if !ok {
				panic(fmt.Sprintf("invalid column name: %v", name))
			}
			value.AppendVector(b.Vecs[i], heap)
		}

		rows++
		if rows == maxRows {
			break
		}
	}

	if rows > 0 {
		b.InitZsOne(rows)
		for _, vec := range b.Vecs {
			nulls.TryExpand(vec.GetNulls(), rows)
		}
	}

	return b, nil
}
