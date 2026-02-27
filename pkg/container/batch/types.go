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

package batch

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// special batch that will never been free.
var (
	EmptyBatch = &Batch{rowCount: 0}

	EmptyForConstFoldBatch = &Batch{
		Vecs:     make([]*vector.Vector, 0),
		rowCount: 1,
	}

	CteEndBatch = &Batch{
		Recursive: 2,
		rowCount:  1,
	}
)

// Batch represents a part of a relationship,
// it's the basic unit for data transmission in the computing layer of MatrixOne.
//
// this includes information such as
// column names (Attrs), column data (Vecs), the number of rows (rowCount),
// aggregation intermediate data (Aggs),
// and some specific flags like Recursive, ShuffleIDX.
//
// offHeap is true means this batch was allocated outside mpool.
type Batch struct {
	// For recursive CTE, 1 is last batch, 2 is end of batch
	Recursive  int32
	ShuffleIDX int32 //used only in shuffle
	// Attrs column name list, letter case: origin
	Attrs []string
	// Vecs col data
	Vecs []*vector.Vector

	ExtraBuf []byte

	// row count of batch, to instead of old len(Zs).
	rowCount int
	offHeap  bool
}
