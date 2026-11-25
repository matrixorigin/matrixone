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
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bufferlease"
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
// column data (Vecs), the number of rows (rowCount),
// and some specific flags like Recursive, ShuffleIDX.
//
// A batch carries no column names: names belong to whoever defines the
// columns (the plan, a table definition, a workspace entry), and travel beside
// the batch when an API needs them.
type Batch struct {
	// For recursive CTE, 1 is last batch, 2 is end of batch
	Recursive  int32
	ShuffleIDX int32 //used only in shuffle
	// Vecs col data
	Vecs []*vector.Vector

	ExtraBuf []byte

	// row count of batch, to instead of old len(Zs). For a selected batch it
	// is the logical (selected) row count.
	rowCount int

	// selection, when non-nil, marks the physical rows that are the batch's
	// logical rows (see selection.go); physicalRows is the vectors' row count.
	selection    *bitmap.Bitmap
	physicalRows int

	// extraBufMP owns ExtraBuf only when it was detached from an accounted
	// off-heap buffer. Ordinary Go-heap ExtraBuf values leave this nil.
	extraBufMP *mpool.MPool

	// leases keeps alive the lent backings (for example pinned FileService
	// cache entries) that this batch's vectors alias. The batch is the lender:
	// it releases them whenever it drops or reuses its column data (Clean,
	// CleanOnlyData, FreeColumns, decoding into the batch). A vector moved out of
	// the batch must be copied (MaterializeOwned) if it outlives that point.
	leases bufferlease.Holder
}
