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
type Batch struct {
	// For recursive CTE, 1 is last batch, 2 is end of batch
	Recursive  int32
	ShuffleIDX int32 //used only in shuffle
	// Attrs column name list, letter case: origin
	Attrs []string
	// Vecs col data
	Vecs []*vector.Vector

	// extraBuf is an opaque payload carried with the batch (today: group's
	// partial aggregate state). It is allocated from mp when mp is non-nil;
	// with a nil mp it borrows memory the batch does not own.
	extraBuf []byte

	// row count of batch, to instead of old len(Zs). For a selected batch it
	// is the logical (selected) row count.
	rowCount int

	// selection, when non-nil, marks the physical rows that are the batch's
	// logical rows (see selection.go). Its length is the vectors' physical
	// row count. It points either at ownSel (the batch owns it) or at a
	// bitmap borrowed from the producer of the batch's input.
	selection *bitmap.Bitmap
	// ownSel is the batch's own selection storage, reused for every batch it
	// carries (OwnSelection) and released by Clean or FreeSelection.
	ownSel bitmap.Bitmap

	// mp is the pool for the batch's own memory allocations; today that is
	// only extraBuf. nil means the batch owns nothing from a pool, as for
	// a view or a batch decoded without a pool.
	mp *mpool.MPool
}
