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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
)

var (
	EmptyBatch = &Batch{rowCount: 0}

	EmptyForConstFoldBatch = &Batch{
		Cnt:      1,
		Vecs:     make([]*vector.Vector, 0),
		rowCount: 1,
	}
)

// Batch represents a part of a relationship
// including an optional list of row numbers, columns and list of attributes
//
//	(SelsData, Sels) - list of row numbers
//	(Attrs) - list of attributes
//	(vecs) 	- columns
type Batch struct {
	// For recursive CTE, 1 is last batch, 2 is end of batch
	Recursive int32
	// Ro if true, Attrs is read only
	Ro         bool
	ShuffleIDX int32 //used only in shuffle
	// reference count, default is 1
	Cnt int64
	// Attrs column name list, letter case: origin
	Attrs []string
	// Vecs col data
	Vecs []*vector.Vector

	Aggs []aggexec.AggFuncExec

	// row count of batch, to instead of old len(Zs).
	rowCount int
}
