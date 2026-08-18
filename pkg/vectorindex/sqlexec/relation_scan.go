// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlexec

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// RelationScanRequest is the typed replacement for an internal SELECT issued
// by a vector-index access method.  It is deliberately relational rather than
// algorithm-specific: the caller supplies bound columns, predicates and
// physical top-k parameters; the execution adapter opens readers in the
// current transaction and returns owned batches.
type RelationScanRequest struct {
	Schema       string
	Table        string
	Columns      []string
	Filter       *plan.Expr
	BlockFilters []*plan.Expr
	IndexParam   *plan.IndexReaderParam
	// BatchTransform runs after the exact row filter and before post-filter
	// Top-K compaction. It may mutate the batch in place (for example append a
	// computed distance and apply a distance range).
	BatchTransform func(*batch.Batch) error
	// PostFilterTopOnly prevents storage-level Top-K from running before the
	// exact row filter/BatchTransform while retaining bounded Top-K compaction
	// in the relation scanner.
	PostFilterTopOnly bool
	FilterHint        engine.FilterHint
	PartitionCount    int32
	PartitionIndex    int32
	TxnOffset         int
}

type RelationScanExecutor interface {
	ScanRelation(RelationScanRequest) (executor.Result, error)
}
