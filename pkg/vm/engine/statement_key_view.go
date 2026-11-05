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

package engine

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// StatementKeyView holds a statement's prior data while its actions are being
// evaluated. Lookup uses each relation's existing physical primary-key domain;
// the base table preserves physical PK identity. A returned row is caller-owned.
// Validate runs before physical mutations. Close runs after readers and writers
// quiesce, including before statement rollback or retry.
type StatementKeyView interface {
	Lookup(context.Context, uint64, *vector.Vector, int) (*batch.Batch, error)
	Validate(context.Context) error
	Close() error
}

// StatementViewReservation charges retained capture metadata and workspace
// references to the existing query memory budget.
type StatementViewReservation interface{ Release() bool }

type StatementKeyViewProvider interface {
	NewStatementKeyView(context.Context, []Relation, any, *mpool.AllocationAccount,
		func(uint64) (StatementViewReservation, error)) (StatementKeyView, error)
}
