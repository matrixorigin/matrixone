// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package databranchutils

import (
	"context"
	"math"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

// AdvanceLineageSnapshot crosses the current physical timestamp and returns a
// physical-only historical boundary strictly before the new transaction snapshot.
// The workspace owns snapshot advancement: RC deletes must follow rows relocated
// by TN flush/merge before the new snapshot is used. Callers must not rewind only
// the transaction timestamp after the workspace has transferred those deletes.
func AdvanceLineageSnapshot(ctx context.Context, op client.TxnOperator) (int64, error) {
	physical := op.SnapshotTS().PhysicalTime
	if physical > math.MaxInt64-int64(time.Microsecond) {
		return 0, moerr.NewInternalError(ctx, "cannot advance lineage snapshot past the timestamp limit")
	}
	workspace := op.GetWorkspace()
	if workspace == nil {
		return 0, moerr.NewInternalError(ctx, "missing lineage transaction workspace")
	}
	requested := physical + int64(time.Microsecond)
	if err := workspace.AdvanceSnapshot(ctx, timestamp.Timestamp{PhysicalTime: requested}); err != nil {
		return 0, err
	}
	updated := op.SnapshotTS().PhysicalTime
	if updated <= requested {
		return 0, moerr.NewInternalError(ctx, "failed to advance lineage snapshot")
	}
	return updated - int64(time.Nanosecond), nil
}
