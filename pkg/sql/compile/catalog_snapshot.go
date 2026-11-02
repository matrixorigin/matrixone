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

package compile

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

type catalogLifecycleSnapshotHooks struct {
	acquireReadBarrier func(context.Context) (timestamp.Timestamp, error)
	currentSnapshot    func() timestamp.Timestamp
	updateSnapshot     func(context.Context, timestamp.Timestamp) error
}

func advanceCatalogLifecycleSnapshot(ctx context.Context, hooks catalogLifecycleSnapshotHooks, subject string) error {
	if hooks.acquireReadBarrier == nil || hooks.currentSnapshot == nil || hooks.updateSnapshot == nil {
		return moerr.NewInternalErrorf(ctx, "incomplete %s snapshot hooks", subject)
	}
	frontier, err := hooks.acquireReadBarrier(ctx)
	if err != nil {
		return err
	}
	if frontier.IsEmpty() {
		return moerr.NewInternalError(ctx, "logtail read barrier returned an empty timestamp")
	}
	if hooks.currentSnapshot().Less(frontier) {
		if err = hooks.updateSnapshot(ctx, frontier); err != nil {
			return err
		}
		if hooks.currentSnapshot().Less(frontier) {
			return moerr.NewInternalErrorf(
				ctx,
				"%s snapshot did not advance after the logtail read barrier", subject,
			)
		}
	}
	return ctx.Err()
}
