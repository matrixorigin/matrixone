// Copyright 2026 Matrix Origin
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

package testutil

import (
	"context"
	"errors"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"time"
)

// TaskServiceGetter exposes a service holder without implying storage readiness.
type TaskServiceGetter interface {
	GetTaskService() (taskservice.TaskService, bool)
}

// WaitTaskServiceReady observes actual storage before admitting test workloads.
// Only the owned not-ready sentinel is retried, within the caller's budget.
func WaitTaskServiceReady(
	ctx context.Context,
	getter TaskServiceGetter,
	retryInterval time.Duration,
) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if service, ok := getter.GetTaskService(); ok && service != nil {
			// Holder publication does not imply backing storage availability.
			// Ping also succeeds with no store; a bounded read observes the real
			// boundary and requests refresh without retrying any business write.
			_, err := service.QueryDaemonTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, 0))
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err == nil || !errors.Is(err, taskservice.ErrNotReady) {
				return err
			}
		}
		timer := time.NewTimer(retryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return moerr.AttachCause(ctx, ctx.Err())
		case <-timer.C:
		}
	}
}
