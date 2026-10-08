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

package disttae

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"go.uber.org/zap"
)

const workspaceSpillCleanupTimeout = 30 * time.Second
const workspaceSpillCleanupRetryInterval = time.Minute

// A spill object belongs to this owner only after its workspace publication
// has failed. Successful spills transfer ownership to the transaction instead.
type workspaceSpillCleanupTarget struct {
	name string
	fs   fileservice.FileService
}

// workspaceSpillCleanupOwner retains exact unpublished object names across a
// failed cleanup. It is engine-owned because the failed transaction may be
// rolled back and destroyed before the object store recovers.
type workspaceSpillCleanupOwner struct {
	sync.Mutex
	pending map[string]workspaceSpillCleanupTarget
	running bool
}

func (e *Engine) cleanupUnpublishedWorkspaceSpill(
	ctx context.Context, targets []workspaceSpillCleanupTarget,
) error {
	if len(targets) == 0 {
		return nil
	}
	owner := &e.workspaceSpillCleanup
	owner.Lock()
	if owner.pending == nil {
		owner.pending = make(map[string]workspaceSpillCleanupTarget)
	}
	for _, target := range targets {
		if target.name != "" {
			owner.pending[target.name] = target
		}
	}
	owner.Unlock()

	cleanupCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), workspaceSpillCleanupTimeout)
	err := owner.retry(cleanupCtx)
	cancel()
	if err != nil {
		owner.startRetry()
	}
	return err
}

// retry holds the owner lock across deletion so that a concurrent attempt
// cannot release or overwrite a name while an earlier deletion is in flight.
// No transaction/workspace lock is held by the caller during this I/O.
func (owner *workspaceSpillCleanupOwner) retry(ctx context.Context) error {
	owner.Lock()
	defer owner.Unlock()
	var retryErr error
	for name, target := range owner.pending {
		if _, err := ioutil.DeleteUnpublishedObjects(ctx, target.fs, name); err != nil {
			retryErr = errors.Join(retryErr, err)
			continue
		}
		delete(owner.pending, name)
	}
	return retryErr
}

func (owner *workspaceSpillCleanupOwner) startRetry() {
	owner.Lock()
	if owner.running || len(owner.pending) == 0 {
		owner.Unlock()
		return
	}
	owner.running = true
	owner.Unlock()
	go func() {
		ticker := time.NewTicker(workspaceSpillCleanupRetryInterval)
		defer ticker.Stop()
		for range ticker.C {
			ctx, cancel := context.WithTimeout(
				context.Background(), workspaceSpillCleanupTimeout)
			err := owner.retry(ctx)
			cancel()
			if err != nil {
				logutil.Warn("unpublished workspace spill cleanup remains pending",
					zap.Error(err))
			}
			owner.Lock()
			if len(owner.pending) == 0 {
				owner.running = false
				owner.Unlock()
				return
			}
			owner.Unlock()
		}
	}()
}
