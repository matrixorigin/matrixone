// Copyright 2021 Matrix Origin
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

package txnimpl

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"go.uber.org/zap"
)

// Transferred tombstones have not crossed the transaction publication
// boundary yet, so cleanup must outlive request cancellation. It must not,
// however, make a failed commit worker wait indefinitely for degraded object
// storage. DeletePersisted passes this bound through to every delete batch.
const transferredTombstoneCleanupTimeout = time.Minute

// transferredTombstoneSinker is the part of ioutil.Sinker used while a TN
// transaction rewrites tombstones to the current data-object generation.
// Keeping this boundary explicit makes the publication and cleanup contract
// independently testable.
type transferredTombstoneSinker interface {
	Write(context.Context, *batch.Batch) error
	Sync(context.Context) error
	GetResult() ([]objectio.ObjectStats, []*batch.Batch)
	DeletePersisted(context.Context) (int, error)
	Close() error
}

// transferredTombstoneSink owns every object produced by sinker until publish
// registers all returned stats in the transaction workspace. Before that
// boundary, every exit path must delete the unpublished objects. Afterwards,
// transaction commit/rollback owns their lifecycle.
type transferredTombstoneSink struct {
	sinker    transferredTombstoneSinker
	wrote     bool
	published bool
}

func (s *transferredTombstoneSink) write(ctx context.Context, bat *batch.Batch) error {
	if s.published {
		return moerr.NewInternalError(ctx, "write transferred tombstones after publication")
	}
	if err := s.sinker.Write(ctx, bat); err != nil {
		return err
	}
	s.wrote = true
	return nil
}

func (s *transferredTombstoneSink) publish(
	ctx context.Context,
	register func(...objectio.ObjectStats),
) error {
	if s.published {
		return moerr.NewInternalError(ctx, "publish transferred tombstones more than once")
	}
	if !s.wrote {
		return moerr.NewInternalError(ctx, "publish transferred tombstones before write")
	}
	if err := s.sinker.Sync(ctx); err != nil {
		return err
	}
	stats, tail := s.sinker.GetResult()
	if len(tail) != 0 {
		return moerr.NewInternalErrorf(
			ctx,
			"transferred tombstone sink returned %d in-memory batches",
			len(tail),
		)
	}
	if len(stats) == 0 {
		return moerr.NewInternalError(ctx, "transferred tombstone sink returned no objects")
	}
	register(stats...)
	s.published = true
	return nil
}

func (s *transferredTombstoneSink) close(ctx context.Context, priorErr error) error {
	if !s.published {
		if priorErr == nil {
			priorErr = moerr.NewInternalError(ctx, "transferred tombstones closed before publication")
		}

		// The operation context is commonly cancelled on the error paths for
		// which cleanup is required. Preserve its values but give cleanup an
		// independent total bound. DeletePersisted's per-request timeout is
		// consequently capped by this earlier deadline too.
		cleanupCtx, cancel := context.WithTimeoutCause(
			context.WithoutCancel(ctx),
			transferredTombstoneCleanupTimeout,
			moerr.CauseCleanUpUselessFiles,
		)
		count, cleanupErr := s.sinker.DeletePersisted(cleanupCtx)
		cancel()
		if cleanupErr != nil {
			logutil.Error(
				"failed to delete unpublished transferred tombstone objects",
				zap.Int("object-count", count),
				zap.NamedError("operation-error", priorErr),
				zap.NamedError("cleanup-error", cleanupErr),
			)
		}
	}

	closeErr := s.sinker.Close()
	if priorErr != nil {
		// The transaction error is the primary outcome. Returning a joined or
		// wrapped error here would hide its concrete *moerr.Error from the TN
		// RPC encoder and change retryable conflicts into internal errors.
		if closeErr != nil {
			logutil.Error(
				"failed to close transferred tombstone sink",
				zap.NamedError("operation-error", priorErr),
				zap.NamedError("close-error", closeErr),
			)
		}
		return priorErr
	}
	return closeErr
}
