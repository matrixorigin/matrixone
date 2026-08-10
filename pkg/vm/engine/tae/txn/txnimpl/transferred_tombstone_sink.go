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
	"errors"
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

// A terminal handoff writes only a small metadata marker. Keep its failure
// budget independent from the already exhausted object-delete deadline.
const transferredTombstoneHandoffTimeout = 10 * time.Second

// transferredTombstoneSinker is the part of ioutil.Sinker used while a TN
// transaction rewrites tombstones to the current data-object generation.
// Keeping this boundary explicit makes the publication and cleanup contract
// independently testable.
type transferredTombstoneSinker interface {
	Write(context.Context, *batch.Batch) error
	Sync(context.Context) error
	GetResult() ([]objectio.ObjectStats, []*batch.Batch)
	DeletePersisted(context.Context) ([]string, error)
	Close() error
}

// transferredTombstoneSink owns every object produced by sinker until publish
// registers all returned stats in the transaction workspace. Before that
// boundary, every exit path must delete the unpublished objects. Afterwards,
// transaction commit/rollback owns their lifecycle.
type transferredTombstoneSink struct {
	sinker          transferredTombstoneSinker
	wrote           bool
	published       bool
	cleanupPending  bool
	cleanupFiles    []string
	closed          bool
	cleanupDeadline time.Time
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

		cleanupErr := s.deletePersisted(ctx)
		if cleanupErr != nil {
			// DeletePersisted deliberately retains the exact object references on
			// failure. Do not destroy that retry owner in Close. TransferDeletes
			// hands this sink to txnTable, whose rollback and final Close retry it.
			s.cleanupPending = true
			return priorErr
		}
	}

	closeErr := s.closeSinker()
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

func (s *transferredTombstoneSink) retryCleanup(ctx context.Context) error {
	if !s.cleanupPending {
		return nil
	}
	if err := s.deletePersisted(ctx); err != nil {
		return err
	}
	s.cleanupPending = false
	return s.closeSinker()
}

func (s *transferredTombstoneSink) deletePersisted(ctx context.Context) error {
	// The operation context is commonly cancelled on the error paths for which
	// cleanup is required. Preserve its values but give the sink's complete
	// cleanup lifecycle one total bound, shared by rollback retries.
	cleanupCtx, cancel := s.newCleanupContext(ctx)
	defer cancel()
	files, err := s.sinker.DeletePersisted(cleanupCtx)
	if err == nil {
		s.cleanupFiles = nil
		return nil
	}
	s.cleanupFiles = append(s.cleanupFiles[:0], files...)
	return errors.Join(
		moerr.NewInternalErrorf(
			cleanupCtx,
			"delete %d unpublished transferred tombstone objects",
			len(files),
		),
		err,
	)
}

func (s *transferredTombstoneSink) newCleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if s.cleanupDeadline.IsZero() {
		s.cleanupDeadline = time.Now().Add(transferredTombstoneCleanupTimeout)
	}
	return context.WithDeadlineCause(
		context.WithoutCancel(ctx),
		s.cleanupDeadline,
		moerr.CauseCleanUpUselessFiles,
	)
}

func (s *transferredTombstoneSink) closeSinker() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.sinker.Close()
}

func combineTxnLifecycleErrors(primary, secondary error) error {
	if primary == nil {
		return secondary
	}
	if secondary == nil {
		return primary
	}
	return errors.Join(primary, secondary)
}
