// Copyright 2022 Matrix Origin
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

package db_holder

import (
	"context"
	"database/sql"
	"errors"
	"math"

	"github.com/google/uuid"
)

const MetricFileProgressTable = "metric_file_progress"
const metricFileProgressRelation = "system_metrics." + MetricFileProgressTable

const CreateMetricFileProgress = "CREATE TABLE IF NOT EXISTS " + metricFileProgressRelation +
	" (publisher_id varchar(36) not null, first_seq bigint not null, last_seq bigint not null," +
	" revision tinyint not null default 0, primary key (publisher_id, first_seq))"

// MetricFilePublication is allocated once per file write and survives object
// storage retries. SQL-only batches do not consume publication sequences.
type MetricFilePublication struct {
	Publisher uuid.UUID
	Sequence  int64
}

const (
	metricOriginFence = -1
	metricFileFence   = 0
)

type metricSequenceRange struct {
	first int64
	last  int64
}

// CommitMetricFile records the file's completion before its UUID receipt is
// removed. The range and the metric rows share a transaction if replay loads
// the rows. If the original write committed, replay records the range after
// observing its existing receipt instead.
func CommitMetricFile(ctx context.Context, db *sql.DB, id uuid.UUID, publication MetricFilePublication, load func(*sql.Conn) error) error {
	err := recordMetricFile(ctx, db, id, publication, load, nil)
	if isMetricDuplicate(err) {
		err = recordMetricFile(ctx, db, id, publication, nil, err)
	}
	if err != nil {
		return err
	}
	// Keep this outside the transaction that creates the receipt. The unique
	// key must participate in COMMIT even if a competing original writer uses
	// optimistic transactions. Failure here keeps the file for a later retry.
	return withMetricBatchTxn(ctx, db, func(conn *sql.Conn) error {
		// An optimistic first write may still be committing when replay
		// wins the UUID. Fence it before deleting the key that would have
		// conflicted with it. This fence is independent of slow CSV LOADs.
		if err := lockMetricFileProgress(ctx, conn, publication.Publisher.String(), metricOriginFence); err != nil {
			return err
		}
		return execMetricBatch(ctx, conn, deleteMetricReceipt, id.String())
	})
}

func recordMetricFile(
	ctx context.Context, db *sql.DB, id uuid.UUID, publication MetricFilePublication, load func(*sql.Conn) error, duplicate error,
) error {
	publisher, seq := publication.Publisher.String(), publication.Sequence
	return withMetricBatchTxn(ctx, db, func(conn *sql.Conn) error {
		// The permanent prefix row (first_seq = 0) is also a write fence.
		// Pessimistic replays serialize on this row. Optimistic replays
		// conflict at commit if another replay advanced the same publisher.
		// Foreground SQL writers never take this lock.
		if err := lockMetricFileProgress(ctx, conn, publisher, metricFileFence); err != nil {
			return err
		}
		var previous metricSequenceRange
		if err := WithMetricBatchTimeout(ctx, func(opCtx context.Context) error {
			return conn.QueryRowContext(opCtx, "select first_seq, last_seq from "+metricFileProgressRelation+
				" where publisher_id = ? and first_seq <= ? order by first_seq desc limit 1", publisher, seq).
				Scan(&previous.first, &previous.last)
		}); err != nil {
			return err
		}
		if previous.last >= seq {
			return nil
		}

		if duplicate != nil {
			// Recheck after the failed transaction and its locks are gone.
			// A duplicate from a different statement cannot establish delivery.
			var committed bool
			if err := WithMetricBatchTimeout(ctx, func(opCtx context.Context) error {
				return conn.QueryRowContext(opCtx, metricReceiptExists, id.String()).Scan(&committed)
			}); err != nil {
				return errors.Join(duplicate, err)
			}
			if !committed {
				return duplicate
			}
		} else {
			claimed, err := insertMetricReceipt(ctx, conn, id)
			if err != nil {
				return err
			}
			if claimed {
				if err := load(conn); err != nil {
					return err
				}
			}
		}
		return extendMetricFileProgress(ctx, conn, publisher, seq, previous)
	})
}

func lockMetricFileProgress(ctx context.Context, conn *sql.Conn, publisher string, fence int) error {
	return execMetricBatch(ctx, conn, "insert into "+metricFileProgressRelation+
		" (publisher_id, first_seq, last_seq) values (?, ?, 0) on duplicate key update revision = 1-revision", publisher, fence)
}

func extendMetricFileProgress(ctx context.Context, conn *sql.Conn, publisher string, seq int64, previous metricSequenceRange) error {
	last := seq
	if seq < math.MaxInt64 {
		err := WithMetricBatchTimeout(ctx, func(opCtx context.Context) error {
			return conn.QueryRowContext(opCtx, "select last_seq from "+metricFileProgressRelation+
				" where publisher_id = ? and first_seq = ?", publisher, seq+1).Scan(&last)
		})
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil {
			if err := execMetricBatch(ctx, conn, "delete from "+metricFileProgressRelation+
				" where publisher_id = ? and first_seq = ?", publisher, seq+1); err != nil {
				return err
			}
		}
	}
	if previous.last == seq-1 {
		return execMetricBatch(ctx, conn, "update "+metricFileProgressRelation+
			" set last_seq = ? where publisher_id = ? and first_seq = ?", last, publisher, previous.first)
	}
	return execMetricBatch(ctx, conn, "insert into "+metricFileProgressRelation+
		" (publisher_id, first_seq, last_seq) values (?, ?, ?)", publisher, seq, last)
}
