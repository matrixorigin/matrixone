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

package sidecarflight

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
)

// Run claims the ticket exactly once and pulls one FlightData message at a
// time. The next message is not requested until fill returns and the decoded
// MO batch has been released, so the existing MySQL writer supplies the
// transport's backpressure.
func (e *Execution) Run(
	ctx context.Context,
	mp *mpool.MPool,
	counters *perfcounter.CounterSet,
	fill func(*batch.Batch, *perfcounter.CounterSet) error,
) error {
	if e == nil || e.runtime == nil || mp == nil || fill == nil {
		return fmt.Errorf("sidecar flight: invalid execution")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	e.mu.Lock()
	if e.started || e.terminal {
		e.mu.Unlock()
		return fmt.Errorf("sidecar flight: ticket was already claimed or completed")
	}
	e.started = true
	streamCtx, cancel := context.WithCancel(ctx)
	e.streamCancel = cancel
	e.mu.Unlock()
	defer cancel()
	stopCancellation := make(chan struct{})
	cancellationDone := make(chan struct{})
	go func() {
		defer close(cancellationDone)
		select {
		case <-ctx.Done():
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), e.runtime.config.CleanupTimeout)
			defer cleanupCancel()
			_ = e.CancelAndJoin(cleanupCtx)
		case <-stopCancellation:
		}
	}()
	defer func() {
		close(stopCancellation)
		<-cancellationDone
	}()

	stream, err := e.runtime.conn.NewStream(streamCtx, serverStream, doGetMethod)
	if err != nil {
		return fmt.Errorf("sidecar flight: open result stream: %w", err)
	}
	if err = stream.SendMsg(&flightTicket{Ticket: e.ticket}); err != nil {
		return fmt.Errorf("sidecar flight: send ticket: %w", err)
	}
	if err = stream.CloseSend(); err != nil {
		return fmt.Errorf("sidecar flight: close ticket request: %w", err)
	}
	seenSchema := false
	for {
		data := new(flightData)
		err = stream.RecvMsg(data)
		if err == io.EOF {
			if !seenSchema {
				return fmt.Errorf("sidecar flight: stream ended before its schema")
			}
			return e.finishSuccess()
		}
		if err != nil {
			return fmt.Errorf("sidecar flight: receive result: %w", err)
		}
		if len(data.DataHeader) == 0 || uint64(len(data.DataBody)) > e.runtime.config.MaxBatchBytes {
			return fmt.Errorf("sidecar flight: malformed or oversized FlightData")
		}
		if !seenSchema {
			if len(data.DataBody) != 0 {
				return fmt.Errorf("sidecar flight: schema message contains a body")
			}
			if err = e.schema.validateStreamSchema(data.DataHeader); err != nil {
				return fmt.Errorf("sidecar flight: stream schema: %w", err)
			}
			seenSchema = true
			continue
		}
		bat, decodeErr := e.schema.decodeRecordBatch(data.DataHeader, data.DataBody, e.runtime.config.MaxBatchBytes, mp)
		if decodeErr != nil {
			return fmt.Errorf("sidecar flight: decode record batch: %w", decodeErr)
		}
		fillErr := func() error {
			defer bat.Clean(mp)
			return fill(bat, counters)
		}()
		if fillErr != nil {
			return fmt.Errorf("sidecar flight: write result batch: %w", fillErr)
		}
	}
}

func (e *Execution) finishSuccess() error {
	e.mu.Lock()
	if e.cleanupRunning {
		done := e.cleanupDone
		e.mu.Unlock()
		<-done
		e.mu.Lock()
		err := e.cleanupErr
		e.mu.Unlock()
		if err == nil {
			err = fmt.Errorf("sidecar flight: execution was cancelled while finishing")
		}
		return err
	}
	if e.terminal {
		err := e.cleanupErr
		e.mu.Unlock()
		return err
	}
	e.terminal = true
	close(e.cleanupDone)
	e.mu.Unlock()
	e.runtime.remove(e)
	return nil
}

// CancelAndJoin interrupts a blocked DoGet first, then waits for the sidecar's
// quiescent acknowledgement. It is idempotent and safe to race with Run or CN
// shutdown. The caller must not release read leases until it returns.
func (e *Execution) CancelAndJoin(ctx context.Context) error {
	if e == nil || e.runtime == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	e.mu.Lock()
	if e.terminal {
		err := e.cleanupErr
		e.mu.Unlock()
		return err
	}
	if e.cleanupRunning {
		done := e.cleanupDone
		e.mu.Unlock()
		select {
		case <-done:
			e.mu.Lock()
			err := e.cleanupErr
			e.mu.Unlock()
			return err
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	e.cleanupRunning = true
	cancel := e.streamCancel
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	actionErr := e.runtime.cancel(ctx, e.ticket, nil)
	e.mu.Lock()
	e.cleanupErr = actionErr
	e.terminal = true
	close(e.cleanupDone)
	e.mu.Unlock()
	e.runtime.remove(e)
	return actionErr
}

// CleanupAfterRun preserves the cancellation-before-lease-release ordering.
// Successful EOF is already quiescent; every other terminal path requires the
// explicit cancel-and-join acknowledgement.
func (e *Execution) CleanupAfterRun(ctx context.Context, runErr error) error {
	if runErr == nil {
		return nil
	}
	return errors.Join(runErr, e.CancelAndJoin(ctx))
}
