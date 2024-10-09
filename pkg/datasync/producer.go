// Copyright 2021 - 2024 Matrix Origin
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

package datasync

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"go.uber.org/zap"
)

// adjustLeaseHolderID updates the leaseholder ID of the data.
// The original leaseholder ID is replica ID of TN, reuse the payload and
// set it to upstream Lsn.
func adjustLeaseholderID(old []byte, upstreamLsn uint64) {
	// headerSize is 4.
	binaryEnc.PutUint64(old[headerSize:], upstreamLsn)
}

type producerOption func(*producer)

func withIgnoreWhenError(b bool) producerOption {
	return func(p *producer) {
		p.ignoreWhenError = b
	}
}

func withRetryTimes(r int) producerOption {
	return func(p *producer) {
		p.retryTimes = r
	}
}

type producer struct {
	common
	client *logClient
	// dataQ contains all the data comes from TN shard.
	dataQ queue
	// writeLsn if the LSN of the newest entry in log shard.
	writeLsn *atomic.Uint64
	// retryTimes is the times to retry when there is a write error.
	retryTimes int
	// ignoreWhenError ignores the data when there is a write error.
	ignoreWhenError bool
}

func newProducer(
	common common, qSize int, writeLsn *atomic.Uint64, opts ...producerOption,
) Producer {
	p := &producer{
		common: common,
		client: newLogClient(common, logShardID),
		dataQ: newDataQueue(qSize, func(w *wrappedData) bool {
			if locations := getLocations(logservice.LogRecord{
				Type: logservice.UserRecord,
				Data: w.data,
			}); len(locations) == 0 {
				return true
			}
			return false
		}),
		writeLsn:   writeLsn,
		retryTimes: 20,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Enqueue implements the Producer interface.
func (p *producer) Enqueue(ctx context.Context, w *wrappedData) {
	p.dataQ.enqueue(ctx, w)
}

// Close implements the Worker interface.
func (p *producer) Close() {
	p.client.close()
	p.dataQ.close()
}

func (p *producer) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		default:
			w, err := p.dataQ.dequeue(ctx)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					p.log.Error("failed to dequeue data", zap.Error(err))
				}
				p.pool.release(w)
				continue
			}
			// handle the data.
			p.handle(ctx, w)
		}
	}
}

func (p *producer) handle(ctx context.Context, w *wrappedData) {
	if w == nil {
		return
	}

	defer p.pool.release(w)
	if w.wg != nil {
		defer w.wg.Done()
	}

	// Check the upstream Lsn, it must be available.
	if w.upstreamLsn == 0 {
		panic("invalid upstream Lsn")
	}

	if locations := getLocations(logservice.LogRecord{
		Type: logservice.UserRecord,
		Data: w.data,
	}); len(locations) > 0 {
		// TODO(volgariver6): maybe we should produce a parsed instance?
		p.produce(ctx, w)
	}
}

func (p *producer) produce(ctx context.Context, w *wrappedData) {
	// Update the leaseholder ID field in the payload to upstream Lsn.
	adjustLeaseholderID(w.data, w.upstreamLsn)

	var lsn uint64
	var err error
	for {
		lsn, err = p.client.writeWithRetry(ctx, w.data, p.retryTimes)
		if err != nil {
			p.log.Error("failed to write entry", zap.Error(err))
			if ctx.Err() != nil {
				return
			}
			if p.ignoreWhenError {
				return
			}
		} else {
			break
		}
	}

	// Forward the writeLsn.
	if lsn > p.writeLsn.Load() {
		p.writeLsn.Store(lsn)
	}
}
