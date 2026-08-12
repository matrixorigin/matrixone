// Copyright 2023 Matrix Origin
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

package fileservice

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
)

// IOMerger merges multiple I/O requests to single one
type IOMerger struct {
	flying sync.Map // IOMergeKey -> chan struct{}
}

type IOMergeKey struct {
	Path   string
	Offset int64
	End    int64
	Policy Policy
	// FullObject describes the physical read range. CacheFill additionally
	// distinguishes requests whose successful leader publishes a reusable
	// cache entry. Followers must not wait for a generation that cannot publish
	// the artifact they intend to reuse.
	FullObject bool
	CacheFill  bool
}

func NewIOMerger() *IOMerger {
	return &IOMerger{}
}

var slowIOWaitDuration = time.Second * 10

var maxIOWaitDuration = time.Minute

var shortIOWaitDuration = time.Millisecond * 200

func waitForIOGeneration(
	ctx context.Context,
	key IOMergeKey,
	generation <-chan struct{},
	maxWaitDuration time.Duration,
) (completed bool, err error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	metric.IOMergerCounterWait.Add(1)
	t0 := time.Now()
	defer func() {
		metric.IOMergerDurationWait.Observe(time.Since(t0).Seconds())
	}()

	var deadline time.Time
	if maxWaitDuration > 0 {
		deadline = t0.Add(maxWaitDuration)
	}
	nextWaitDuration := func() (time.Duration, bool) {
		if deadline.IsZero() {
			return slowIOWaitDuration, false
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return 0, true
		}
		if slowIOWaitDuration <= 0 {
			return remaining, false
		}
		return min(slowIOWaitDuration, remaining), false
	}

	waitDuration, expired := nextWaitDuration()
	if expired {
		return false, nil
	}
	if waitDuration <= 0 {
		select {
		case <-generation:
			return true, nil
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	timer := time.NewTimer(waitDuration)
	defer timer.Stop()
	for {
		select {
		case <-generation:
			return true, nil
		case <-ctx.Done():
			return false, ctx.Err()
		case <-timer.C:
			waitDuration, expired = nextWaitDuration()
			if expired {
				return false, nil
			}
			logutil.Warn("wait io for too long",
				zap.Any("wait", time.Since(t0)),
				zap.Any("key", key),
			)
			timer.Reset(waitDuration)
		}
	}
}

func (i *IOMerger) merge(key IOMergeKey) (func(), chan struct{}) {
	if v, ok := i.flying.Load(key); ok {
		return nil, v.(chan struct{})
	}

	// try initiate
	generation := make(chan struct{})
	v, loaded := i.flying.LoadOrStore(key, generation)
	if loaded {
		return nil, v.(chan struct{})
	}

	// initiated
	metric.IOMergerCounterInitiate.Add(1)
	t0 := time.Now()
	return func() {
		i.flying.Delete(key)
		close(generation)
		metric.IOMergerDurationInitiate.Observe(time.Since(t0).Seconds())
	}, nil
}

// Merge preserves the original callback API for callers that only need one
// bounded wait. S3FS and LocalFS use merge directly so a follower can remain
// bound to the exact generation it collided with across multiple wait phases.
func (i *IOMerger) Merge(key IOMergeKey, maxWaitDuration time.Duration) (done func(), wait func()) {
	done, waiter := i.merge(key)
	if waiter == nil {
		return done, nil
	}
	return nil, func() {
		_, _ = waitForIOGeneration(context.Background(), key, waiter, maxWaitDuration)
	}
}

func (i *IOMerger) IsMerging(key IOMergeKey) bool {
	_, ok := i.flying.Load(key)
	return ok
}

func (i *IOVector) ioMergeKey() IOMergeKey {
	key := IOMergeKey{
		Path:   i.FilePath,
		Policy: i.Policy,
	}
	min, max, readFull := i.readRange()
	if readFull {
		key.FullObject = true
		return key
	}
	return i.ioMergeKeyWithRange(key, min, max)
}

func (i *IOVector) ioMergeKeyWithRange(key IOMergeKey, min *int64, max *int64) IOMergeKey {
	if min != nil {
		key.Offset = *min
	}
	if max != nil {
		key.End = *max
	}
	return key
}
