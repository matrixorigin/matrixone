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
}

func NewIOMerger() *IOMerger {
	return &IOMerger{}
}

var slowIOWaitDuration = time.Second * 10

func (i *IOMerger) waitFunc(key IOMergeKey, ch chan struct{}) func() {
	metric.IOMergerCounterWait.Add(1)
	return func() {
		t0 := time.Now()
		defer func() {
			metric.IOMergerDurationWait.Observe(time.Since(t0).Seconds())
		}()
		for {
			timer := time.NewTimer(slowIOWaitDuration)
			select {
			case <-ch:
				timer.Stop()
				return
			case <-timer.C:
				logutil.Warn("wait io for too long",
					zap.Any("wait", time.Since(t0)),
					zap.Any("key", key),
				)
			}
		}
	}
}

func (i *IOMerger) Merge(key IOMergeKey) (done func(), wait func()) {
	if v, ok := i.flying.Load(key); ok {
		// wait
		return nil, i.waitFunc(key, v.(chan struct{}))
	}

	// try initiate
	ch := make(chan struct{})
	v, loaded := i.flying.LoadOrStore(key, ch)
	if loaded {
		// not the first request, wait
		return nil, i.waitFunc(key, v.(chan struct{}))
	}

	// initiated
	metric.IOMergerCounterInitiate.Add(1)
	t0 := time.Now()
	return func() {
		defer func() {
			metric.IOMergerDurationInitiate.Observe(time.Since(t0).Seconds())
		}()
		i.flying.Delete(key)
		close(ch)
	}, nil
}

func (i *IOVector) ioMergeKey() IOMergeKey {
	key := IOMergeKey{
		Path:   i.FilePath,
		Policy: i.Policy,
	}
	min, max, readFull := i.readRange()
	if readFull {
		return key
	}
	if min != nil {
		key.Offset = *min
	}
	if max != nil {
		key.End = *max
	}
	return key
}
