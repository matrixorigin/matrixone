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
	"go.uber.org/zap"
)

type IOLockKey struct {
	Path   string
	Offset int64
	End    int64
}

type IOLocks struct {
	locks sync.Map
}

var slowIOWaitDuration = time.Second * 10

func (i *IOLocks) Lock(key IOLockKey) (unlock func(), wait func()) {
	ch := make(chan struct{})
	v, loaded := i.locks.LoadOrStore(key, ch)

	if loaded {
		// not locked
		wait = func() {
			t0 := time.Now()
			for {
				timer := time.NewTimer(slowIOWaitDuration)
				select {
				case <-v.(chan struct{}):
					timer.Stop()
					return
				case <-timer.C:
					logutil.Warn("wait io lock for too long",
						zap.Any("wait", time.Since(t0)),
						zap.Any("key", key),
					)
				}
			}
		}
		return
	}

	// locked
	unlock = func() {
		i.locks.Delete(key)
		close(ch)
	}

	return
}
