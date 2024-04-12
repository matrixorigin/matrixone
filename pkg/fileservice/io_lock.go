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
	locks sync.Map // IOLockKey -> chan struct{}
}

func NewIOLocks() *IOLocks {
	return &IOLocks{}
}

var slowIOWaitDuration = time.Second * 10

func (i *IOLocks) waitFunc(key IOLockKey, ch chan struct{}) func() {
	return func() {
		t0 := time.Now()
		for {
			timer := time.NewTimer(slowIOWaitDuration)
			select {
			case <-ch:
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
}

func (i *IOLocks) Lock(key IOLockKey) (unlock func(), wait func()) {
	if v, ok := i.locks.Load(key); ok {
		// wait
		return nil, i.waitFunc(key, v.(chan struct{}))
	}

	// try lock
	ch := make(chan struct{})
	v, loaded := i.locks.LoadOrStore(key, ch)
	if loaded {
		// lock failed, wait
		return nil, i.waitFunc(key, v.(chan struct{}))
	}

	// locked
	return func() {
		i.locks.Delete(key)
		close(ch)
	}, nil
}
