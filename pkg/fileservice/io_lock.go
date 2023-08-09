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

import "sync"

type IOLockKey struct {
	File string
}

type IOLocks struct {
	locks sync.Map
}

func (i *IOLocks) Lock(key IOLockKey) (unlock func(), wait func()) {
	ch := make(chan struct{})
	v, loaded := i.locks.LoadOrStore(key, ch)
	if loaded {
		// not locked
		wait = func() {
			<-v.(chan struct{})
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
