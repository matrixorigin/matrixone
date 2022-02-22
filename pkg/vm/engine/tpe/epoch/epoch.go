// Copyright 2021 Matrix Origin
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

package epoch

import "sync"

type EpochHandler struct {
	epoch uint64
	rwlock sync.RWMutex
}

func NewEpochHandler() *EpochHandler {
	return &EpochHandler{}
}

func (eh *EpochHandler) SetEpoch(e uint64) {
	eh.rwlock.Lock()
	defer eh.rwlock.Unlock()
	eh.epoch = e
}

func (eh *EpochHandler) GetEpoch() uint64{
	eh.rwlock.RLock()
	defer eh.rwlock.RUnlock()
	return eh.epoch
}

func (eh *EpochHandler) RemoveDeletedTable(epoch uint64) (int, error) {
	eh.rwlock.Lock()
	defer eh.rwlock.Unlock()
	//TODO:implement
	return 0, nil
}