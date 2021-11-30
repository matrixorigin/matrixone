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

package wal

import "sync"

var (
	_entryPool = sync.Pool{New: func() interface{} {
		return &Entry{}
	}}
)

type Entry struct {
	wg      sync.WaitGroup
	Id      uint64
	Payload Payload
}

func GetEntry(id uint64) *Entry {
	e := _entryPool.Get().(*Entry)
	e.Id = id
	e.wg.Add(1)
	return e
}

func (e *Entry) reset() {
	e.Payload = nil
	e.Id = 0
	e.wg = sync.WaitGroup{}
}

func (e *Entry) SetDone() {
	e.wg.Done()
}

func (e *Entry) WaitDone() {
	e.wg.Wait()
}

func (e *Entry) Free() {
	e.reset()
	_entryPool.Put(e)
}
