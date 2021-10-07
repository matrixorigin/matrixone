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

package logstore

import "sync"

type AsyncEntry interface {
	Entry
	GetError() error
	WaitDone() error
	DoneWithErr(error)
}

var (
	_asyncEntPool = sync.Pool{New: func() interface{} {
		e := &AsyncBaseEntry{
			BaseEntry: *newBaseEntry(),
		}
		return e
	}}
)

func NewAsyncBaseEntry() *AsyncBaseEntry {
	e := _asyncEntPool.Get().(*AsyncBaseEntry)
	e.wg.Add(1)
	return e
}

type AsyncBaseEntry struct {
	BaseEntry
	wg  sync.WaitGroup
	err error
}

func (e *AsyncBaseEntry) reset() {
	e.wg = sync.WaitGroup{}
	e.BaseEntry.reset()
}

func (e *AsyncBaseEntry) DoneWithErr(err error) {
	e.err = err
	e.wg.Done()
}

func (e *AsyncBaseEntry) WaitDone() error {
	e.wg.Wait()
	return e.err
}

func (e *AsyncBaseEntry) GetError() error {
	return e.err
}

func (e *AsyncBaseEntry) IsAsync() bool {
	return true
}

func (e *AsyncBaseEntry) Free() {
	if e == nil {
		return
	}
	e.reset()
	_asyncEntPool.Put(e)
}
