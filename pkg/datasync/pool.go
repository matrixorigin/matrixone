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

import "sync"

type wrappedData struct {
	data        []byte
	upstreamLsn uint64
	wg          *sync.WaitGroup
}

func newWrappedData(data []byte, upstreamLsn uint64, wg *sync.WaitGroup) *wrappedData {
	return &wrappedData{
		data:        data,
		upstreamLsn: upstreamLsn,
		wg:          wg,
	}
}

type dataPool interface {
	// acquire acquires data from the pool. The parameter is the data size.
	acquire(int) *wrappedData
	// release releases the data back to the pool.
	release(*wrappedData)
}

type bytesPool struct {
	pool sync.Pool
}

// newDataPool creates a new data pool. The parameter indicates the
// size of the data in the pool.
func newDataPool(size int) dataPool {
	return &bytesPool{
		pool: sync.Pool{
			New: func() any {
				return newWrappedData(make([]byte, size), 0, nil)
			},
		},
	}
}

// acquire implements the dataPool interface.
// If the size is larger than the size of data in the pool,
// creates new data instead of fetch from the pool.
func (p *bytesPool) acquire(size int) *wrappedData {
	w := p.pool.Get().(*wrappedData)
	if cap(w.data) < size {
		p.pool.Put(w)
		w = newWrappedData(make([]byte, size), 0, nil)
	} else {
		w.data = w.data[:size]
	}
	return w
}

// release implements the dataPool interface.
// If the size of the data is larger than the default value,
// do not put it back to the pool.
func (p *bytesPool) release(w *wrappedData) {
	if w == nil {
		return
	}
	if cap(w.data) > defaultDataSize {
		return
	}
	w.upstreamLsn = 0
	p.pool.Put(w)
}
