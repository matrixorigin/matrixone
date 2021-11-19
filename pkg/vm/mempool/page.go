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

package mempool

import (
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/malloc"
)

type Page struct {
	Next *Page
	Buf  []byte
}

const (
	PageSize             = 1 << 20
	MaxPageCacheSize     = 1 << 30
	MaxPageCacheCapacity = MaxPageCacheSize / PageSize
)

var (
	PageCacheIdleReleaseTimeout = time.Second * 10
)

func newPage() *Page {
	return &Page{
		Buf: malloc.Malloc(PageSize)[:0],
	}
}

func (p *Page) Reset() {
	p.Next = nil
	p.Buf = p.Buf[:0]
}

var pageCache = make(chan *Page, MaxPageCacheCapacity)

var pageCacheLastGetAt int64

func getPage() *Page {
	select {
	case page := <-pageCache:
		// from cache
		atomic.AddInt64(&pageCacheLastGetAt, 1)
		return page
	default:
		// cache is empty
		return newPage()
	}
}

func releasePages(page *Page) {
	for page != nil {
		next := page.Next
		page.Reset()
		select {
		case pageCache <- page:
			// cache not full
		default:
			// cache full
			return
		}
		page = next
	}
}

func init() {
	// page cache idle release
	go func() {
		drainPendingAt := atomic.LoadInt64(&pageCacheLastGetAt)
		for range time.NewTicker(PageCacheIdleReleaseTimeout).C {
			lastGetAt := atomic.LoadInt64(&pageCacheLastGetAt)
			if lastGetAt == drainPendingAt {
				// not active, drain the cache
			loop:
				for {
					select {
					case <-pageCache:
					default:
						break loop
					}
				}
			}
			drainPendingAt = lastGetAt
		}
	}()
}
