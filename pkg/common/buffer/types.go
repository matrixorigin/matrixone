// Copyright 2021 - 2023 Matrix Origin
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

package buffer

import (
	"sync"
)

const (
	// 4KB may be a good size for page
	PageSize  = 4 << 10
	WordSize  = 8
	PageCount = (1 << 10) / WordSize
	PageFree  = 0
	PageFull  = 1
)

type page struct {
	seat uint64
	head uintptr
	ptr  uintptr
	data []byte
	next *page
	mu   sync.Mutex
}

type freeList struct {
	sync.Mutex
	head *page
}

type pageHeader struct {
	freeList *freeList
	fullList *freeList
}

type Buffer struct {
	data     []byte
	ptr      uintptr
	freeList *freeList
	pages    [PageCount]*pageHeader
}
