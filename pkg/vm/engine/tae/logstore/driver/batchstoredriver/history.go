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

package batchstoredriver

import (
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

var (
	ErrHistoryEntryNotFound = moerr.NewInternalError("tae: history not found")
)

type HistoryFactory func() History

var (
	DefaultHistoryFactory = func() History {
		return newHistory(nil)
	}
)

type history struct {
	mu      *sync.RWMutex
	entries []VFile
}

func newHistory(mu *sync.RWMutex) *history {
	if mu == nil {
		mu = new(sync.RWMutex)
	}
	return &history{
		mu: mu,
	}
}

func (h *history) String() string {
	s := "{"
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, entry := range h.entries {
		s = fmt.Sprintf("%s\n%s", s, entry.String())
	}
	if len(h.entries) > 0 {
		s = fmt.Sprintf("%s\n}", s)
	} else {
		s = fmt.Sprintf("%s}", s)
	}
	return s
}

func (h *history) OldestEntry() VFile {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.entries) == 0 {
		return nil
	}
	return h.entries[0]
}

func (h *history) Entries() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.entries)
}

func (h *history) Empty() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.entries) == 0
}

func (h *history) findEntry(id int) (int, VFile) {
	for idx, entry := range h.entries {
		if entry.Id() == id {
			return idx, entry
		}
	}
	return 0, nil
}

func (h *history) DropEntry(id int) (VFile, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	idx, entry := h.findEntry(id)
	if entry == nil {
		return nil, ErrHistoryEntryNotFound
	}
	h.entries = append(h.entries[:idx], h.entries[idx+1:]...)
	return entry, entry.Destroy()
}

func (h *history) Extend(entries ...VFile) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.entries = append(h.entries, entries...)
}

func (h *history) Append(entry VFile) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.entries = append(h.entries, entry)
}

func (h *history) GetEntry(id int) VFile {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, e := h.findEntry(id)
	return e
}

func (h *history) EntryIds() []int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	ids := make([]int, len(h.entries))
	for idx, entry := range h.entries {
		ids[idx] = entry.Id()
	}
	return ids
}

func (h *history) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, entry := range h.entries {
		_ = entry.Close()
	}
	h.entries = nil
}
