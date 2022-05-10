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

package store

import (
	"errors"
	"fmt"
	"sync"
)

var (
	HistoryEntryNotFoundErr = errors.New("aoe: history not found")
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
	s := fmt.Sprintf("{")
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
		return nil, HistoryEntryNotFoundErr
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

type entryWrapper struct {
	offset int
	entry  VFile
}

// One worker
// h.mu.Rlock
// wrapper
func (h *history) TryTruncate() error {
	c := newCompactor()
	toDelete := make([]entryWrapper, 0, 4)
	h.mu.RLock()
	entries := make([]VFile, len(h.entries))
	for i, entry := range h.entries {
		entries[i] = entry
	}
	h.mu.RUnlock()
	for i := len(entries) - 1; i >= 0; i-- {
		e := entries[i]
		e.PrepareCompactor(c)
	}
	for i := len(entries) - 1; i >= 0; i-- {
		e := entries[i]
		e.LoadMeta()
		wrapper := entryWrapper{entry: e}
		if e.IsToDelete(c) {
			wrapper.offset = i
			toDelete = append(toDelete, wrapper)
		}
		// e.FreeMeta()
	}
	h.mu.Lock()
	for _, wrapper := range toDelete {
		h.entries = append(h.entries[:wrapper.offset], h.entries[wrapper.offset+1:]...)
	}
	h.mu.Unlock()
	for _, wrapper := range toDelete {
		if err := wrapper.entry.Destroy(); err != nil {
			return err
		}
	}
	return nil
}

func (h *history) Replay(r *replayer, observer ReplayObserver) error {
	for _, entry := range h.entries {
		err := entry.Replay(r, observer)
		if err != nil {
			return err
		}
	}
	return nil
}
