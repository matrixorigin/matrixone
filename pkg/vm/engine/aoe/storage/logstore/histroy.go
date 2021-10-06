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

import (
	"errors"
	"fmt"
	"io"
	"sync"
)

var (
	VersionNotFoundErr = errors.New("aoe: version not found")
)

type HistoryFactory func() IHistory

var (
	DefaltHistoryFactory = func() IHistory {
		return &baseHistroy{
			versions: make([]*VersionFile, 0),
		}
	}
)

type IHistory interface {
	String() string
	Append(*VersionFile)
	Extend([]*VersionFile)
	Versions() []uint64
	VersionCnt() int
	Truncate(uint64) error
	Version(uint64) *VersionFile
	GetOldest() *VersionFile
	Empty() bool
	ReplayVersions(VersionReplayHandler, ReplayObserver) error
}

type baseHistroy struct {
	mu       sync.RWMutex
	versions []*VersionFile
}

func (h *baseHistroy) ReplayVersions(handler VersionReplayHandler, observer ReplayObserver) error {
	for _, version := range h.versions {
		observer.OnNewVersion(version.Version)
		for {
			if err := handler(version, observer); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
		}
	}
	return nil
}

func (h *baseHistroy) GetOldest() *VersionFile {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.versions) == 0 {
		return nil
	}
	return h.versions[0]
}

func (h *baseHistroy) VersionCnt() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.versions)
}

func (h *baseHistroy) Empty() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.versions) == 0
}

func (h *baseHistroy) Truncate(id uint64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	pos, version := h.findVersion(id)
	if version == nil {
		return VersionNotFoundErr
	}
	h.versions = append(h.versions[:pos], h.versions[pos+1:]...)
	return version.Destroy()
}

func (h *baseHistroy) findVersion(id uint64) (int, *VersionFile) {
	for pos, version := range h.versions {
		if version.Version == id {
			return pos, version
		}
	}
	return 0, nil
}

func (h *baseHistroy) Version(id uint64) *VersionFile {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, v := h.findVersion(id)
	return v
}

func (h *baseHistroy) Versions() []uint64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	ids := make([]uint64, len(h.versions))
	for i, v := range h.versions {
		ids[i] = v.Version
	}
	return ids
}

func (h *baseHistroy) Append(vf *VersionFile) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.versions = append(h.versions, vf)
}

func (h *baseHistroy) Extend(vf []*VersionFile) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.versions = append(h.versions, vf...)
}

func (h *baseHistroy) String() string {
	s := fmt.Sprintf("{")
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, v := range h.versions {
		s = fmt.Sprintf("%s\n%s", s, v.Name())
	}
	if len(h.versions) > 0 {
		s = fmt.Sprintf("%s\n}", s)
	} else {
		s = fmt.Sprintf("%s}", s)
	}
	return s
}
