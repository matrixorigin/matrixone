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
	"matrixone/pkg/logutil"
)

type EntryHandler = func(io.Reader, *EntryMeta) (Entry, int64, error)

type Replayer interface {
	Replay(Store) error
	Truncate(Store) error
	RegisterEntryHandler(EntryType, EntryHandler) error
	GetOffset() int64
}

type simpleReplayer struct {
	uncommitted []Entry
	committed   []Entry
	handlers    map[EntryType]EntryHandler
	count       int
	offset      int64
	truncOffset int64
}

func NewSimpleReplayer() *simpleReplayer {
	replayer := &simpleReplayer{
		uncommitted: make([]Entry, 0),
		committed:   make([]Entry, 0),
		handlers:    make(map[EntryType]EntryHandler),
	}
	replayer.handlers[ETFlush] = replayer.onFlush
	return replayer
}

func (replayer *simpleReplayer) onFlush(r io.Reader, meta *EntryMeta) (Entry, int64, error) {
	entry := NewBaseEntryWithMeta(meta)
	n, err := entry.ReadFrom(r)
	if err != nil {
		return nil, int64(n), err
	}
	return entry, int64(n), nil
}

func (replayer *simpleReplayer) GetOffset() int64 {
	return replayer.truncOffset
}

func (replayer *simpleReplayer) Truncate(s Store) error {
	return s.Truncate(replayer.truncOffset)
}

func (replayer *simpleReplayer) RegisterEntryHandler(eType EntryType, handler EntryHandler) error {
	duplicate := replayer.handlers[eType]
	if duplicate != nil {
		return errors.New(fmt.Sprintf("duplicate handler found for %d", eType))
	}
	replayer.handlers[eType] = handler
	return nil
}

func (replayer *simpleReplayer) doReplay(r *VersionFile) error {
	meta := NewEntryMeta()
	n, err := meta.ReadFrom(r)
	if err != nil {
		return err
	}
	eType := meta.GetType()
	replayer.offset += int64(n)
	handler := replayer.handlers[eType]
	if handler == nil {
		logutil.Infof("Replaying (%d, %d, %d) - %d", eType, meta.PayloadSize(), replayer.offset, replayer.count)
		return errors.New(fmt.Sprintf("no handler for type: %d", eType))
	}
	if entry, n, err := handler(r, meta); err != nil {
		return err
	} else {
		if n != int64(meta.PayloadSize()) {
			panic(fmt.Sprintf("bad %d, %d for type %d", n, meta.PayloadSize(), eType))
		}
		replayer.offset += n
		if !entry.GetMeta().IsFlush() {
			replayer.uncommitted = append(replayer.uncommitted, entry)
		} else {
			replayer.committed = append(replayer.committed, replayer.uncommitted...)
			replayer.uncommitted = replayer.uncommitted[:0]
			replayer.truncOffset = replayer.offset
		}
	}
	replayer.count++
	return nil
}

func (replayer *simpleReplayer) Replay(s Store) error {
	err := s.ForLoopVersions(replayer.doReplay)
	logutil.Infof("replay count: %d", replayer.count)
	return err
}
