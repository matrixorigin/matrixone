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

package metadata

import (
	"errors"
	"fmt"
	"io"
	"matrixone/pkg/logutil"
	"matrixone/pkg/vm/engine/aoe/storage/logstore"
	"sync"
)

type Store = logstore.Store

func defaultHandler(r io.Reader, meta *LogEntryMeta) (LogEntry, int64, error) {
	entry := logstore.NewBaseEntryWithMeta(meta)
	n, err := entry.ReadFrom(r)
	if err != nil {
		return nil, int64(n), err
	}
	return entry, int64(n), nil
}

type catalogReplayer struct {
	catalog  *Catalog
	replayed int
	offset   int64
}

func newCatalogReplayer() *catalogReplayer {
	replayer := &catalogReplayer{}
	return replayer
}

func (replayer *catalogReplayer) RebuildCatalog(mu *sync.RWMutex, cfg *CatalogCfg, syncerCfg *SyncerCfg) (*Catalog, error) {
	replayer.catalog = NewCatalog(mu, cfg, syncerCfg)
	if err := replayer.Replay(replayer.catalog.Store); err != nil {
		return nil, err
	}
	replayer.catalog.Store.TryCompact()
	return replayer.catalog, nil
}

func (replayer *catalogReplayer) doReplay(r *logstore.VersionFile, observer logstore.ReplayObserver) error {
	meta := logstore.NewEntryMeta()
	_, err := meta.ReadFrom(r)
	if err != nil {
		return err
	}
	if entry, n, err := defaultHandler(r, meta); err != nil {
		return err
	} else {
		if n != int64(meta.PayloadSize()) {
			return errors.New(fmt.Sprintf("payload mismatch: %d != %d", n, meta.PayloadSize()))
		}
		if err = replayer.catalog.onReplayEntry(entry, observer); err != nil {
			return err
		}
	}
	replayer.offset += int64(meta.Size())
	replayer.offset += int64(meta.PayloadSize())
	replayer.replayed++
	return nil
}

func (replayer *catalogReplayer) Replay(s Store) error {
	err := s.ReplayVersions(replayer.doReplay)
	logutil.Infof("Total %d entries replayed", replayer.replayed)
	return err
}

func (replayer *catalogReplayer) GetOffset() int64 {
	return replayer.offset
}

func (replayer *catalogReplayer) Truncate(s Store) error {
	return s.Truncate(replayer.offset)
}

func (replayer *catalogReplayer) TotalEntries() int {
	return replayer.replayed
}

func (replayer *catalogReplayer) String() string {
	s := fmt.Sprintf("<CatalogReplayer>(Entries:%d,Offset:%d)", replayer.replayed, replayer.offset)
	return s
}

func (replayer *catalogReplayer) RegisterEntryHandler(_ LogEntryType, _ logstore.EntryHandler) error {
	return nil
}
