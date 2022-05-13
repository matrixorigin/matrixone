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
	"io"
	"os"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

const (
	DefaultVersionFileSize = 300 * int(common.M)
)

type RotationCfg struct {
	RotateChecker  IRotateChecker
	Observer       Observer
	HistoryFactory HistoryFactory
}

type VersionReplayHandler = func(*VersionFile, ReplayObserver) error

type StoreFileWriter interface {
	io.Writer
	PrepareWrite(int) error
	ApplyCommit(uint64)
	ApplyCheckpoint(common.Range)
}

type StoreFile interface {
	StoreFileWriter
	io.Closer
	sync.Locker
	RLock()
	RUnlock()
	Sync() error
	Truncate(int64) error
	Stat() (os.FileInfo, error)
	GetHistory() IHistory
	ReplayVersions(VersionReplayHandler) error
	TryCompact()
}

type Store interface {
	io.Closer
	AppendEntry(Entry) error
	Sync() error
	ReplayVersions(VersionReplayHandler) error
	Truncate(int64) error
	GetHistory() IHistory
	TryCompact()
}

type store struct {
	dir  string
	name string
	file StoreFile
	pos  int64
}

func New(dir, name string, cfg *RotationCfg) (*store, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err = os.MkdirAll(dir, os.ModePerm); err != nil {
			return nil, err
		}
	}
	if cfg == nil {
		cfg = &RotationCfg{}
	}
	w, err := OpenRotational(dir, name, DefaultSuffix, cfg.HistoryFactory, cfg.RotateChecker, cfg.Observer)
	if err != nil {
		return nil, err
	}
	s := &store{
		dir:  dir,
		name: name,
		file: w,
	}
	stats, err := s.file.Stat()
	if err != nil {
		w.Close()
		return nil, err
	}
	s.pos = stats.Size()
	// s.writer = s.file
	return s, err
}

func (s *store) Close() error {
	// if err := s.writer.Flush(); err != nil {
	// 	return err
	// }
	if err := s.file.Sync(); err != nil {
		return err
	}
	return s.file.Close()
}

func (s *store) Truncate(size int64) error {
	return s.file.Truncate(size)
}

func (s *store) GetHistory() IHistory {
	return s.file.GetHistory()
}

func (s *store) AppendEntry(entry Entry) error {
	// defer entry.Free()
	if _, err := entry.WriteTo(s.file, s.file); err != nil {
		return err
	}
	// logutil.Infof("WriteEntry %d, Size=%d", entry.Type(), entry.Size())
	return nil
}

func (s *store) Sync() error {
	if err := s.AppendEntry(FlushEntry); err != nil {
		return err
	}
	// if err := s.writer.Flush(); err != nil {
	// 	return err
	// }
	err := s.file.Sync()
	return err
}

func (s *store) ReplayVersions(handler VersionReplayHandler) error {
	return s.file.ReplayVersions(handler)
}

func (s *store) TryCompact() {
	s.file.TryCompact()
}
