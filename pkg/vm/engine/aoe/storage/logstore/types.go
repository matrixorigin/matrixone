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
	"io"
	"os"
	"path/filepath"
	"sync"
)

type StoreEntryHandler = func(io.Reader) error

type Store interface {
	io.Closer
	AppendEntry(Entry) error
	Sync() error
	ForLoopEntries(StoreEntryHandler) error
	Truncate(int64) error
}

type store struct {
	mu     sync.RWMutex
	dir    string
	name   string
	file   *os.File
	pos    int64
	writer io.Writer
	// reader *bufio.Reader
	reader io.Reader
}

func New(dir, name string) (*store, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err = os.MkdirAll(dir, os.ModePerm); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return nil, err
	}
	s := &store{
		dir:  dir,
		name: name,
		file: f,
	}
	stats, err := s.file.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	s.pos = stats.Size()
	// s.writer = bufio.NewWriter(s.file)
	s.writer = s.file
	// s.reader = bufio.NewReader(s.file)
	s.reader = s.file
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

func (s *store) AppendEntry(entry Entry) error {
	if _, err := entry.WriteTo(s.writer, &s.mu); err != nil {
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

func (s *store) ForLoopEntries(handler StoreEntryHandler) error {
	for {
		if err := handler(s.reader); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}
