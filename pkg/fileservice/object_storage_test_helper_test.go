// Copyright 2025 Matrix Origin
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

package fileservice

import (
	"context"
	"io"
	"iter"
	"strings"
	"time"
)

type dummyObjectStorage struct{}

func (dummyObjectStorage) Delete(ctx context.Context, keys ...string) error {
	return nil
}

func (dummyObjectStorage) Exists(ctx context.Context, key string) (bool, error) {
	return false, nil
}

func (dummyObjectStorage) List(ctx context.Context, prefix string) iter.Seq2[*DirEntry, error] {
	return func(yield func(*DirEntry, error) bool) {}
}

func (dummyObjectStorage) Read(ctx context.Context, key string, min *int64, max *int64) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (dummyObjectStorage) Stat(ctx context.Context, key string) (int64, error) {
	return 0, nil
}

func (dummyObjectStorage) Write(ctx context.Context, key string, r io.Reader, sizeHint *int64, expire *time.Time) error {
	_, _ = io.Copy(io.Discard, r)
	return nil
}

type readTestObjectStorage struct {
	dummyObjectStorage
	open func(context.Context, string, *int64, *int64, *ObjectIdentity) (io.ReadCloser, error)
}

func (s *readTestObjectStorage) Read(ctx context.Context, key string, min, max *int64) (io.ReadCloser, error) {
	return s.open(ctx, key, min, max, nil)
}
func (s *readTestObjectStorage) ReadObjectWithIdentity(ctx context.Context, key string, min, max *int64, expected ObjectIdentity) (io.ReadCloser, error) {
	return s.open(ctx, key, min, max, &expected)
}
func (s *readTestObjectStorage) StatObjectIdentity(context.Context, string) (ObjectIdentity, error) {
	return ObjectIdentity{ETag: "version1", Size: 8}, nil
}
