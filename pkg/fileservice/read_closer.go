// Copyright 2026 Matrix Origin
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
	"sync"
)

// NewReadCloserWithOwnedFileService takes ownership of a non-nil reader and
// service. Close closes the reader before the service, even if the reader
// returns an error. Repeated and concurrent Close calls return that same error.
// Borrowed services must remain with their owner and must not use this wrapper.
func NewReadCloserWithOwnedFileService(ctx context.Context, reader io.ReadCloser, service FileService) io.ReadCloser {
	return &readCloserWithFileService{ReadCloser: reader, service: service, ctx: ctx}
}

type readCloserWithFileService struct {
	io.ReadCloser
	service FileService
	ctx     context.Context
	once    sync.Once
	err     error
}

func (r *readCloserWithFileService) Close() error {
	r.once.Do(func() {
		r.err = r.ReadCloser.Close()
		r.service.Close(r.ctx)
	})
	return r.err
}
