// Copyright 2023 Matrix Origin
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
	"time"
)

type objectStorageSemaphore struct {
	upstream  ObjectStorage
	semaphore chan struct{}
}

func newObjectStorageSemaphore(
	upstream ObjectStorage,
	capacity int64,
) *objectStorageSemaphore {
	return &objectStorageSemaphore{
		upstream:  upstream,
		semaphore: make(chan struct{}, capacity),
	}
}

func (o *objectStorageSemaphore) acquire() {
	o.semaphore <- struct{}{}
}

func (o *objectStorageSemaphore) release() {
	<-o.semaphore
}

var _ ObjectStorage = new(objectStorageSemaphore)

func (o *objectStorageSemaphore) Delete(ctx context.Context, keys ...string) (err error) {
	o.acquire()
	defer o.release()
	return o.upstream.Delete(ctx, keys...)
}

func (o *objectStorageSemaphore) Exists(ctx context.Context, key string) (bool, error) {
	o.acquire()
	defer o.release()
	return o.upstream.Exists(ctx, key)
}

func (o *objectStorageSemaphore) List(ctx context.Context, prefix string, fn func(isPrefix bool, key string, size int64) (bool, error)) (err error) {
	// this operation may block for a long time, and less used, skip semaphore
	return o.upstream.List(ctx, prefix, fn)
}

func (o *objectStorageSemaphore) Read(ctx context.Context, key string, min *int64, max *int64) (r io.ReadCloser, err error) {
	o.acquire()
	defer o.release()
	return o.upstream.Read(ctx, key, min, max)
}

func (o *objectStorageSemaphore) Stat(ctx context.Context, key string) (size int64, err error) {
	o.acquire()
	defer o.release()
	return o.upstream.Stat(ctx, key)
}

func (o *objectStorageSemaphore) Write(ctx context.Context, key string, r io.Reader, size int64, expire *time.Time) (err error) {
	// this operation may block for a long time, and less used, skip semaphore
	return o.upstream.Write(ctx, key, r, size, expire)
}
