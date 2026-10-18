// Copyright 2024 Matrix Origin
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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type objectStorageHTTPTrace struct {
	upstream ObjectStorage
}

func newObjectStorageHTTPTrace(upstream ObjectStorage) *objectStorageHTTPTrace {
	return &objectStorageHTTPTrace{
		upstream: upstream,
	}
}

var _ ObjectStorage = new(objectStorageHTTPTrace)
var _ ParallelMultipartWriter = new(objectStorageHTTPTrace)
var _ objectStorageCopier = new(objectStorageHTTPTrace)
var _ objectStorageIdentityReader = new(objectStorageHTTPTrace)

func (o *objectStorageHTTPTrace) CopyObject(
	ctx context.Context,
	src ObjectStorage,
	srcKey string,
	dstKey string,
) (bool, error) {
	copier, ok := o.upstream.(objectStorageCopier)
	if !ok {
		return false, nil
	}
	ctx = withHTTPTrace(ctx)
	return copier.CopyObject(ctx, src, srcKey, dstKey)
}

func (o *objectStorageHTTPTrace) Delete(ctx context.Context, keys ...string) (err error) {
	ctx = withHTTPTrace(ctx)
	return o.upstream.Delete(ctx, keys...)
}

func (o *objectStorageHTTPTrace) Exists(ctx context.Context, key string) (bool, error) {
	ctx = withHTTPTrace(ctx)
	return o.upstream.Exists(ctx, key)
}

func (o *objectStorageHTTPTrace) List(ctx context.Context, prefix string) iter.Seq2[*DirEntry, error] {
	ctx = withHTTPTrace(ctx)
	return o.upstream.List(ctx, prefix)
}

func (o *objectStorageHTTPTrace) Read(ctx context.Context, key string, min *int64, max *int64) (r io.ReadCloser, err error) {
	ctx = withHTTPTrace(ctx)
	return o.upstream.Read(ctx, key, min, max)
}

func (o *objectStorageHTTPTrace) StatObjectIdentity(ctx context.Context, key string) (ObjectIdentity, error) {
	upstream, ok := o.upstream.(objectStorageIdentityReader)
	if !ok {
		return ObjectIdentity{}, moerr.NewNotSupported(ctx, "object storage identity")
	}
	ctx = withHTTPTrace(ctx)
	return upstream.StatObjectIdentity(ctx, key)
}

func (o *objectStorageHTTPTrace) ReadObjectWithIdentity(
	ctx context.Context,
	key string,
	min *int64,
	max *int64,
	expected ObjectIdentity,
) (io.ReadCloser, error) {
	upstream, ok := o.upstream.(objectStorageIdentityReader)
	if !ok {
		return nil, moerr.NewNotSupported(ctx, "conditional object storage read")
	}
	ctx = withHTTPTrace(ctx)
	return upstream.ReadObjectWithIdentity(ctx, key, min, max, expected)
}

func (o *objectStorageHTTPTrace) Stat(ctx context.Context, key string) (size int64, err error) {
	ctx = withHTTPTrace(ctx)
	return o.upstream.Stat(ctx, key)
}

func (o *objectStorageHTTPTrace) Write(ctx context.Context, key string, r io.Reader, sizeHint *int64, expire *time.Time) (err error) {
	ctx = withHTTPTrace(ctx)
	return o.upstream.Write(ctx, key, r, sizeHint, expire)
}

func (o *objectStorageHTTPTrace) SupportsParallelMultipart() bool {
	if p, ok := o.upstream.(ParallelMultipartWriter); ok {
		return p.SupportsParallelMultipart()
	}
	return false
}

func (o *objectStorageHTTPTrace) WriteMultipartParallel(ctx context.Context, key string, r io.Reader, sizeHint *int64, opt *ParallelMultipartOption) (err error) {
	ctx = withHTTPTrace(ctx)
	if p, ok := o.upstream.(ParallelMultipartWriter); ok {
		return p.WriteMultipartParallel(ctx, key, r, sizeHint, opt)
	}
	return moerr.NewNotSupportedNoCtx("parallel multipart upload")
}

// Mark an object-store operation without retaining mutable request state. SDK
// retries, multipart parts and lazy iterators get their own trace at RoundTrip.
type httpTraceKey struct{}

func withHTTPTrace(ctx context.Context) context.Context {
	return context.WithValue(ctx, httpTraceKey{}, true)
}
