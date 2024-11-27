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
	"net/http/httptrace"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
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

func (o *objectStorageHTTPTrace) Delete(ctx context.Context, keys ...string) (err error) {
	traceInfo := o.newTraceInfo()
	defer o.closeTraceInfo(traceInfo)
	ctx = httptrace.WithClientTrace(ctx, traceInfo.trace)
	return o.upstream.Delete(ctx, keys...)
}

func (o *objectStorageHTTPTrace) Exists(ctx context.Context, key string) (bool, error) {
	traceInfo := o.newTraceInfo()
	defer o.closeTraceInfo(traceInfo)
	ctx = httptrace.WithClientTrace(ctx, traceInfo.trace)
	return o.upstream.Exists(ctx, key)
}

func (o *objectStorageHTTPTrace) List(ctx context.Context, prefix string) iter.Seq2[*DirEntry, error] {
	traceInfo := o.newTraceInfo()
	defer o.closeTraceInfo(traceInfo)
	ctx = httptrace.WithClientTrace(ctx, traceInfo.trace)
	return o.upstream.List(ctx, prefix)
}

func (o *objectStorageHTTPTrace) Read(ctx context.Context, key string, min *int64, max *int64) (r io.ReadCloser, err error) {
	traceInfo := o.newTraceInfo()
	defer o.closeTraceInfo(traceInfo)
	ctx = httptrace.WithClientTrace(ctx, traceInfo.trace)
	return o.upstream.Read(ctx, key, min, max)
}

func (o *objectStorageHTTPTrace) Stat(ctx context.Context, key string) (size int64, err error) {
	traceInfo := o.newTraceInfo()
	defer o.closeTraceInfo(traceInfo)
	ctx = httptrace.WithClientTrace(ctx, traceInfo.trace)
	return o.upstream.Stat(ctx, key)
}

func (o *objectStorageHTTPTrace) Write(ctx context.Context, key string, r io.Reader, sizeHint *int64, expire *time.Time) (err error) {
	traceInfo := o.newTraceInfo()
	defer o.closeTraceInfo(traceInfo)
	ctx = httptrace.WithClientTrace(ctx, traceInfo.trace)
	return o.upstream.Write(ctx, key, r, sizeHint, expire)
}

func (o *objectStorageHTTPTrace) newTraceInfo() *traceInfo {
	return reuse.Alloc[traceInfo](nil)
}

func (o *objectStorageHTTPTrace) closeTraceInfo(info *traceInfo) {
	reuse.Free(info, nil)
}
