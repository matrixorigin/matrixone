// Copyright 2022 Matrix Origin
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

import "context"

type DiskCacheCallbacks struct {
	OnWritten []OnDiskCacheWrittenFunc
	OnEvict   []OnDiskCacheEvictFunc
}

type OnDiskCacheWrittenFunc = func(
	filePath string,
	entry IOEntry,
)

type OnDiskCacheEvictFunc = func(
	diskFilePath string,
)

type ctxKeyDiskCacheCallbacks struct{}

var CtxKeyDiskCacheCallbacks ctxKeyDiskCacheCallbacks

func OnDiskCacheWritten(ctx context.Context, fn OnDiskCacheWrittenFunc) (ret context.Context) {
	var callbacks *DiskCacheCallbacks
	v := ctx.Value(CtxKeyDiskCacheCallbacks)
	if v == nil {
		callbacks = new(DiskCacheCallbacks)
		ret = context.WithValue(ctx, CtxKeyDiskCacheCallbacks, callbacks)
	} else {
		callbacks = v.(*DiskCacheCallbacks)
		ret = ctx
	}
	callbacks.OnWritten = append(callbacks.OnWritten, fn)
	return
}

func OnDiskCacheEvict(ctx context.Context, fn OnDiskCacheEvictFunc) (ret context.Context) {
	var callbacks *DiskCacheCallbacks
	v := ctx.Value(CtxKeyDiskCacheCallbacks)
	if v == nil {
		callbacks = new(DiskCacheCallbacks)
		ret = context.WithValue(ctx, CtxKeyDiskCacheCallbacks, callbacks)
	} else {
		callbacks = v.(*DiskCacheCallbacks)
		ret = ctx
	}
	callbacks.OnEvict = append(callbacks.OnEvict, fn)
	return
}
