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

package indexfile

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

// OpenCachedObject opens, held, the object at object when cache holds it, and returns nil
// when it does not. A read of the object downloads it whole into the cache; the returned
// File holds the entry until Close. Only cancellation is an error.
func OpenCachedObject(ctx context.Context, cache *fileservice.IndexFileCache, object string) (*File, error) {
	return openObjectCached(ctx, cache, fileservice.IndexObjectEntry(object), 0)
}

// openObjectCached opens entry name held. A missing entry, or one of another size than a
// known size, is a miss (nil, nil); the latter is deleted. Only cancellation is an error.
func openObjectCached(ctx context.Context, cache *fileservice.IndexFileCache, name string, size int64) (*File, error) {
	hold, err := cache.Hold(name)
	if err != nil {
		return nil, err
	}
	files, err := cache.Open(ctx, name, fileservice.IndexObjectFile)
	if err != nil {
		hold.Release()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			logutil.Warn("index file cache open failed", zap.String("entry", name), zap.Error(err))
		}
		return nil, nil
	}
	data := files[0]
	info, err := data.Stat()
	if err == nil && (size <= 0 || info.Size() == size) {
		return &File{f: data, size: info.Size(), hold: hold}, nil
	}
	_ = data.Close()
	hold.Release()
	logutil.Warn("index file cache object entry invalid, reading it again", zap.String("entry", name), zap.Error(err))
	if delErr := cache.Delete(ctx, name); delErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
	}
	return nil, nil
}
