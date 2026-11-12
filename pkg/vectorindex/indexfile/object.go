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
	"errors"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

// OpenObject returns the object file of fs at object, of size bytes or 0 when unknown:
// from the index-file cache when present, otherwise read whole from fs and cached. The
// entry is evicted by the cache like any index file; the returned File holds it until
// Close.
func OpenObject(ctx context.Context, cache *fileservice.IndexFileCache, fs fileservice.FileService, object string, size int64) (*File, error) {
	name := fileservice.IndexObjectEntry(object)
	if f, err := openObjectCached(ctx, cache, name, size); f != nil || err != nil {
		return f, err
	}
	unlock, err := rebuildLocks.lock(ctx, cache.Path()+"\x00"+name)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if f, err := openObjectCached(ctx, cache, name, size); f != nil || err != nil {
		return f, err
	}

	if size <= 0 {
		entry, err := fs.StatFile(ctx, object)
		if err != nil {
			return nil, err
		}
		size = entry.Size
	}
	tmp, remove, err := cache.CreateTempFile()
	if err != nil {
		return nil, err
	}
	defer remove()
	if err := readObject(ctx, fs, object, size, tmp); err != nil {
		_ = tmp.Close()
		return nil, err
	}
	if _, err := cache.Put(ctx, name, []fileservice.IndexCacheFile{{Name: fileservice.IndexObjectFile, Path: tmp.Name()}}); err != nil {
		_ = tmp.Close()
		return nil, err
	}
	if f, err := openObjectCached(ctx, cache, name, size); f != nil || err != nil {
		_ = tmp.Close()
		return f, err
	}
	// Not cached (the cache write failed or the entry was evicted at once): serve the
	// read file, which stays readable through tmp after remove.
	return &File{f: tmp, size: size}, nil
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

// readObject writes the size bytes of the object of fs at object to dst, reading the
// ranges fileservice.IndexObjectDownload splits it into at once.
func readObject(ctx context.Context, fs fileservice.FileService, object string, size int64, dst io.WriterAt) (err error) {
	bounds, end := fileservice.IndexObjectDownload(fs, size)
	defer func() { end(err == nil) }()
	if bounds == nil {
		bounds = []int64{0, size}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make(chan error, len(bounds)-1)
	n := 0
	for i := 0; i+1 < len(bounds); i++ {
		if bounds[i] >= bounds[i+1] {
			continue
		}
		n++
		go func(start, end int64) {
			err := readObjectRange(ctx, fs, object, start, end, dst)
			if err != nil {
				cancel()
			}
			errs <- err
		}(bounds[i], bounds[i+1])
	}
	for range n {
		err = errors.Join(err, <-errs)
	}
	return err
}

// readObjectRange writes the bytes [start, end) of the object to dst at start.
func readObjectRange(ctx context.Context, fs fileservice.FileService, object string, start, end int64, dst io.WriterAt) error {
	var r io.ReadCloser
	vec := &fileservice.IOVector{
		FilePath: object,
		Policy:   fileservice.SkipAllCache,
		Entries:  []fileservice.IOEntry{{Offset: start, Size: end - start, ReadCloserForRead: &r}},
	}
	if err := fs.Read(ctx, vec); err != nil {
		return err
	}
	defer vec.Release()
	defer r.Close()
	n, err := io.Copy(io.NewOffsetWriter(dst, start), r)
	if err != nil {
		return err
	}
	if n != end-start {
		return io.ErrUnexpectedEOF
	}
	return nil
}
