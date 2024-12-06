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
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"go.uber.org/zap"
)

type diskObjectStorage struct {
	path            string
	perfCounterSets []*perfcounter.CounterSet
}

func newDiskObjectStorage(
	_ context.Context,
	args ObjectStorageArguments,
	perfCounterSets []*perfcounter.CounterSet,
) (*diskObjectStorage, error) {

	if err := args.validate(); err != nil {
		return nil, err
	}

	logutil.Info("new object storage",
		zap.Any("sdk", "disk"),
		zap.Any("arguments", args),
	)

	path, err := filepath.Abs(args.Bucket)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(path, 0755); err != nil {
		if !os.IsExist(err) {
			return nil, err
		}
	}

	return &diskObjectStorage{
		path:            path,
		perfCounterSets: perfCounterSets,
	}, nil
}

var _ ObjectStorage = new(diskObjectStorage)

func (d *diskObjectStorage) Delete(ctx context.Context, keys ...string) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Delete.Add(1)
	}, d.perfCounterSets...)

	for _, key := range keys {
		path := filepath.Join(d.path, key)
		_ = os.Remove(path)
	}
	return nil
}

func (d *diskObjectStorage) Exists(ctx context.Context, key string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	path := filepath.Join(d.path, key)
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (d *diskObjectStorage) List(
	ctx context.Context,
	prefix string,
) iter.Seq2[*DirEntry, error] {
	return func(yield func(*DirEntry, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(nil, err)
			return
		}

		perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
			counter.FileService.S3.List.Add(1)
		}, d.perfCounterSets...)

		dir, prefix := path.Split(prefix)

		f, err := os.Open(filepath.Join(d.path, dir))
		if err != nil {
			if os.IsNotExist(err) {
				return
			}
			yield(nil, err)
			return
		}
		defer f.Close()

	read:
		for {
			infos, err := f.Readdir(256)

			for _, info := range infos {
				name := info.Name()
				if strings.HasSuffix(name, ".mofstemp") {
					continue
				}
				if !strings.HasPrefix(name, prefix) {
					continue
				}
				if !yield(&DirEntry{
					IsDir: info.IsDir(),
					Name:  path.Join(dir, name),
					Size:  info.Size(),
				}, nil) {
					break read
				}
			}

			if err != nil {
				if err == io.EOF {
					break read
				}
				yield(nil, err)
				return
			}

		}
	}
}

func (d *diskObjectStorage) Read(ctx context.Context, key string, min *int64, max *int64) (r io.ReadCloser, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Get.Add(1)
	}, d.perfCounterSets...)

	path := filepath.Join(d.path, key)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, moerr.NewFileNotFoundNoCtx(key)
		}
		return nil, err
	}

	var reader io.Reader
	reader = f

	if min != nil {
		pos, err := f.Seek(*min, io.SeekStart)
		if err != nil {
			return nil, err
		}
		if pos != *min {
			return nil, moerr.NewEmptyRangeNoCtx(key)
		}
	}

	if max != nil {
		limit := *max
		if min != nil {
			limit -= *min
		}
		if limit <= 0 {
			return nil, moerr.NewEmptyRangeNoCtx(key)
		}
		reader = io.LimitReader(reader, limit)
	}

	return &readCloser{
		r:         reader,
		closeFunc: f.Close,
	}, nil
}

func (d *diskObjectStorage) Stat(ctx context.Context, key string) (size int64, err error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Head.Add(1)
	}, d.perfCounterSets...)

	path := filepath.Join(d.path, key)
	stat, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, moerr.NewFileNotFoundNoCtx(key)
		}
		return 0, err
	}

	size = stat.Size()

	return
}

func (d *diskObjectStorage) Write(ctx context.Context, key string, r io.Reader, sizeHint *int64, expire *time.Time) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Put.Add(1)
	}, d.perfCounterSets...)

	tempFile, err := os.CreateTemp(d.path, "*.mofstemp")
	if err != nil {
		return err
	}

	n, err := io.Copy(
		tempFile,
		r,
	)
	if err != nil {
		return err
	}

	if sizeHint != nil && n != *sizeHint {
		return moerr.NewSizeNotMatchNoCtx(key)
	}

	if err := tempFile.Close(); err != nil {
		return err
	}

	path := filepath.Join(d.path, key)
	err = os.MkdirAll(filepath.Dir(path), 0755)
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}

	if err := os.Rename(tempFile.Name(), path); err != nil {
		return err
	}

	return nil
}
