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
	"cmp"
	"context"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
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

func (d *diskObjectStorage) List(ctx context.Context, prefix string, fn func(isPrefix bool, key string, size int64) (bool, error)) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	dir, prefix := path.Split(prefix)

	f, err := os.Open(filepath.Join(d.path, dir))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	infos, err := f.Readdir(-1)
	if err != nil {
		return err
	}
	slices.SortFunc(infos, func(a, b fs.FileInfo) int {
		return cmp.Compare(a.Name(), b.Name())
	})

	for _, info := range infos {
		name := info.Name()
		if strings.HasSuffix(name, ".mofstemp") {
			continue
		}
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		if more, err := fn(info.IsDir(), path.Join(dir, name), info.Size()); err != nil {
			return err
		} else if !more {
			break
		}
	}

	return nil
}

func (d *diskObjectStorage) Read(ctx context.Context, key string, min *int64, max *int64) (r io.ReadCloser, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

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

func (d *diskObjectStorage) Write(ctx context.Context, key string, r io.Reader, size int64, expire *time.Time) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	tempFile, err := os.CreateTemp(d.path, "*.mofstemp")
	if err != nil {
		return err
	}

	_, err = io.Copy(
		tempFile,
		r,
	)
	if err != nil {
		return err
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
