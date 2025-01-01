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
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/colinmarc/hdfs/v2"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"go.uber.org/zap"
)

type HDFS struct {
	name            string
	client          *hdfs.Client
	perfCounterSets []*perfcounter.CounterSet
	keyPrefix       string
}

func NewHDFS(
	ctx context.Context,
	args ObjectStorageArguments,
	perfCounterSets []*perfcounter.CounterSet,
) (*HDFS, error) {

	if err := args.validate(); err != nil {
		return nil, err
	}

	u, err := url.Parse(args.Endpoint)
	if err != nil {
		return nil, err
	}

	client, err := hdfs.NewClient(hdfs.ClientOptions{
		Addresses: []string{
			u.Host,
		},
		User:             firstNonZero(args.User, "hadoop"),
		NamenodeDialFunc: httpDialer.DialContext,
		DatanodeDialFunc: httpDialer.DialContext,
	})
	if err != nil {
		return nil, err
	}

	logutil.Info("new object storage",
		zap.Any("sdk", "hdfs"),
		zap.Any("arguments", args),
	)

	return &HDFS{
		name:            args.Name,
		client:          client,
		perfCounterSets: perfCounterSets,
		keyPrefix:       path.Clean(args.KeyPrefix),
	}, nil
}

var _ ObjectStorage = new(HDFS)

func (h *HDFS) Delete(ctx context.Context, keys ...string) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Delete.Add(1)
	}, h.perfCounterSets...)

	for _, key := range keys {
		_, err := DoWithRetry("HDFS: delete", func() (bool, error) {
			return true, h.client.Remove(h.keyToPath(key))
		}, maxRetryAttemps, IsRetryableError)
		if err != nil {
			logutil.Warn("HDFS: delete file", zap.Error(err))
		}
	}

	return nil
}

func (h *HDFS) keyToPath(key string) string {
	return path.Join(h.keyPrefix, key)
}

func (h *HDFS) Exists(ctx context.Context, key string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Head.Add(1)
	}, h.perfCounterSets...)

	_, err := h.client.Stat(h.keyToPath(key))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (h *HDFS) List(ctx context.Context, prefix string) iter.Seq2[*DirEntry, error] {
	return func(yield func(*DirEntry, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(nil, err)
			return
		}

		perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
			counter.FileService.S3.List.Add(1)
		}, h.perfCounterSets...)

		dir, prefix := path.Split(prefix)

		f, err := h.client.Open(h.keyToPath(dir))
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

func (h *HDFS) Read(ctx context.Context, key string, min *int64, max *int64) (r io.ReadCloser, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Get.Add(1)
	}, h.perfCounterSets...)

	f, err := h.client.Open(h.keyToPath(key))
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

func (h *HDFS) Stat(ctx context.Context, key string) (size int64, err error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Head.Add(1)
	}, h.perfCounterSets...)

	stat, err := h.client.Stat(h.keyToPath(key))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, moerr.NewFileNotFoundNoCtx(key)
		}
		return 0, err
	}

	size = stat.Size()

	return
}

func (h *HDFS) Write(ctx context.Context, key string, r io.Reader, sizeHint *int64, expire *time.Time) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Put.Add(1)
	}, h.perfCounterSets...)

	tempFilePath := h.keyToPath(key) + ".mofstemp"
	tempFile, err := h.client.Create(tempFilePath)
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

	filePath := h.keyToPath(key)
	err = h.client.MkdirAll(path.Dir(filePath), 0755)
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}

	if err := os.Rename(tempFilePath, filePath); err != nil {
		return err
	}

	return nil
}
