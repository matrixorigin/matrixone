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

import (
	"context"
	"flag"
	"os"
	"path/filepath"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"go.uber.org/zap"
)

var fixMissingFlag = flag.Bool(
	"fs-fix-missing",
	false,
	"indicates file services to try their best to fix missing files",
)

var fixMissingFromEnv = os.Getenv("MO_FS_FIX_MISSING") != ""

func (s *S3FS) restoreFromDiskCache(ctx context.Context) {
	if s.diskCache == nil {
		return
	}
	cache := s.diskCache
	logutil.Info("restore S3FS from disk cache",
		zap.Any("fs name", s.Name()),
		zap.Any("cache dir", cache.path),
	)

	counterSet := new(perfcounter.CounterSet)
	ctx = perfcounter.WithCounterSet(ctx, counterSet)
	numS3Write := counterSet.FileService.S3.Put.Load()

	err := filepath.WalkDir(cache.path, func(diskPath string, entry os.DirEntry, err error) error {

		// ignore error
		if err != nil {
			logutil.Info("restore from disk cache error",
				zap.Any("do", "WalkDir entry"),
				zap.Any("error", err),
			)
			return nil
		}

		// ignore entry
		if entry.IsDir() {
			return nil
		}

		path, err := cache.decodeFilePath(diskPath)
		if err != nil {
			logutil.Info("restore from disk cache error",
				zap.Any("do", "decode file path"),
				zap.Any("error", err),
			)
			// ignore
			return nil
		}

		f, err := os.Open(diskPath)
		if err != nil {
			logutil.Info("restore from disk cache error",
				zap.Any("do", "open file"),
				zap.Any("error", err),
			)
			return nil
		}
		defer f.Close()

		err = s.Write(ctx, IOVector{
			FilePath: path,
			Entries: []IOEntry{
				{
					Size:           -1,
					ReaderForWrite: f,
				},
			},
		})
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists) {
				// already exists
				return nil
			}
			logutil.Info("restore from disk cache error",
				zap.Any("do", "write"),
				zap.Any("error", err),
			)
			return nil
		}

		n := counterSet.FileService.S3.Put.Load()
		if n > numS3Write {
			logutil.Info("restore from disk cache",
				zap.Any("path", path),
			)
			numS3Write = n
		}

		return nil
	})
	if err != nil {
		logutil.Info("restore from disk cache error",
			zap.Any("do", "WalkDir"),
			zap.Any("error", err),
		)
	}

}
