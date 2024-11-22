// Copyright 2021 Matrix Origin
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

package gc

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

var deleteTimeout = 10 * time.Minute
var deleteBatchSize = 1000

func SetDeleteBatchSize(cnt int) {
	deleteBatchSize = cnt
}
func SetDeleteTimeout(duration time.Duration) {
	deleteTimeout = duration
}

type Deleter struct {
	// toDeletePaths is list of files that can be GC
	toDeletePaths   []string
	fs              *objectio.ObjectFS
	deleteTimeout   time.Duration
	deleteBatchSize int
}

func NewDeleter(fs *objectio.ObjectFS) *Deleter {
	w := &Deleter{
		fs:              fs,
		deleteTimeout:   deleteTimeout,
		deleteBatchSize: deleteBatchSize,
	}
	logutil.Info(
		"GC-Worker-Init",
		zap.Duration("delete-timeout", w.deleteTimeout),
		zap.Int("delete-batch-size", w.deleteBatchSize),
	)
	return w
}

func (g *Deleter) DeleteMany(
	ctx context.Context,
	taskName string,
	paths []string,
) (err error) {
	beforeCnt := len(g.toDeletePaths)
	g.toDeletePaths = append(g.toDeletePaths, paths...)

	if len(g.toDeletePaths) == 0 {
		return
	}
	cnt := len(g.toDeletePaths)

	now := time.Now()
	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		logger(
			"GC-ExecDelete-Done",
			zap.String("task", taskName),
			zap.Error(err),
			zap.Duration("duration", time.Since(now)),
		)
	}()
	logutil.Info(
		"GC-ExecDelete-Start",
		zap.String("task", taskName),
		zap.Int("before-cnt", beforeCnt),
		zap.Int("cnt", cnt),
	)

	toDeletePaths := g.toDeletePaths

	for i := 0; i < cnt; i += g.deleteBatchSize {
		end := i + g.deleteBatchSize
		if end > cnt {
			end = cnt
		}
		now := time.Now()
		deleteCtx, cancel := context.WithTimeout(ctx, g.deleteTimeout)
		defer cancel()
		err = g.fs.DelFiles(deleteCtx, toDeletePaths[i:end])
		logutil.Info(
			"GC-ExecDelete-Group",
			zap.String("task", taskName),
			zap.Strings("paths", toDeletePaths[i:end]),
			zap.Duration("duration", time.Since(now)),
			zap.Int("left", cnt-end),
			zap.Int("cnt", end-i),
			zap.Error(err),
		)
		if err != nil && !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return
		}
		err = nil
		g.toDeletePaths = toDeletePaths[end:]
	}

	if cap(g.toDeletePaths) > 5000 {
		g.toDeletePaths = make([]string, 0, 1000)
	} else {
		g.toDeletePaths = g.toDeletePaths[:0]
	}
	return
}
