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
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

var deleteTimeout = 10 * time.Minute
var deleteBatchSize = 1000
var deleteWorkerNum = 4

func SetDeleteBatchSize(cnt int) {
	deleteBatchSize = cnt
}
func SetDeleteTimeout(duration time.Duration) {
	deleteTimeout = duration
}
func SetDeleteWorkerNum(num int) {
	if num > 0 {
		deleteWorkerNum = num
	}
}

type Deleter struct {
	// toDeletePaths is list of files that can be GC
	toDeletePaths   []string
	fs              fileservice.FileService
	deleteTimeout   time.Duration
	deleteBatchSize int
	workerNum       int
}

func NewDeleter(fs fileservice.FileService) *Deleter {
	w := &Deleter{
		fs:              fs,
		deleteTimeout:   deleteTimeout,
		deleteBatchSize: deleteBatchSize,
		workerNum:       deleteWorkerNum,
	}
	logutil.Info(
		"GC-Worker-Init",
		zap.Duration("delete-timeout", w.deleteTimeout),
		zap.Int("delete-batch-size", w.deleteBatchSize),
		zap.Int("delete-worker-num", w.workerNum),
	)
	return w
}

// deleteBatch deletes a batch of files and returns error if any
func (g *Deleter) deleteBatch(
	ctx context.Context,
	taskName string,
	paths []string,
) error {
	now := time.Now()
	deleteCtx, cancel := context.WithTimeout(ctx, g.deleteTimeout)
	defer cancel()

	err := g.fs.Delete(deleteCtx, paths...)
	logutil.Info(
		"GC-ExecDelete-Batch",
		zap.String("task", taskName),
		zap.Strings("paths", paths),
		zap.Int("cnt", len(paths)),
		zap.Duration("duration", time.Since(now)),
		zap.Error(err),
	)
	return err
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
		zap.Int("worker-num", g.workerNum),
	)

	toDeletePaths := g.toDeletePaths

	// Split paths into batches
	var batches [][]string
	for i := 0; i < cnt; i += g.deleteBatchSize {
		end := i + g.deleteBatchSize
		if end > cnt {
			end = cnt
		}
		batches = append(batches, toDeletePaths[i:end])
	}

	// Use concurrent workers to delete batches
	if g.workerNum <= 1 || len(batches) <= 1 {
		// Single worker mode (original behavior)
		for _, batch := range batches {
			select {
			case <-ctx.Done():
				err = context.Cause(ctx)
				return
			default:
			}
			if deleteErr := g.deleteBatch(ctx, taskName, batch); deleteErr != nil {
				if !moerr.IsMoErrCode(deleteErr, moerr.ErrFileNotFound) {
					err = deleteErr
					return
				}
			}
		}
	} else {
		// Concurrent worker mode
		var wg sync.WaitGroup
		errCh := make(chan error, len(batches))
		semaphore := make(chan struct{}, g.workerNum)

		// Use a separate context to allow cancellation of pending goroutines
		workerCtx, cancelWorkers := context.WithCancel(ctx)
		defer cancelWorkers()

		for _, batch := range batches {
			// Try to acquire semaphore or check context cancellation
			select {
			case <-ctx.Done():
				err = context.Cause(ctx)
				// Wait for already started goroutines to finish
				wg.Wait()
				return
			case semaphore <- struct{}{}: // Acquire semaphore
				// Got semaphore, continue
			}

			wg.Add(1)

			go func(paths []string) {
				defer wg.Done()
				defer func() { <-semaphore }() // Release semaphore

				if deleteErr := g.deleteBatch(workerCtx, taskName, paths); deleteErr != nil {
					if !moerr.IsMoErrCode(deleteErr, moerr.ErrFileNotFound) {
						errCh <- deleteErr
					}
				}
			}(batch)
		}

		wg.Wait()
		close(errCh)

		// Check for errors
		for e := range errCh {
			if e != nil {
				err = e
				return
			}
		}
	}

	if cap(g.toDeletePaths) > 5000 {
		g.toDeletePaths = make([]string, 0, 1000)
	} else {
		g.toDeletePaths = g.toDeletePaths[:0]
	}
	return
}
