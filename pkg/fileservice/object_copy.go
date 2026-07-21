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

package fileservice

import (
	"context"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

const DefaultObjectCopyConcurrency = 16

type ObjectCopy struct {
	SourcePath      string
	DestinationPath string
}

type ObjectCopyResult struct {
	Copied bool
}

type ObjectCopyOptions struct {
	// Concurrency defaults to DefaultObjectCopyConcurrency when it is not
	// positive.
	Concurrency int
}

// ObjectBatchCopier is an optional provider-native batch-copy capability.
// Backends without it use concurrent ObjectCopier calls.
type ObjectBatchCopier interface {
	CopyObjects(
		ctx context.Context,
		srcFS FileService,
		copies []ObjectCopy,
		options ObjectCopyOptions,
	) ([]ObjectCopyResult, error)
}

// CopyObjects attempts provider-side copies with bounded concurrency. Results
// correspond one-to-one with copies. A false Copied result means that the
// destination does not support provider-side copy for that object and the
// caller may use a streaming fallback.
func CopyObjects(
	ctx context.Context,
	srcFS, dstFS FileService,
	copies []ObjectCopy,
	options ObjectCopyOptions,
) ([]ObjectCopyResult, error) {
	results := make([]ObjectCopyResult, len(copies))
	if len(copies) == 0 {
		return results, nil
	}
	if batchCopier, ok := dstFS.(ObjectBatchCopier); ok {
		return batchCopier.CopyObjects(ctx, srcFS, copies, options)
	}
	copier, ok := dstFS.(ObjectCopier)
	if !ok {
		return results, nil
	}
	concurrency := options.Concurrency
	if concurrency <= 0 {
		concurrency = DefaultObjectCopyConcurrency
	}
	concurrency = min(concurrency, len(copies))

	var next atomic.Uint64
	group, copyCtx := errgroup.WithContext(ctx)
	for range concurrency {
		group.Go(func() error {
			for {
				if err := copyCtx.Err(); err != nil {
					return err
				}
				i := int(next.Add(1) - 1)
				if i >= len(copies) {
					return nil
				}
				copied, err := copier.CopyObject(
					copyCtx,
					srcFS,
					copies[i].SourcePath,
					copies[i].DestinationPath,
				)
				if err != nil {
					return err
				}
				results[i].Copied = copied
			}
		})
	}
	return results, group.Wait()
}
