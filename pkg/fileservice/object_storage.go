// Copyright 2023 Matrix Origin
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
	"runtime"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
)

const smallObjectThreshold = 64 * (1 << 20)
const (
	// defaultParallelMultipartPartSize defines the default per-part size for parallel multipart uploads.
	defaultParallelMultipartPartSize = 64 * (1 << 20)
	// minMultipartPartSize is the minimum allowed part size for S3-compatible multipart uploads.
	minMultipartPartSize = 5 * (1 << 20)
	// maxMultipartPartSize is the maximum allowed part size for S3-compatible multipart uploads.
	maxMultipartPartSize = 5 * (1 << 30)
	// maxMultipartParts is the maximum allowed parts for S3-compatible multipart uploads.
	maxMultipartParts = 10000
)

var (
	parallelUploadPoolOnce sync.Once
	parallelUploadPool     *ants.Pool
)

func getParallelUploadPool() *ants.Pool {
	parallelUploadPoolOnce.Do(func() {
		pool, err := ants.NewPool(runtime.NumCPU())
		if err != nil {
			panic(err)
		}
		parallelUploadPool = pool
	})
	return parallelUploadPool
}

func normalizeParallelOption(opt *ParallelMultipartOption) ParallelMultipartOption {
	res := ParallelMultipartOption{}
	if opt != nil {
		res = *opt
	}
	if res.PartSize <= 0 {
		res.PartSize = defaultParallelMultipartPartSize
	}
	if res.PartSize < minMultipartPartSize {
		res.PartSize = minMultipartPartSize
	}
	if res.PartSize > maxMultipartPartSize {
		res.PartSize = maxMultipartPartSize
	}
	if res.Concurrency <= 0 {
		res.Concurrency = runtime.NumCPU()
	}
	if res.Concurrency < 1 {
		res.Concurrency = 1
	}
	return res
}

type ObjectStorage interface {
	// List lists objects with specified prefix
	List(
		ctx context.Context,
		prefix string,
	) iter.Seq2[*DirEntry, error]

	// Stat returns informations about an object
	Stat(
		ctx context.Context,
		key string,
	) (
		size int64,
		err error,
	)

	// Exists reports whether specified object exists
	Exists(
		ctx context.Context,
		key string,
	) (
		bool,
		error,
	)

	// Write writes an object
	Write(
		ctx context.Context,
		key string,
		r io.Reader,
		sizeHint *int64,
		expire *time.Time,
	) (
		err error,
	)

	// Read returns an io.Reader for specified object range
	Read(
		ctx context.Context,
		key string,
		min *int64,
		max *int64,
	) (
		r io.ReadCloser,
		err error,
	)

	// Delete deletes objects
	Delete(
		ctx context.Context,
		keys ...string,
	) (
		err error,
	)
}

// ParallelMultipartWriter is implemented by storages that support parallel multipart uploads.
type ParallelMultipartWriter interface {
	SupportsParallelMultipart() bool
	WriteMultipartParallel(
		ctx context.Context,
		key string,
		r io.Reader,
		sizeHint *int64,
		opt *ParallelMultipartOption,
	) error
}

// ParallelMultipartOption controls part size and parallelism of multipart uploads.
type ParallelMultipartOption struct {
	// PartSize configures each part size; defaults to 64MB if zero.
	PartSize int64
	// Concurrency configures worker count; defaults to runtime.NumCPU() if zero.
	Concurrency int
	// Expire sets object expiration.
	Expire *time.Time
}
