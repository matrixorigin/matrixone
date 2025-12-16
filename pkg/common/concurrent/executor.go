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

package concurrent

import (
	"context"
	"runtime"

	"golang.org/x/sync/errgroup"
)

type ThreadPoolExecutor struct {
	nthreads int
}

func NewThreadPoolExecutor(nthreads int) ThreadPoolExecutor {
	if nthreads == 0 {
		nthreads = runtime.NumCPU()
	}
	return ThreadPoolExecutor{nthreads: nthreads}
}

func (e ThreadPoolExecutor) Execute(
	ctx context.Context,
	nitems int,
	fn func(ctx context.Context, thread_id int, start, end int) error) (err error) {

	g, ctx := errgroup.WithContext(ctx)
	chunksz := (nitems + e.nthreads - 1) / e.nthreads
	for i := 0; i < e.nthreads; i++ {

		start := i * chunksz
		if start >= nitems {
			break
		}

		end := start + chunksz
		if end > nitems {
			end = nitems
		}

		thread_id := i
		g.Go(func() error {
			if err2 := fn(ctx, thread_id, start, end); err2 != nil {
				return err2
			}

			return nil
		})
	}

	return g.Wait()
}
