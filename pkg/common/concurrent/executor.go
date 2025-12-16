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
	"errors"
	"runtime"
	"sync"
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
	fn func(ctx context.Context, thread_id int, item_id int) error) (err error) {

	var wg sync.WaitGroup
	errs := make(chan error, e.nthreads)

	for i := 0; i < e.nthreads; i++ {
		wg.Add(1)
		go func(thread_id int) {
			defer wg.Done()

			for j := thread_id; j < nitems; j += e.nthreads {

				if ctx.Err() != nil {
					// context is cancelled or deadline exceeded
					errs <- ctx.Err()
					return
				}

				err2 := fn(ctx, thread_id, j)
				if err2 != nil {
					errs <- err2
					return
				}
			}

		}(i)
	}

	wg.Wait()

	if len(errs) > 0 {
		for i := 0; i < len(errs); i++ {
			err = errors.Join(err, <-errs)
		}
	}
	return

}
