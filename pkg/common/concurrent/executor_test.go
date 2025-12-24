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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExecutor(t *testing.T) {

	ctx := context.Background()
	nthreads := 3
	vec := make([]int, 1024)
	answer := 0
	for i := range vec {
		vec[i] = i
		answer += i
	}

	e := NewThreadPoolExecutor(nthreads)

	r := make([]int, nthreads)

	err := e.Execute(ctx, len(vec), func(ctx context.Context, thread_id int, start, end int) error {
		subSlice := vec[start:end]
		for j := range subSlice {
			if j%100 == 0 && ctx.Err() != nil {
				return ctx.Err()
			}

			r[thread_id] += subSlice[j]
		}
		return nil
	})

	require.NoError(t, err)

	sum := 0
	for _, v := range r {
		sum += v
	}

	require.Equal(t, sum, answer)
}
