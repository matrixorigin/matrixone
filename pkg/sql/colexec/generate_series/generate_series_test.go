// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package generate_series

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDoGenerateInt(t *testing.T) {
	kases := []struct {
		start int
		end   int
		step  int
		res   []int
		err   bool
	}{
		{
			start: 1,
			end:   10,
			step:  1,
			res:   []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
		{
			start: 1,
			end:   10,
			step:  2,
			res:   []int{1, 3, 5, 7, 9},
		},
		{
			start: 1,
			end:   10,
			step:  3,
			res:   []int{1, 4, 7, 10},
		},
		{
			start: 1,
			end:   10,
			step:  4,
			res:   []int{1, 5, 9},
		},
		{
			start: 1,
			end:   10,
			step:  -1,
			err:   true,
		},
		{
			start: 10,
			end:   1,
			step:  -1,
			res:   []int{10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		},
		{
			start: 10,
			end:   1,
			step:  -2,
			res:   []int{10, 8, 6, 4, 2},
		},
		{
			start: 10,
			end:   1,
			step:  1,
			err:   true,
		},
		{
			start: 1,
			end:   10,
			step:  0,
			err:   true,
		},
		{
			start: 1,
			end:   1,
			step:  0,
			err:   true,
		},
		{
			start: 1,
			end:   10,
			step:  -1,
			err:   true,
		},
		{
			start: 1,
			end:   1,
			step:  1,
			res:   []int{1},
		},
	}
	for _, kase := range kases {
		res, err := doGenerateInt(kase.start, kase.end, kase.step)
		if kase.err {
			require.NotNil(t, err)
			continue
		}
		require.Nil(t, err)
		require.Equal(t, kase.res, res)
	}
}
