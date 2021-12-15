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

package times

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCalcuteCount(t *testing.T) {
	z := int64(10)
	vzs := [][]int64{
		{1, 2, 3},
		{2, 3, 4},
		{5, 6, 7},
	}
	vsels := [][]int64{
		{1, 2, 0},
		{0, 1, 1},
		{1, 2, 0},
	}

	expected := []int64{
		240, 280, 200, 360, 420, 300, 360, 420, 300,
		360, 420, 300, 540, 630, 450, 540, 630, 450,
		120, 140, 100, 180, 210, 150, 180, 210, 150,
	}

	ctr := Container{}
	ctr.calculateCount(z, vzs, vsels)
	require.EqualValues(t, ctr.zs, expected)
}
