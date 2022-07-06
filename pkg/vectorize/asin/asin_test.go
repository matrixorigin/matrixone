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

package asin

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/stretchr/testify/require"
)

func TestAsin(t *testing.T) {
	//Test values
	nums := []float64{0, 1}
	//Predefined Correct Values
	asinNums := []float64{0, 1.5707963267948966}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run asin function
	AsinResult := Asin(nums, newNums)
	require.Equal(t, nulls.Any(AsinResult.Nsp), false)

	for i := range AsinResult.Result {
		require.Equal(t, asinNums[i], AsinResult.Result[i])
	}
}
