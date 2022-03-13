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

package power

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPower(t *testing.T) {
	//Test values
	lvs := []float64{2, 3, 4, 5, 6, 7, 8, 9}
	rvs := []float64{2, 2, 3, 3, 4, 4, 2, 2}

	//Predefined Correct Values
	expected := []float64{4, 9, 64, 125, 1296, 2401, 64, 81}

	newNums := powerPure(lvs, rvs, rvs)

	for i, _ := range newNums {
		require.Equal(t, expected[i], newNums[i])
	}
}

func TestPowerScalarLeftConst(t *testing.T) {
	//Test values
	var lc float64 = 2
	rvs := []float64{2, 3, 4, 5, 6, 7, 8, 9}

	//Predefined Correct Values
	expected := []float64{4, 8, 16, 32, 64, 128, 256, 512}

	newNums := powerScalarLeftConstPure(lc, rvs, rvs)

	for i, _ := range newNums {
		require.Equal(t, expected[i], newNums[i])
	}
}

func TestPowerScalarRightConst(t *testing.T) {
	//Test values
	lvs := []float64{2, 3, 4, 5, 6, 7, 8, 9}
	var rc float64 = 2

	//Predefined Correct Values
	expected := []float64{4, 9, 16, 25, 36, 49, 64, 81}

	newNums := powerScalarRightConstPure(rc, lvs, lvs)

	for i, _ := range newNums {
		require.Equal(t, expected[i], newNums[i])
	}
}
