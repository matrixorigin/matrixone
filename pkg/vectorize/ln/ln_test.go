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

package ln

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLnUint8(t *testing.T) {
	//Test values
	nums := []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	//Predefined Correct Values
	expect := []float64{0, 0.6931471805599453, 1.0986122886681096,
		1.3862943611198906, 1.6094379124341003, 1.791759469228055,
		1.9459101490553132, 2.0794415416798357, 2.1972245773362196,
		2.302585092994046,
	}

	//Init a new variable
	actual := make([]float64, len(nums))
	//Run ln function
	lnResult := LnUint8(nums, actual)
	require.Equal(t, nulls.Any(lnResult.Nsp), false)

	for i := range actual {
		require.Equal(t, expect[i], actual[i])
	}
}

func TestLnUint16(t *testing.T) {
	//Test values
	nums := []uint16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	//Predefined Correct Values
	expect := []float64{0, 0.6931471805599453, 1.0986122886681096,
		1.3862943611198906, 1.6094379124341003, 1.791759469228055,
		1.9459101490553132, 2.0794415416798357, 2.1972245773362196,
		2.302585092994046,
	}

	//Init a new variable
	actual := make([]float64, len(nums))
	//Run ln function
	lnResult := LnUint16(nums, actual)
	require.Equal(t, nulls.Any(lnResult.Nsp), false)

	for i := range actual {
		require.Equal(t, expect[i], actual[i])
	}
}

func TestLnUint32(t *testing.T) {
	//Test values
	nums := []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	//Predefined Correct Values
	expect := []float64{0, 0.6931471805599453, 1.0986122886681096,
		1.3862943611198906, 1.6094379124341003, 1.791759469228055,
		1.9459101490553132, 2.0794415416798357, 2.1972245773362196,
		2.302585092994046,
	}

	//Init a new variable
	actual := make([]float64, len(nums))
	//Run ln function
	lnResult := LnUint32(nums, actual)
	require.Equal(t, nulls.Any(lnResult.Nsp), false)

	for i := range actual {
		require.Equal(t, expect[i], actual[i])
	}
}

func TestLnUint64(t *testing.T) {
	//Test values
	nums := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	//Predefined Correct Values
	expect := []float64{0, 0.6931471805599453, 1.0986122886681096,
		1.3862943611198906, 1.6094379124341003, 1.791759469228055,
		1.9459101490553132, 2.0794415416798357, 2.1972245773362196,
		2.302585092994046,
	}

	//Init a new variable
	actual := make([]float64, len(nums))
	//Run ln function
	lnResult := LnUint64(nums, actual)
	require.Equal(t, nulls.Any(lnResult.Nsp), false)

	for i := range actual {
		require.Equal(t, expect[i], actual[i])
	}
}

func TestLnInt8(t *testing.T) {
	//Test values
	nums := []int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	//Predefined Correct Values
	expect := []float64{0, 0.6931471805599453, 1.0986122886681096,
		1.3862943611198906, 1.6094379124341003, 1.791759469228055,
		1.9459101490553132, 2.0794415416798357, 2.1972245773362196,
		2.302585092994046,
	}

	//Init a new variable
	actual := make([]float64, len(nums))
	//Run ln function
	lnResult := LnInt8(nums, actual)
	require.Equal(t, nulls.Any(lnResult.Nsp), false)

	for i := range actual {
		require.Equal(t, expect[i], actual[i])
	}
}

func TestLnInt16(t *testing.T) {
	//Test values
	nums := []int16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	//Predefined Correct Values
	expect := []float64{0, 0.6931471805599453, 1.0986122886681096,
		1.3862943611198906, 1.6094379124341003, 1.791759469228055,
		1.9459101490553132, 2.0794415416798357, 2.1972245773362196,
		2.302585092994046,
	}

	//Init a new variable
	actual := make([]float64, len(nums))
	//Run ln function
	lnResult := LnInt16(nums, actual)
	require.Equal(t, nulls.Any(lnResult.Nsp), false)

	for i := range actual {
		require.Equal(t, expect[i], actual[i])
	}
}

func TestLnInt32(t *testing.T) {
	//Test values
	nums := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	//Predefined Correct Values
	expect := []float64{0, 0.6931471805599453, 1.0986122886681096,
		1.3862943611198906, 1.6094379124341003, 1.791759469228055,
		1.9459101490553132, 2.0794415416798357, 2.1972245773362196,
		2.302585092994046,
	}

	//Init a new variable
	actual := make([]float64, len(nums))
	//Run ln function
	lnResult := LnInt32(nums, actual)
	require.Equal(t, nulls.Any(lnResult.Nsp), false)

	for i := range actual {
		require.Equal(t, expect[i], actual[i])
	}
}

func TestLnInt64(t *testing.T) {
	//Test values
	nums := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	//Predefined Correct Values
	expect := []float64{0, 0.6931471805599453, 1.0986122886681096,
		1.3862943611198906, 1.6094379124341003, 1.791759469228055,
		1.9459101490553132, 2.0794415416798357, 2.1972245773362196,
		2.302585092994046,
	}

	//Init a new variable
	actual := make([]float64, len(nums))
	//Run ln function
	lnResult := LnInt64(nums, actual)
	require.Equal(t, nulls.Any(lnResult.Nsp), false)

	for i := range actual {
		require.Equal(t, expect[i], actual[i])
	}
}

func TestLnFloat32(t *testing.T) {
	//Test values
	nums := []float32{1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9}
	//Predefined Correct Values
	expect := []float64{
		0.0953102014787409,
		0.7884573820386862,
		1.1939224540228235,
		1.4816045625986316,
		1.7047480922384253,
		1.8870696345827689,
		2.0412203040888763,
		2.1747517431585770,
		2.2925347186082479,
	}
	//Init a new variable
	actual := make([]float64, len(nums))
	//Run ln function
	lnResult := LnFloat32(nums, actual)
	require.Equal(t, nulls.Any(lnResult.Nsp), false)

	for i := range actual {
		require.Equal(t, expect[i], actual[i])
	}
}

func TestLnFloat64(t *testing.T) {
	//Test values
	nums := []float64{1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9}
	//Predefined Correct Values
	expect := []float64{
		0.09531017980432493,
		0.7884573603642703,
		1.1939224684724346,
		1.4816045409242156,
		1.7047480922384253,
		1.8870696490323797,
		2.0412203288596382,
		2.174751721484161,
		2.2925347571405443,
	}
	//Init a new variable
	actual := make([]float64, len(nums))
	//Run ln function
	lnResult := LnFloat64(nums, actual)
	require.Equal(t, nulls.Any(lnResult.Nsp), false)

	for i := range actual {
		require.Equal(t, expect[i], actual[i])
	}
}

func TestNegativeLnInt8(t *testing.T) {
	//Test values
	nums := []int8{1, -1, 2, -2, 3, -3}
	//Init a new variable
	actual := make([]float64, len(nums))
	//Run ln function
	lnResult := LnInt8(nums, actual)
	for i := 0; i < len(actual)/2; i++ {
		require.Equal(t, nulls.Contains(lnResult.Nsp, uint64(i*2)), false)
		require.Equal(t, nulls.Contains(lnResult.Nsp, uint64(i*2+1)), true)
	}
}

func TestNegativeLnInt16(t *testing.T) {
	//Test values
	nums := []int16{1, -1, 2, -2, 3, -3}
	//Init a new variable
	actual := make([]float64, len(nums))
	//Run ln function
	lnResult := LnInt16(nums, actual)
	for i := 0; i < len(actual)/2; i++ {
		require.Equal(t, nulls.Contains(lnResult.Nsp, uint64(i*2)), false)
		require.Equal(t, nulls.Contains(lnResult.Nsp, uint64(i*2+1)), true)
	}
}

func TestNegativeLnInt32(t *testing.T) {
	//Test values
	nums := []int32{1, -1, 2, -2, 3, -3}
	//Init a new variable
	actual := make([]float64, len(nums))
	//Run ln function
	lnResult := LnInt32(nums, actual)
	for i := 0; i < len(actual)/2; i++ {
		require.Equal(t, nulls.Contains(lnResult.Nsp, uint64(i*2)), false)
		require.Equal(t, nulls.Contains(lnResult.Nsp, uint64(i*2+1)), true)
	}
}

func TestNegativeLnInt64(t *testing.T) {
	//Test values
	nums := []int64{1, -1, 2, -2, 3, -3}
	//Init a new variable
	actual := make([]float64, len(nums))
	//Run ln function
	lnResult := LnInt64(nums, actual)
	for i := 0; i < len(actual)/2; i++ {
		require.Equal(t, nulls.Contains(lnResult.Nsp, uint64(i*2)), false)
		require.Equal(t, nulls.Contains(lnResult.Nsp, uint64(i*2+1)), true)
	}
}

func TestNegativeLnFloat32(t *testing.T) {
	//Test values
	nums := []float32{1, -1.1, 2, -2.2, 3, -3.3}
	//Init a new variable
	actual := make([]float64, len(nums))
	//Run ln function
	lnResult := LnFloat32(nums, actual)
	for i := 0; i < len(actual)/2; i++ {
		require.Equal(t, nulls.Contains(lnResult.Nsp, uint64(i*2)), false)
		require.Equal(t, nulls.Contains(lnResult.Nsp, uint64(i*2+1)), true)
	}
}

func TestNegativeLnFloat64(t *testing.T) {
	//Test values
	nums := []float64{1, -1.1, 2, -2.2, 3, -3.3}
	//Init a new variable
	actual := make([]float64, len(nums))
	//Run ln function
	lnResult := LnFloat64(nums, actual)
	for i := 0; i < len(actual)/2; i++ {
		require.Equal(t, nulls.Contains(lnResult.Nsp, uint64(i*2)), false)
		require.Equal(t, nulls.Contains(lnResult.Nsp, uint64(i*2+1)), true)
	}
}

func TestZeroLnInt8(t *testing.T) {
	//Test values
	nums := []int8{0}
	//Init a new variable
	actual := make([]float64, 1)
	//Run ln function
	lnResult := LnInt8(nums, actual)
	require.Equal(t, nulls.Contains(lnResult.Nsp, 0), true)
}

func TestZeroLnInt16(t *testing.T) {
	//Test values
	nums := []int16{0}
	//Init a new variable
	actual := make([]float64, 1)
	//Run ln function
	lnResult := LnInt16(nums, actual)
	require.Equal(t, nulls.Contains(lnResult.Nsp, 0), true)
}

func TestZeroLnInt32(t *testing.T) {
	//Test values
	nums := []int32{0}
	//Init a new variable
	actual := make([]float64, 1)
	//Run ln function
	lnResult := LnInt32(nums, actual)
	require.Equal(t, nulls.Contains(lnResult.Nsp, 0), true)
}

func TestZeroLnInt64(t *testing.T) {
	//Test values
	nums := []int64{0}
	//Init a new variable
	actual := make([]float64, 1)
	//Run ln function
	lnResult := LnInt64(nums, actual)
	require.Equal(t, nulls.Contains(lnResult.Nsp, 0), true)
}

func TestZeroLnFloat32(t *testing.T) {
	//Test values
	nums := []float32{0}
	//Init a new variable
	actual := make([]float64, 1)
	//Run ln function
	lnResult := LnFloat32(nums, actual)
	require.Equal(t, nulls.Contains(lnResult.Nsp, 0), true)
}

func TestZeroLnFloat64(t *testing.T) {
	//Test values
	nums := []float64{0}
	//Init a new variable
	actual := make([]float64, 1)
	//Run ln function
	lnResult := LnFloat64(nums, actual)
	require.Equal(t, nulls.Contains(lnResult.Nsp, 0), true)
}
