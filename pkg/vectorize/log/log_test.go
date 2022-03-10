package log

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLogUint8(t *testing.T) {
	//Test values
	nums := []uint8{1, 2, 55, 44, 33, 22, 16}
	//Predefined Correct Values
	logNums := []float64{0, 0.6931471805599453, 4.007333185232471, 3.784189633918261, 3.4965075614664802, 3.091042453358316, 2.772588722239781}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	LogResult := logUint8(nums, newNums)
	require.Equal(t, nulls.Any(LogResult.Nsp), false)

	for i := range LogResult.Result {
		require.Equal(t, logNums[i], LogResult.Result[i])
	}
}

func TestLogUint16(t *testing.T) {
	//Test values
	nums := []uint16{1, 2, 55, 44, 33, 22, 16}
	//Predefined Correct Values
	logNums := []float64{0, 0.6931471805599453, 4.007333185232471, 3.784189633918261, 3.4965075614664802, 3.091042453358316, 2.772588722239781}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	LogResult := logUint16(nums, newNums)
	require.Equal(t, nulls.Any(LogResult.Nsp), false)

	for i := range LogResult.Result {
		require.Equal(t, logNums[i], LogResult.Result[i])
	}
}

func TestLogUint32(t *testing.T) {
	//Test values
	nums := []uint32{1, 2, 55, 44, 33, 22, 16}
	//Predefined Correct Values
	logNums := []float64{0, 0.6931471805599453, 4.007333185232471, 3.784189633918261, 3.4965075614664802, 3.091042453358316, 2.772588722239781}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	LogResult := logUint32(nums, newNums)
	require.Equal(t, nulls.Any(LogResult.Nsp), false)

	for i := range LogResult.Result {
		require.Equal(t, logNums[i], LogResult.Result[i])
	}
}

func TestLogUint64(t *testing.T) {
	//Test values
	nums := []uint64{1, 2, 55, 44, 33, 22, 16}
	//Predefined Correct Values
	logNums := []float64{0, 0.6931471805599453, 4.007333185232471, 3.784189633918261, 3.4965075614664802, 3.091042453358316, 2.772588722239781}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	LogResult := logUint64(nums, newNums)
	require.Equal(t, nulls.Any(LogResult.Nsp), false)

	for i := range LogResult.Result {
		require.Equal(t, logNums[i], LogResult.Result[i])
	}
}

func TestLogint8(t *testing.T) {
	//Test values
	nums := []int8{1, 2, 55, 44, 33, 22, 16}
	//Predefined Correct Values
	logNums := []float64{0, 0.6931471805599453, 4.007333185232471, 3.784189633918261, 3.4965075614664802, 3.091042453358316, 2.772588722239781}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	LogResult := logInt8(nums, newNums)
	require.Equal(t, nulls.Any(LogResult.Nsp), false)

	for i := range LogResult.Result {
		require.Equal(t, logNums[i], LogResult.Result[i])
	}
}

func TestLogint16(t *testing.T) {
	//Test values
	nums := []int16{1, 2, 55, 44, 33, 22, 16}
	//Predefined Correct Values
	logNums := []float64{0, 0.6931471805599453, 4.007333185232471, 3.784189633918261, 3.4965075614664802, 3.091042453358316, 2.772588722239781}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	LogResult := logInt16(nums, newNums)
	require.Equal(t, nulls.Any(LogResult.Nsp), false)

	for i := range LogResult.Result {
		require.Equal(t, logNums[i], LogResult.Result[i])
	}
}

func TestLogint32(t *testing.T) {
	//Test values
	nums := []int32{1, 2, 55, 44, 33, 22, 16}
	//Predefined Correct Values
	logNums := []float64{0, 0.6931471805599453, 4.007333185232471, 3.784189633918261, 3.4965075614664802, 3.091042453358316, 2.772588722239781}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	LogResult := logInt32(nums, newNums)
	require.Equal(t, nulls.Any(LogResult.Nsp), false)

	for i := range LogResult.Result {
		require.Equal(t, logNums[i], LogResult.Result[i])
	}
}

func TestLogint64(t *testing.T) {
	//Test values
	nums := []int64{1, 2, 55, 44, 33, 22, 16}
	//Predefined Correct Values
	logNums := []float64{0, 0.6931471805599453, 4.007333185232471, 3.784189633918261, 3.4965075614664802, 3.091042453358316, 2.772588722239781}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	LogResult := logInt64(nums, newNums)
	require.Equal(t, nulls.Any(LogResult.Nsp), false)

	for i := range LogResult.Result {
		require.Equal(t, logNums[i], LogResult.Result[i])
	}
}

func TestLogfloat32(t *testing.T) {
	//Test values
	nums := []float32{1.23, 2.34, 55.66, 44.33, 33.66, 22.22, 16.16}
	//Predefined Correct Values
	logNums := []float64{0.20701418489122547, 0.850150892689828, 4.019261753356316, 3.7916616900620523, 3.516310184229449, 3.1009927533093453, 2.782539043650629}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	LogResult := logFloat32(nums, newNums)
	require.Equal(t, nulls.Any(LogResult.Nsp), false)

	for i := range LogResult.Result {
		require.Equal(t, logNums[i], LogResult.Result[i])
	}
}

func TestLogfloat64(t *testing.T) {
	//Test values
	nums := []float64{1.23, 2.34, 55.66, 44.33, 33.66, 22.22, 16.16}
	//Predefined Correct Values
	logNums := []float64{0.20701416938432612, 0.85015092936961, 4.019261756097745,
		3.7916616487569623, 3.51631018876266, 3.100992784211484, 2.7825390530929495}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	LogResult := logFloat64(nums, newNums)
	require.Equal(t, nulls.Any(LogResult.Nsp), false)

	for i := range LogResult.Result {
		require.Equal(t, logNums[i], LogResult.Result[i])
	}
}
