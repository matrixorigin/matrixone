package ceil

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestExpUint8(t *testing.T) {
	//Test values
	nums := []uint8{1, 4, 6, 8, 11, 17, 47}
	//Predefined Correct Values
	ceilnum := []uint8{1, 4, 6, 8, 11, 17, 47}
	//Init a new variable
	newnum := make([]uint8, len(nums))
	//Run abs function
	ceilresult := ceilUint8(nums, newnum)

	for i := range ceilresult {
		require.Equal(t, ceilnum[i], ceilresult[i])
	}
}
func TestExpUint16(t *testing.T) {
	//Test values
	nums := []uint16{1, 4, 6, 8, 11, 17, 47}
	//Predefined Correct Values
	ceilnum := []uint16{1, 4, 6, 8, 11, 17, 47}
	//Init a new variable
	newnum := make([]uint16, len(nums))
	//Run abs function
	ceilresult := ceilUint16(nums, newnum)

	for i := range ceilresult {
		require.Equal(t, ceilnum[i], ceilresult[i])
	}
}
func TestExpUint32(t *testing.T) {
	//Test values
	nums := []uint32{1, 4, 6, 8, 11, 17, 47}
	//Predefined Correct Values
	ceilnum := []uint32{1, 4, 6, 8, 11, 17, 47}
	//Init a new variable
	newnum := make([]uint32, len(nums))
	//Run abs function
	ceilresult := ceilUint32(nums, newnum)

	for i := range ceilresult {
		require.Equal(t, ceilnum[i], ceilresult[i])
	}
}
func TestExpUint64(t *testing.T) {
	//Test values
	nums := []uint64{1, 2, 3, 8, 10, 12, 40}
	//Predefined Correct Values
	ceilnum := []uint64{1, 2, 3, 8, 10, 12, 40}
	//Init a new variable
	newnum := make([]uint64, len(nums))
	//Run abs function
	ceilresult := ceilUint64(nums, newnum)

	for i := range ceilresult {
		require.Equal(t, ceilnum[i], ceilresult[i])
	}
}
func TestExpint8(t *testing.T) {
	//Test values
	nums := []int8{1, 2, 3, 8, 10, 12, 40}
	//Predefined Correct Values
	ceilnum := []int8{1, 2, 3, 8, 10, 12, 40}
	//Init a new variable
	newnum := make([]int8, len(nums))
	//Run abs function
	ceilresult := ceilInt8(nums, newnum)

	for i := range ceilresult {
		require.Equal(t, ceilnum[i], ceilresult[i])
	}
}
func TestExpint16(t *testing.T) {
	//Test values
	nums := []int16{1, 2, 3, 8, 10, 12, 40}
	//Predefined Correct Values
	ceilnum := []int16{1, 2, 3, 8, 10, 12, 40}
	//Init a new variable
	newnum := make([]int16, len(nums))
	//Run abs function
	ceilresult := ceilInt16(nums, newnum)

	for i := range ceilresult {
		require.Equal(t, ceilnum[i], ceilresult[i])
	}
}
func TestExpint32(t *testing.T) {
	//Test values
	nums := []int32{1, 2, 3, 8, 10, 12, 40}
	//Predefined Correct Values
	ceilnum := []int32{1, 2, 3, 8, 10, 12, 40}
	//Init a new variable
	newnum := make([]int32, len(nums))
	//Run abs function
	ceilresult := ceilInt32(nums, newnum)

	for i := range ceilresult {
		require.Equal(t, ceilnum[i], ceilresult[i])
	}
}
func TestExpint64(t *testing.T) {
	//Test values
	nums := []int64{1, 2, 3, 8, 10, 12, 40}
	//Predefined Correct Values
	ceilnum := []int64{1, 2, 3, 8, 10, 12, 40}
	//Init a new variable
	newnum := make([]int64, len(nums))
	//Run abs function
	ceilresult := ceilInt64(nums, newnum)

	for i := range ceilresult {
		require.Equal(t, ceilnum[i], ceilresult[i])
	}
}
func TestExpfloat32(t *testing.T) {
	//Test values
	nums := []float32{1.4, -2.3, 3.3, -8.2, -10.2, 12.2, -40.6}
	//Predefined Correct Values
	ceilnum := []float32{2, -2, 4, -8, -10, 13, -40}
	//Init a new variable
	newnum := make([]float32, len(nums))
	//Run abs function
	ceilresult := ceilFloat32(nums, newnum)

	for i := range ceilresult {
		require.Equal(t, ceilnum[i], ceilresult[i])
	}
}
func TestExpfloat64(t *testing.T) {
	//Test values
	nums := []float64{1.4, -2.3, 3.3, -8.2, -10.2, 12.2, -40.6}
	//Predefined Correct Values
	ceilnum := []float64{2, -2, 4, -8, -10, 13, -40}
	//Init a new variable
	newnum := make([]float64, len(nums))
	//Run abs function
	ceilresult := ceilFloat64(nums, newnum)

	for i := range ceilresult {
		require.Equal(t, ceilnum[i], ceilresult[i])
	}
}

func TestExpString(t *testing.T) {
	//Test values
	nums := []string{"1.4", "-2.3", "3.3", "-8.2", "-10.2", "12.2", "-40.6"}
	//Predefined Correct Values
	ceilnum := []float64{2, -2, 4, -8, -10, 13, -40}
	//Init a new variable
	newnum := make([]float64, len(nums))
	//Run abs function
	ceilresult := ceilString(nums, newnum)

	for i := range ceilresult {
		require.Equal(t, ceilnum[i], ceilresult[i])
	}
}
