package binary

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestExtractFromDate(t *testing.T) {
	vector0 := testutil.MakeScalarVarchar("year", 4)
	vector1 := testutil.MakeDateVector([]string{"2020-01-01", "2021-02-03", "2024-03-04", ""}, []uint64{3})
	inputVectors := make([]*vector.Vector, 2)
	inputVectors[0] = vector0
	inputVectors[1] = vector1
	proc := testutil.NewProc()
	outputVector, err := ExtractFromDate(inputVectors, proc)
	require.NoError(t, err)
	outputValues, ok := outputVector.Col.([]uint32)
	require.True(t, ok)
	require.Equal(t, []uint32{2020, 2021, 2024, 1}, outputValues)
	require.True(t, nulls.Contains(outputVector.Nsp, uint64(3)))

	vector0 = testutil.MakeScalarVarchar("month", 4)
	vector1 = testutil.MakeDateVector([]string{"2020-01-01", "2021-02-03", "2024-03-04", ""}, []uint64{3})
	inputVectors = make([]*vector.Vector, 2)
	inputVectors[0] = vector0
	inputVectors[1] = vector1
	proc = testutil.NewProc()
	outputVector, err = ExtractFromDate(inputVectors, proc)
	require.NoError(t, err)
	outputValues, ok = outputVector.Col.([]uint32)
	fmt.Println(outputValues)
	require.True(t, ok)
	require.Equal(t, []uint32{1, 2, 3, 1}, outputValues)
	require.True(t, nulls.Contains(outputVector.Nsp, uint64(3)))

	vector0 = testutil.MakeScalarVarchar("day", 4)
	vector1 = testutil.MakeDateVector([]string{"2020-01-01", "2021-02-03", "2024-03-04", ""}, []uint64{3})
	inputVectors = make([]*vector.Vector, 2)
	inputVectors[0] = vector0
	inputVectors[1] = vector1
	proc = testutil.NewProc()
	outputVector, err = ExtractFromDate(inputVectors, proc)
	require.NoError(t, err)
	outputValues, ok = outputVector.Col.([]uint32)
	fmt.Println(outputValues)
	require.True(t, ok)
	require.Equal(t, []uint32{1, 3, 4, 1}, outputValues)
	require.True(t, nulls.Contains(outputVector.Nsp, uint64(3)))

	vector0 = testutil.MakeScalarVarchar("year_month", 4)
	vector1 = testutil.MakeDateVector([]string{"2020-01-01", "2021-02-03", "2024-03-04", ""}, []uint64{3})
	inputVectors = make([]*vector.Vector, 2)
	inputVectors[0] = vector0
	inputVectors[1] = vector1
	proc = testutil.NewProc()
	outputVector, err = ExtractFromDate(inputVectors, proc)
	require.NoError(t, err)
	outputValues, ok = outputVector.Col.([]uint32)
	fmt.Println(outputValues)
	require.True(t, ok)
	require.Equal(t, []uint32{202001, 202102, 202403, 101}, outputValues)
	require.True(t, nulls.Contains(outputVector.Nsp, uint64(3)))
}
