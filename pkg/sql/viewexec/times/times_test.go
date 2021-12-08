package times

import (
	"github.com/stretchr/testify/require"
	"testing"
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
