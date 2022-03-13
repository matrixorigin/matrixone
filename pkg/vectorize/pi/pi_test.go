package pi

import (
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func TestPi(t *testing.T) {
	newNums := getPiImpl()
	require.Equal(t, math.Pi, newNums)
}
