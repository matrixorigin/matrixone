package embed

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStartSingleCN(t *testing.T) {
	c, err := NewCluster(WithCNCount(1))
	require.NoError(t, err)
	require.NoError(t, c.Start())
}

func TestStartMultiCN(t *testing.T) {
	c, err := NewCluster(WithCNCount(3))
	require.NoError(t, err)
	require.NoError(t, c.Start())
}
