package merge

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestResourceController(t *testing.T) {
	rc := new(resourceController)
	rc.setMemLimit(10000)
	require.Equal(t, rc.limit, int64(9000))
}
