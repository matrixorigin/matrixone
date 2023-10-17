package v2

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestA(t *testing.T) {
	c := NewDashboardCreator("http://127.0.0.1:3000", "admin", "admin", "Prometheus")
	require.NoError(t, c.Create())
}
