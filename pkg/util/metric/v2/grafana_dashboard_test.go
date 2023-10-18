package v2

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateDashboard(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	c := NewDashboardCreator("http://127.0.0.1:3000", "admin", "admin", "Prometheus")
	require.NoError(t, c.Create())
}
