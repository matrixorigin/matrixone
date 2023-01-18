package testcase

import (
	"context"
	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/tests/service"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"testing"
	"time"
)

const defaultTestTimeout = 3 * time.Minute

// TestCalculateStorageUsage
// example like tasks.TestCronTask in task_test.go
func TestCalculateStorageUsage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	opt := service.DefaultOptions()
	// initialize cluster
	c, err := service.NewCluster(t, opt.WithLogLevel(zap.InfoLevel))
	require.NoError(t, err)

	// close the cluster
	defer func(c service.Cluster) {
		require.NoError(t, c.Close())
	}(c)
	// start the cluster
	require.NoError(t, c.Start())

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil, nil)
	pu.SV.SetDefaultValues()

	ieFactory := func() ie.InternalExecutor {
		return frontend.NewInternalExecutor(pu)
	}

	err = metric.CalculateStorageUsage(ctx, ieFactory)
	require.Nil(t, err)

}
