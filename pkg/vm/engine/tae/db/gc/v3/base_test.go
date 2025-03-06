package gc

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

// TestBaseCheckpointGCJobDefaults tests the default configuration values
func TestBaseCheckpointGCJobDefaults(t *testing.T) {
	job := &BaseCheckpointGCJob{}
	job.fillDefaults()

	assert.Equal(t, Default_Coarse_EstimateRows, job.config.coarseEstimateRows)
	assert.Equal(t, Default_Coarse_Probility, job.config.coarseProbility)
	assert.Equal(t, Default_CanGC_TailSize, job.config.canGCCacheSize)
}

// TestBaseCheckpointGCJobCustomConfig tests custom configuration
func TestBaseCheckpointGCJobCustomConfig(t *testing.T) {
	job := &BaseCheckpointGCJob{}
	customEstimateRows := 5000000
	customProbability := 0.00002
	customCacheSize := 32 * 1024 * 1024

	opt := WithGCJobCoarseConfig(customEstimateRows, customProbability, customCacheSize)
	opt(job)

	assert.Equal(t, customEstimateRows, job.config.coarseEstimateRows)
	assert.Equal(t, customProbability, job.config.coarseProbility)
	assert.Equal(t, customCacheSize, job.config.canGCCacheSize)
}

// TestBaseCheckpointGCJobClose tests the cleanup of resources
func TestBaseCheckpointGCJobClose(t *testing.T) {
	transObjects := make(map[string]*ObjectEntry)
	// Add some test objects
	transObjects["test1"] = &ObjectEntry{
		stats:    &objectio.ObjectStats{},
		createTS: types.BuildTS(1, 0),
		dropTS:   types.BuildTS(2, 0),
	}

	job, _ := getJob(context.Background(), t, transObjects)
	err := job.Close()
	require.NoError(t, err)
	assert.Empty(t, job.transObjects)
	assert.Nil(t, job.sourcer)
	assert.Nil(t, job.buffer)
	assert.Nil(t, job.snapshotMeta)
	assert.Nil(t, job.accountSnapshots)
	assert.Nil(t, job.pitr)
	assert.Nil(t, job.ts)
}

// TestSnapshotAndPitrFineFilter tests the fine filter functionality
func TestSnapshotAndPitrFineFilter(t *testing.T) {
	ts := types.BuildTS(100, 0)
	accountSnapshots := map[uint32][]types.TS{
		1: {types.BuildTS(50, 0)},
	}
	pitrs := &logtail.PitrInfo{}
	snapshotMeta := &logtail.SnapshotMeta{}
	transObjects := make(map[string]*ObjectEntry)

	filter, err := MakeSnapshotAndPitrFineFilter(
		&ts,
		accountSnapshots,
		pitrs,
		snapshotMeta,
		transObjects,
	)

	require.NoError(t, err)
	require.NotNil(t, filter)
}

// TestGCJobExecute tests the execution flow of a GC job
func TestGCJobExecute(t *testing.T) {
	ctx := context.Background()
	// Create test objects with overlapping timestamps
	job, _ := getJob(ctx, t, nil)

	mockProvider := &MockFilterProvider{
		coarseFilterFn: func(ctx context.Context, bm *bitmap.Bitmap, bat *batch.Batch, mp *mpool.MPool) error {
			return nil
		},
		fineFilterFn: func(ctx context.Context, bm *bitmap.Bitmap, bat *batch.Batch, mp *mpool.MPool) error {
			return nil
		},
	}
	job.filterProvider = mockProvider

	err := job.Execute(ctx)
	require.NoError(t, err)

	// Verify results initialization
	assert.NotNil(t, job.result.filesToGC)
	assert.NotNil(t, job.result.filesNotGC)
}
