package gc

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// TestComplexGCScenario tests a more complex GC scenario with multiple objects
func TestComplexGCScenario(t *testing.T) {
	ctx := context.Background()

	// Create test objects with different timestamps
	objects := map[string]*ObjectEntry{
		"obj1": {
			stats:    &objectio.ObjectStats{},
			createTS: types.BuildTS(1, 0),
			dropTS:   types.BuildTS(5, 0),
		},
		"obj2": {
			stats:    &objectio.ObjectStats{},
			createTS: types.BuildTS(2, 0),
			dropTS:   types.BuildTS(6, 0),
		},
		"obj3": {
			stats:    &objectio.ObjectStats{},
			createTS: types.BuildTS(3, 0),
			dropTS:   types.BuildTS(7, 0),
		},
	}

	job, _ := getJob(ctx, t, objects)
	// Setup mock filter provider
	mockProvider := &MockFilterProvider{
		coarseFilterFn: func(ctx context.Context, bm *bitmap.Bitmap, bat *batch.Batch, mp *mpool.MPool) error {
			// Simulate coarse filtering
			return nil
		},
		fineFilterFn: func(ctx context.Context, bm *bitmap.Bitmap, bat *batch.Batch, mp *mpool.MPool) error {
			// Simulate fine filtering
			return nil
		},
	}
	job.filterProvider = mockProvider

	// Execute GC
	err := job.Execute(ctx)
	require.NoError(t, err)

	// Verify results
	assert.NotNil(t, job.result.filesToGC)
	assert.NotNil(t, job.result.filesNotGC)
}

// TestConcurrentGCOperations tests GC operations under concurrent scenarios
func TestConcurrentGCOperations(t *testing.T) {
	ctx := context.Background()

	// Create multiple GC jobs
	jobs := make([]*BaseCheckpointGCJob, 3)
	now := time.Now().UTC()
	for i := 0; i < 3; i++ {

		// Add some test objects
		transObjects := make(map[string]*ObjectEntry)
		transObjects[fmt.Sprintf("obj%d", i)] = &ObjectEntry{
			stats:    &objectio.ObjectStats{},
			createTS: types.BuildTS(now.UnixNano()+1+int64(i), 0),
			dropTS:   types.BuildTS(now.UnixNano()+5+int64(i), 0),
		}
		jobs[i], _ = getJob(ctx, t, transObjects)

		// Setup mock filter provider
		jobs[i].filterProvider = &MockFilterProvider{
			coarseFilterFn: func(ctx context.Context, bm *bitmap.Bitmap, bat *batch.Batch, mp *mpool.MPool) error {
				return nil
			},
			fineFilterFn: func(ctx context.Context, bm *bitmap.Bitmap, bat *batch.Batch, mp *mpool.MPool) error {
				return nil
			},
		}
	}

	// Run jobs concurrently
	errChan := make(chan error, len(jobs))
	for _, job := range jobs {
		go func(j *BaseCheckpointGCJob) {
			errChan <- j.Execute(ctx)
		}(job)
	}

	// Wait for all jobs to complete
	for i := 0; i < len(jobs); i++ {
		err := <-errChan
		require.NoError(t, err)
	}

	// Verify results
	for _, job := range jobs {
		assert.NotNil(t, job.result.filesToGC)
		assert.NotNil(t, job.result.filesNotGC)
	}
}

// TestGCWithSnapshots tests GC behavior with different snapshot configurations
func TestGCWithSnapshots(t *testing.T) {
	ctx := context.Background()
	ts := types.BuildTS(100, 0)

	// Create test snapshots
	accountSnapshots := map[uint32][]types.TS{
		1: {types.BuildTS(50, 0), types.BuildTS(75, 0)},
		2: {types.BuildTS(60, 0)},
	}

	snapshotMeta := &logtail.SnapshotMeta{}
	pitrs := &logtail.PitrInfo{}

	// Create test objects
	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, false, false, false)
	noid2 := objectio.NewObjectid()
	stats2 := objectio.NewObjectStatsWithObjectID(&noid2, false, false, false)
	transObjects := map[string]*ObjectEntry{
		"obj1": {
			stats:    stats,
			createTS: types.BuildTS(40, 0),
			dropTS:   types.BuildTS(80, 0),
		},
		"obj2": {
			stats:    stats2,
			createTS: types.BuildTS(70, 0),
			dropTS:   types.BuildTS(90, 0),
		},
	}

	// Create and test fine filter
	filter, err := MakeSnapshotAndPitrFineFilter(
		&ts,
		accountSnapshots,
		pitrs,
		snapshotMeta,
		transObjects,
	)
	require.NoError(t, err)
	require.NotNil(t, filter)
	mp, err := mpool.NewMPool("GC", 0, mpool.NoFixed)
	require.NoError(t, err)

	bat := createTestBatch()
	for _, obj := range transObjects {
		bat.Vecs[0].Append(obj.stats[:], false)
		bat.Vecs[1].Append(obj.createTS, false)
		bat.Vecs[2].Append(obj.dropTS, false)
		bat.Vecs[3].Append(uint64(1), false)
		bat.Vecs[4].Append(uint64(1), false)
	}

	// Test filter
	bm := bitmap.Bitmap{}
	bm.TryExpandWithSize(len(transObjects))
	err = filter(ctx, &bm, containers.ToCNBatch(bat), mp)
	require.NoError(t, err)
}
