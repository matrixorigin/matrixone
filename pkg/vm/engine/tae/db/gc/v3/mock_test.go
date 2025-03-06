package gc

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path"
	"testing"
)

// MockBaseReader implements engine.BaseReader interface for testing
type MockBaseReader struct {
	batches []*batch.Batch
	current int
	err     error
}

func NewMockBaseReader(batches []*batch.Batch) *MockBaseReader {
	return &MockBaseReader{
		batches: batches,
		current: 0,
	}
}

func (m *MockBaseReader) Read(ctx context.Context, attrs []string, expr *plan.Expr, mp *mpool.MPool, bat *batch.Batch) (bool, error) {
	if m.err != nil {
		return false, m.err
	}
	if m.current >= len(m.batches) {
		return true, nil
	}

	// Copy data from current batch to output batch
	srcBat := m.batches[m.current]
	if err := bat.UnionWindow(srcBat, 0, srcBat.RowCount(), mp); err != nil {
		return false, err
	}
	m.current++
	return false, nil
}

func (m *MockBaseReader) Close() error {
	return nil
}

// MockFilterProvider implements FilterProvider interface for testing
type MockFilterProvider struct {
	coarseFilterFn FilterFn
	fineFilterFn   FilterFn
}

func (m *MockFilterProvider) CoarseFilter(ctx context.Context) (FilterFn, error) {
	return m.coarseFilterFn, nil
}

func (m *MockFilterProvider) FineFilter(ctx context.Context) (FilterFn, error) {
	return m.fineFilterFn, nil
}

// Helper functions to create test batches
func createTestBatch() *containers.Batch {
	opt := containers.Options{}
	opt.Capacity = 0
	bat := containers.BuildBatch(ObjectTableAttrs, ObjectTableTypes, opt)
	// Add test data here if needed
	return bat
}

func getJob(ctx context.Context, t *testing.T, transObjects map[string]*ObjectEntry) (*BaseCheckpointGCJob, map[string]*ObjectEntry) {
	if transObjects == nil {
		noid := objectio.NewObjectid()
		stats := objectio.NewObjectStatsWithObjectID(&noid, false, false, false)
		noid2 := objectio.NewObjectid()
		stats2 := objectio.NewObjectStatsWithObjectID(&noid2, false, false, false)
		noid3 := objectio.NewObjectid()
		stats3 := objectio.NewObjectStatsWithObjectID(&noid3, false, false, false)
		transObjects = map[string]*ObjectEntry{
			"obj1": {
				stats:    stats,
				createTS: types.BuildTS(40, 0),
				dropTS:   types.BuildTS(50, 0),
			},
			"obj2": {
				stats:    stats2,
				createTS: types.BuildTS(45, 0),
				dropTS:   types.BuildTS(60, 0),
			},
			"obj3": {
				stats:    stats3,
				createTS: types.BuildTS(55, 0),
				dropTS:   types.BuildTS(70, 0),
			},
		}
	}
	// Create test batch
	bat := createTestBatch()
	for _, obj := range transObjects {
		bat.Vecs[0].Append(obj.stats[:], false)
		bat.Vecs[1].Append(obj.createTS, false)
		bat.Vecs[2].Append(obj.dropTS, false)
		bat.Vecs[3].Append(uint64(1), false)
		bat.Vecs[4].Append(uint64(1), false)
	}
	dir := testutils.InitTestEnv("GCV3", t)
	dir = path.Join(dir, "/local")
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)
	buffer := containers.NewOneSchemaBatchBuffer(mpool.GB, ObjectTableAttrs, ObjectTableTypes)
	require.NotNil(t, buffer)
	job := &BaseCheckpointGCJob{
		transObjects: make(map[string]*ObjectEntry),
		sourcer:      NewMockBaseReader([]*batch.Batch{containers.ToCNBatch(bat)}),
		buffer:       buffer,
	}
	job.GCExecutor = *NewGCExecutor(buffer, true, mpool.GB, common.DefaultAllocator, service)
	return job, transObjects
}
