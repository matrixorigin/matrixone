// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gc

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMockCleaner_Default(t *testing.T) {
	c := NewMockCleaner()
	ctx := context.Background()

	assert.NoError(t, c.Replay(ctx))
	assert.NoError(t, c.Process(ctx, nil))
	assert.NoError(t, c.TryGC(ctx))
	assert.NoError(t, c.Close())
	assert.NoError(t, c.DoCheck(ctx))
	assert.NoError(t, c.RemoveChecker("key"))
	assert.Nil(t, c.GetChecker("key"))
	assert.Nil(t, c.GetScanWaterMark())
	assert.Nil(t, c.GetCheckpointGCWaterMark())
	assert.Nil(t, c.GetScannedWindow())
	assert.Nil(t, c.GetMinMerged())
	assert.Nil(t, c.GetMPool())
	assert.Equal(t, 0, c.AddChecker(nil, "key"))
	assert.False(t, c.GCEnabled())
	assert.Equal(t, "", c.GetTablePK(0))
	assert.Equal(t, "", c.Verify(ctx))
	assert.Nil(t, c.GetSyncProtectionManager())
}

func TestNewMockCleaner_WithReplayFunc(t *testing.T) {
	expectedErr := errors.New("replay error")
	c := NewMockCleaner(WithReplayFunc(func(ctx context.Context) error {
		return expectedErr
	}))
	assert.Equal(t, expectedErr, c.Replay(context.Background()))
}

func TestNewMockCleaner_WithProcessFunc(t *testing.T) {
	expectedErr := errors.New("process error")
	c := NewMockCleaner(WithProcessFunc(func(ctx context.Context) error {
		return expectedErr
	}))
	assert.Equal(t, expectedErr, c.Process(context.Background(), nil))
}

func TestNewMockCleaner_WithTryGCFunc(t *testing.T) {
	expectedErr := errors.New("gc error")
	c := NewMockCleaner(WithTryGCFunc(func(ctx context.Context) error {
		return expectedErr
	}))
	assert.Equal(t, expectedErr, c.TryGC(context.Background()))
}

func TestMockCleaner_SnapshotsAndPITRs(t *testing.T) {
	c := NewMockCleaner()
	snapshots, err := c.GetSnapshots()
	assert.Nil(t, snapshots)
	assert.NoError(t, err)

	pitrs, err := c.GetPITRs()
	assert.Nil(t, pitrs)
	assert.NoError(t, err)

	details, err := c.GetDetails(context.Background())
	assert.Nil(t, details)
	assert.NoError(t, err)

	tables, err := c.ISCPTables()
	assert.Nil(t, tables)
	assert.NoError(t, err)
}

func TestMockCleaner_BackupProtection(t *testing.T) {
	c := NewMockCleaner()
	ts, updateTime, isActive := c.GetBackupProtection()
	assert.True(t, ts.IsEmpty())
	assert.True(t, updateTime.IsZero())
	assert.False(t, isActive)

	// These should not panic
	c.SetBackupProtection(ts)
	c.UpdateBackupProtection(ts)
	c.RemoveBackupProtection()
}

func TestMockCleaner_EnableDisableGC(t *testing.T) {
	c := NewMockCleaner()
	c.EnableGC()
	c.DisableGC()
	assert.False(t, c.GCEnabled())
}

func TestMockCleaner_SetTidAndStop(t *testing.T) {
	c := NewMockCleaner()
	// Should not panic
	c.SetTid(123)
	c.Stop()
}

// ============================================================================
// ValidateSyncProtection Tests
// ============================================================================

func TestValidateSyncProtection_NotFound(t *testing.T) {
	m := NewSyncProtectionManager()
	err := m.ValidateSyncProtection("nonexistent", 100)
	require.Error(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrSyncProtectionNotFound))
}

func TestValidateSyncProtection_SoftDeleted(t *testing.T) {
	m := NewSyncProtectionManager()
	bfData := buildTestBF(t, []string{"obj1"})
	require.NoError(t, m.RegisterSyncProtection("job1", bfData, 1000, "task1"))
	require.NoError(t, m.UnregisterSyncProtection("job1"))

	err := m.ValidateSyncProtection("job1", 100)
	require.Error(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrSyncProtectionSoftDelete))
}

func TestValidateSyncProtection_Expired(t *testing.T) {
	m := NewSyncProtectionManager()
	bfData := buildTestBF(t, []string{"obj1"})
	require.NoError(t, m.RegisterSyncProtection("job1", bfData, 100, "task1"))

	err := m.ValidateSyncProtection("job1", 200)
	require.Error(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrSyncProtectionExpired))
}

func TestValidateSyncProtection_Valid(t *testing.T) {
	m := NewSyncProtectionManager()
	bfData := buildTestBF(t, []string{"obj1"})
	require.NoError(t, m.RegisterSyncProtection("job1", bfData, 1000, "task1"))

	assert.NoError(t, m.ValidateSyncProtection("job1", 100))
}

func TestValidateSyncProtection_ExactTS(t *testing.T) {
	m := NewSyncProtectionManager()
	bfData := buildTestBF(t, []string{"obj1"})
	require.NoError(t, m.RegisterSyncProtection("job1", bfData, 100, "task1"))

	// ValidTS == prepareTS: ValidTS < prepareTS is false, so should pass
	assert.NoError(t, m.ValidateSyncProtection("job1", 100))
}
