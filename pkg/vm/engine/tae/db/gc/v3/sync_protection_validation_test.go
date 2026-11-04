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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
