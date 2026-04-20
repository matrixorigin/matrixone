// Copyright 2021 - 2022 Matrix Origin
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

package moerr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsSyncProtectionValidationError_Nil(t *testing.T) {
	assert.False(t, IsSyncProtectionValidationError(nil))
}

func TestIsSyncProtectionValidationError_NotFound(t *testing.T) {
	err := NewSyncProtectionNotFoundNoCtx("job1")
	assert.True(t, IsSyncProtectionValidationError(err))
}

func TestIsSyncProtectionValidationError_SoftDelete(t *testing.T) {
	err := NewSyncProtectionSoftDeleteNoCtx("job1")
	assert.True(t, IsSyncProtectionValidationError(err))
}

func TestIsSyncProtectionValidationError_Expired(t *testing.T) {
	err := NewSyncProtectionExpiredNoCtx("job1", 100, 200)
	assert.True(t, IsSyncProtectionValidationError(err))
}

func TestIsSyncProtectionValidationError_OtherError(t *testing.T) {
	err := NewInternalErrorNoCtx("some other error")
	assert.False(t, IsSyncProtectionValidationError(err))
}

func TestNewSyncProtectionExpiredNoCtx(t *testing.T) {
	err := NewSyncProtectionExpiredNoCtx("job1", 100, 200)
	assert.NotNil(t, err)
	assert.True(t, IsMoErrCode(err, ErrSyncProtectionExpired))
}
