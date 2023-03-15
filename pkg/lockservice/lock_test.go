// Copyright 2022 Matrix Origin
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

package lockservice

import (
	"testing"

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/stretchr/testify/assert"
)

func TestNewRowLock(t *testing.T) {
	txnID := []byte("1")
	l := newRowLock(txnID, pb.LockMode_Exclusive)
	assert.True(t, l.isLockRow())
	assert.Equal(t, pb.LockMode_Exclusive, l.getLockMode())
}

func TestNewRangeLock(t *testing.T) {
	txnID := []byte("1")
	sl, el := newRangeLock(txnID, pb.LockMode_Shared)

	assert.True(t, sl.isLockRange())
	assert.True(t, sl.isLockRangeStart())
	assert.Equal(t, pb.LockMode_Shared, sl.getLockMode())

	assert.True(t, el.isLockRange())
	assert.True(t, el.isLockRangeEnd())
	assert.Equal(t, pb.LockMode_Shared, el.getLockMode())
}
