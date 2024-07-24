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
	opts := LockOptions{}
	opts.Mode = pb.LockMode_Exclusive
	l := newRowLock(getLogger(""), &lockContext{txn: &activeTxn{txnID: txnID}, opts: opts})
	assert.True(t, l.isLockRow())
	assert.Equal(t, pb.LockMode_Exclusive, l.GetLockMode())
}

func TestNewRangeLock(t *testing.T) {
	txnID := []byte("1")
	opts := LockOptions{}
	opts.Mode = pb.LockMode_Shared
	sl, el := newRangeLock(getLogger(""), &lockContext{txn: &activeTxn{txnID: txnID}, opts: opts})

	assert.Equal(t, pb.LockMode_Shared, sl.GetLockMode())
	assert.True(t, el.isLockRangeEnd())
	assert.Equal(t, pb.LockMode_Shared, el.GetLockMode())
}
