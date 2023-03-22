// Copyright 2023 Matrix Origin
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

package lockop

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

// FetchLockRowsFunc fetch lock rows from vector.
type FetchLockRowsFunc func(
	// primary data vector
	vec *vector.Vector,
	// hodler to encode primary key to lock row
	parker *types.Packer,
	// primary key type
	tp types.Type,
	// global config: max lock rows bytes per lock
	max int,
	// is lock table lock
	lockTabel bool) ([][]byte, lock.Granularity)

// LockOptions lock operation options
type LockOptions struct {
	maxBytesPerLock int
	mode            lock.LockMode
	lockTable       bool
	parker          *types.Packer
	fetchFunc       FetchLockRowsFunc
}

// RetryTimestamp retry timestamp range [from, to)
type RetryTimestamp struct {
	From timestamp.Timestamp
	To   timestamp.Timestamp
}
