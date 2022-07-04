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

package service

import (
	"runtime/debug"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/stretchr/testify/assert"
)

func TestGCWaiter(t *testing.T) {
	var c chan txn.TxnStatus
	var wg sync.WaitGroup
	wg.Add(1)
	func() {
		defer wg.Done()
		w := acquireWaiter()
		c = w.c
		w.close()
	}()
	wg.Wait()

	debug.FreeOSMemory()

	defer func() {
		if err := recover(); err != nil {
			return
		}
		assert.Fail(t, "must panic")
	}()
	c <- txn.TxnStatus_Aborted
}
