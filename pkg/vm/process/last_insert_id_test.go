// Copyright 2026 Matrix Origin
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

package process

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func newLastInsertIDTestProc() *Process {
	return &Process{Base: &BaseProcess{
		LastInsertID:          new(uint64),
		StatementLastInsertID: new(uint64),
	}}
}

func TestSetStatementLastInsertIDIfLater(t *testing.T) {
	proc := newLastInsertIDTestProc()
	proc.SetLastInsertID(7)

	require.Equal(t, uint64(0), proc.SetStatementLastInsertIDIfLater(0))
	require.Equal(t, uint64(7), proc.GetLastInsertID())
	require.Equal(t, uint64(0), proc.GetStatementLastInsertID())

	require.Equal(t, uint64(20), proc.SetStatementLastInsertIDIfLater(20))
	require.Equal(t, uint64(20), proc.SetStatementLastInsertIDIfLater(10))
	require.Equal(t, uint64(20), proc.SetStatementLastInsertIDIfLater(0))
	require.Equal(t, uint64(20), proc.GetLastInsertID())
	require.Equal(t, uint64(20), proc.GetStatementLastInsertID())

	require.Equal(t, uint64(30), proc.SetStatementLastInsertIDIfLater(30))
	require.Equal(t, uint64(30), proc.GetLastInsertID())
	require.Equal(t, uint64(30), proc.GetStatementLastInsertID())

	noStatement := &Process{Base: &BaseProcess{LastInsertID: new(uint64)}}
	require.Equal(t, uint64(5), noStatement.SetStatementLastInsertIDIfLater(5))
	require.Equal(t, uint64(5), noStatement.GetLastInsertID())
	require.Equal(t, uint64(0), noStatement.GetStatementLastInsertID())

	noBase := &Process{}
	require.Equal(t, uint64(3), noBase.SetStatementLastInsertIDIfLater(3))
	noBase.SetLastInsertID(3)
	noBase.SetStatementLastInsertID(3)
	require.Equal(t, uint64(0), noBase.GetStatementLastInsertID())
}

// TestParallelScopesPublishOneLastInsertID runs two statements on one shared
// BaseProcess. Each statement's scopes publish concurrently; afterwards the
// session and statement values are both the statement's largest value.
func TestParallelScopesPublishOneLastInsertID(t *testing.T) {
	proc := newLastInsertIDTestProc()
	const scopes = 16
	const batches = 100

	for stmt, base := range []uint64{100000, 100} {
		proc.SetStatementLastInsertID(0)
		var wg sync.WaitGroup
		for i := 0; i < scopes; i++ {
			scope := &Process{Base: proc.Base}
			value := base + uint64(i)
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := uint64(0); j < batches; j++ {
					scope.SetStatementLastInsertIDIfLater(value + j*scopes)
				}
			}()
		}
		wg.Wait()
		want := base + scopes*batches - 1
		require.Equal(t, want, proc.GetStatementLastInsertID(), "statement %d", stmt)
		require.Equal(t, want, proc.GetLastInsertID(), "statement %d", stmt)
	}
}
