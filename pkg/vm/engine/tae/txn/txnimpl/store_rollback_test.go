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

package txnimpl

import (
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/stretchr/testify/require"
)

type rollbackEntry struct {
	called bool
	err    error
}

func (*rollbackEntry) PrepareCommit() error                     { return nil }
func (e *rollbackEntry) PrepareRollback() error                 { e.called = true; return e.err }
func (*rollbackEntry) ApplyCommit(string) error                 { return nil }
func (*rollbackEntry) ApplyRollback() error                     { return nil }
func (*rollbackEntry) MakeCommand(uint32) (txnif.TxnCmd, error) { return nil, nil }

func TestPrepareRollbackContinuesAcrossDatabases(t *testing.T) {
	firstErr := errors.New("first cleanup failed")
	secondErr := errors.New("second cleanup failed")
	first := &rollbackEntry{err: firstErr}
	second := &rollbackEntry{err: secondErr}
	store := &txnStore{dbs: map[uint64]*txnDB{
		1: {createEntry: first},
		2: {createEntry: second},
	}}
	err := store.PrepareRollback()
	require.ErrorIs(t, err, firstErr)
	require.ErrorIs(t, err, secondErr)
	require.True(t, first.called)
	require.True(t, second.called)
}
