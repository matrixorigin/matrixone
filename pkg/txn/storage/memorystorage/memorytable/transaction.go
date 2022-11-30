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

package memorytable

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// Transaction represents a transaction
// a transaction may contains multiple operations on multiple tables
// a transaction commits atomically
type Transaction struct {
	State     *Atomic[TransactionState]
	BeginTime Time
	tables    map[int64]*transactionTable
}

type transactionTable struct {
	table interface {
		sync.Locker
		commit(tx *Transaction, state any, commitTime Time) error
		abort(*Transaction) error
	}
	state atomic.Value
}

// NewTransaction creates a new transaction using beginTime as snapshot time
func NewTransaction(
	beginTime Time,
) *Transaction {
	return &Transaction{
		State:     NewAtomic(Active),
		BeginTime: beginTime,
		tables:    make(map[int64]*transactionTable),
	}
}

// TransactionState represents state of a transaction
type TransactionState uint8

// String returns the text form of a state
func (t TransactionState) String() string {
	switch t {
	case Active:
		return "active"
	case Committed:
		return "committed"
	case Aborted:
		return "aborted"
	}
	panic(fmt.Sprintf("unknown state: %v", uint8(t)))
}

const (
	// Active is the default state of a newly created transaction
	Active TransactionState = iota
	// Committed is the state of a committed transaction
	Committed
	// Aborted is the state of a aborted transaction
	Aborted
)

// Commit commits the transaction
func (t *Transaction) Commit(commitTime Time) error {
	if state := t.State.Load(); state != Active {
		return moerr.NewTxnNotActive(state.String())
	}
	for _, table := range t.tables {
		table.table.Lock()
		defer table.table.Unlock()
		if err := table.table.commit(t, table.state.Load(), commitTime); err != nil {
			return err
		}
	}
	t.State.Store(Committed)
	return nil
}

// must call with t being locked
func (t *Table[K, V, R]) commit(tx *Transaction, state any, commitTime Time) error {

	currentState := t.state.Load()
	txState := state.(*tableState[K, V])
	newState, err := currentState.merge(txState)
	if err != nil {
		return err
	}
	t.state.Store(newState)
	if !commitTime.IsEmpty() && len(t.history) > 0 {
		last := t.history[len(t.history)-1]
		if !commitTime.Greater(last.Before) {
			return moerr.NewInternalError("commit time too old")
		}
	}
	t.history = append(t.history, &history[K, V]{
		Before: commitTime,
		State:  currentState,
	})

	return nil
}

// Abort aborts the transaction
func (t *Transaction) Abort() error {
	for _, table := range t.tables {
		table.table.Lock()
		defer table.table.Unlock()
		if err := table.table.abort(t); err != nil {
			return err
		}
	}
	t.State.Store(Aborted)
	return nil
}

func (t *Table[K, V, R]) abort(tx *Transaction) error {
	return nil
}
