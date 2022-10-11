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

package memtable

type Transaction struct {
	ID              string
	BeginTime       Time
	Time            Time
	CommitTime      Time
	State           *Atomic[TransactionState]
	IsolationPolicy IsolationPolicy
	committers      map[TxCommitter]struct{}
}

type TxCommitter interface {
	CommitTx(*Transaction) error
	AbortTx(*Transaction)
}

func NewTransaction(
	id string,
	t Time,
	isolationPolicy IsolationPolicy,
) *Transaction {
	return &Transaction{
		ID:              id,
		BeginTime:       t,
		Time:            t,
		State:           NewAtomic[TransactionState](Active),
		IsolationPolicy: isolationPolicy,
		committers:      make(map[TxCommitter]struct{}),
	}
}

type TransactionState uint8

const (
	Active = iota
	Committed
	Aborted
)

func (t *Transaction) Commit(commitTime Time) error {
	t.CommitTime = commitTime
	for committer := range t.committers {
		if err := committer.CommitTx(t); err != nil {
			return err
		}
	}
	t.State.Store(Committed)
	return nil
}

func (t *Transaction) Abort() {
	for committer := range t.committers {
		committer.AbortTx(t)
	}
	t.State.Store(Aborted)
}

func (t *Transaction) Copy() *Transaction {
	newTx := *t
	newTx.State = new(Atomic[TransactionState])
	newTx.State.Store(t.State.Load())
	newTx.committers = make(map[TxCommitter]struct{})
	return &newTx
}
