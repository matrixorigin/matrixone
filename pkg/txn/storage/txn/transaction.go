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

package txnstorage

import (
	"fmt"
	"strings"
)

type Transaction struct {
	ID              string
	BeginTime       Time
	CurrentTime     Time
	State           *Atomic[TransactionState]
	IsolationPolicy IsolationPolicy
}

func NewTransaction(id string, t Time, isolationPolicy IsolationPolicy) *Transaction {
	return &Transaction{
		ID:              id,
		BeginTime:       t,
		CurrentTime:     t,
		State:           NewAtomic[TransactionState](Active),
		IsolationPolicy: isolationPolicy,
	}
}

type TransactionState uint8

const (
	Active = iota
	Committed
	Aborted
)

type ErrWriteConflict struct {
	WritingTx *Transaction
	// if Locked is not nil, the row is locked by another uncommitted transaction
	// caller may wait after the Locked transactiion is committed or aborted, or abort without waiting
	Locked *Transaction
	// if Stale is not nil, the row is updated by another committed transaction after the WritingTx has started
	// caller should abort the WritingTx to avoid lost-update phenomena
	Stale *Transaction
}

func (e ErrWriteConflict) Error() string {
	buf := new(strings.Builder)
	buf.WriteString(fmt.Sprintf("write conflict: %s", e.WritingTx.ID))
	if e.Locked != nil {
		buf.WriteString(fmt.Sprintf(" ,locked by %s", e.Locked.ID))
	}
	if e.Stale != nil {
		buf.WriteString(fmt.Sprintf(" ,stale after %s", e.Stale.ID))
	}
	return buf.String()
}

type ErrReadConflict struct {
	ReadingTx *Transaction
	// if Stale is not nil, the row is updated by another committed transaction after the ReadingTx has started
	// caller should abort the ReadingTx to avoid stale read
	Stale *Transaction
}

func (e ErrReadConflict) Error() string {
	buf := new(strings.Builder)
	buf.WriteString(fmt.Sprintf("read conflict: %s", e.ReadingTx.ID))
	if e.Stale != nil {
		buf.WriteString(fmt.Sprintf(" ,stale after %s", e.Stale.ID))
	}
	return buf.String()
}

func (t *Transaction) Tick() {
	t.CurrentTime = t.CurrentTime.Next()
}
