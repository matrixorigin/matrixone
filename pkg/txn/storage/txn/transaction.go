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

import "fmt"

type Transaction struct {
	ID          string
	BeginTime   Timestamp
	CurrentTime Timestamp
	State       TransactionState
}

func NewTransaction(id string, t Timestamp) *Transaction {
	return &Transaction{
		ID:          id,
		BeginTime:   t,
		CurrentTime: t,
		State:       Active,
	}
}

type TransactionState uint8

const (
	Active = iota
	Committed
	Aborted
)

type ErrWriteConflict struct {
	WritingTx     *Transaction
	ConflictingTx *Transaction
}

func (e ErrWriteConflict) Error() string {
	return fmt.Sprintf("write conflict: %s %s", e.WritingTx.ID, e.ConflictingTx.ID)
}

func (t *Transaction) Tick() {
	t.CurrentTime = t.CurrentTime.Next()
}
