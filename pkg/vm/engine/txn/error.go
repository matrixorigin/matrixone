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

package txnengine

import (
	"errors"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

type TxnError struct {
	txnError *txn.TxnError
}

var _ error = TxnError{}

func (e TxnError) Error() string {
	if e.txnError != nil {
		return e.txnError.DebugString()
	}
	panic("impossible")
}

func errorFromTxnResponses(resps []txn.TxnResponse) error {
	for _, resp := range resps {
		if resp.TxnError != nil {
			return TxnError{
				txnError: resp.TxnError,
			}
		}
	}
	return nil
}

type ErrExisted bool

var _ error = ErrExisted(true)

func (e ErrExisted) Error() string {
	return "existed"
}

type ErrDatabaseNotFound struct {
	ID   string
	Name string
}

var _ error = ErrDatabaseNotFound{}

func (e ErrDatabaseNotFound) Error() string {
	return fmt.Sprintf("database not found: [%s] [%s]", e.Name, e.ID)
}

type ErrRelationNotFound struct {
	ID   string
	Name string
}

var _ error = ErrRelationNotFound{}

func (e ErrRelationNotFound) Error() string {
	return fmt.Sprintf("relation not found: [%s] [%s]", e.Name, e.ID)
}

type ErrDefNotFound struct {
	ID   string
	Name string
}

var _ error = ErrDefNotFound{}

func (e ErrDefNotFound) Error() string {
	return fmt.Sprintf("definition not found: [%s] [%s]", e.Name, e.ID)
}

type ErrIterNotFound struct {
	ID string
}

var _ error = ErrIterNotFound{}

func (e ErrIterNotFound) Error() string {
	return fmt.Sprintf("iter not found: %s", e.ID)
}

type ErrColumnNotFound struct {
	Name string
}

var _ error = ErrColumnNotFound{}

func (e ErrColumnNotFound) Error() string {
	return fmt.Sprintf("column not found: %s", e.Name)
}

type Errors []error

var _ error = Errors{}

func (e Errors) Error() string {
	buf := new(strings.Builder)
	for i, err := range e {
		if i > 0 {
			buf.WriteRune('\n')
		}
		buf.WriteString(err.Error())
	}
	return buf.String()
}

func (e Errors) As(target any) bool {
	for _, err := range e {
		if errors.As(err, target) {
			return true
		}
	}
	return false
}
