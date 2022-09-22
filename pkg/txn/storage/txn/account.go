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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	txnengine "github.com/matrixorigin/matrixone/pkg/vm/engine/txn"
)

func (m *MemHandler) ensureAccount(
	tx *Transaction,
	accessInfo txnengine.AccessInfo,
) (
	err error,
) {

	// ensure catalog db exists for this account
	keys, err := m.databases.Index(tx, Tuple{
		index_AccountID_Name,
		Uint(accessInfo.AccountID),
		Text(catalog.SystemDBName),
	})
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		// create one
		db := &DatabaseRow{
			ID:        txnengine.NewID(),
			AccountID: accessInfo.AccountID,
			Name:      catalog.SystemDBName,
		}
		err := m.databases.Insert(tx, db)
		//TODO add a unique constraint on (AccountID, Name)
		if err != nil {
			return err
		}
	}

	return
}
