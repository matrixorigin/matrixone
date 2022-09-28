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

package memorystorage

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
)

func (m *MemHandler) ensureAccount(
	tx *Transaction,
	accessInfo memoryengine.AccessInfo,
) (
	err error,
) {

	// ensure catalog db exists for this account
	keys, err := m.databases.Index(tx, Tuple{
		index_AccountID_Name,
		Uint(accessInfo.AccountID),
		Text(catalog.MO_CATALOG),
	})
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		// create one
		db := &DatabaseRow{
			ID:        memoryengine.NewID(),
			AccountID: accessInfo.AccountID,
			Name:      catalog.MO_CATALOG,
		}
		err := m.databases.Insert(tx, db)
		//TODO add a unique constraint on (AccountID, Name)
		if err != nil {
			return err
		}
	}

	return
}
