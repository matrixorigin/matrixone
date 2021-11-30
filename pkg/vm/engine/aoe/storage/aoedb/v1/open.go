// Copyright 2021 Matrix Origin
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

package aoedb

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"
)

func OpenWithWalBroker(dirname string, opts *storage.Options) (inst *DB, err error) {
	if opts.Wal != nil && opts.Wal.GetRole() != wal.BrokerRole {
		return nil, db.ErrUnexpectedWalRole
	}
	opts.WalRole = wal.BrokerRole
	return Open(dirname, opts)
}

func Open(dir string, opts *storage.Options) (inst *DB, err error) {
	impl, err := db.Open(dir, opts)
	if err != nil {
		return nil, err
	}

	inst = &DB{Impl: *impl}
	return
}
