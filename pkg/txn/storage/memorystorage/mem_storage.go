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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
)

func NewMemoryStorage(
	mheap *mpool.MPool,
	clock clock.Clock,
	idGenerator memoryengine.IDGenerator,
) (*Storage, error) {

	memHandler := NewMemHandler(mheap, clock, idGenerator)
	catalogHandler, err := NewCatalogHandler(memHandler)
	if err != nil {
		return nil, err
	}
	storage, err := New(catalogHandler)
	if err != nil {
		return nil, err
	}
	return storage, nil

}
