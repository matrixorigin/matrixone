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

package db

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

var dbScope = common.ID{
	TableID: math.MaxUint64 / 2,
}

func MakeDBScopes(entry *catalog.DBEntry) (scopes []common.ID) {
	scope := dbScope
	scope.SegmentID = entry.GetID()
	scopes = append(scopes, scope)
	return
}

func MakeTableScopes(entries ...*catalog.TableEntry) (scopes []common.ID) {
	for _, entry := range entries {
		scopes = append(scopes, *entry.AsCommonID())
	}
	return
}

func MakeSegmentScopes(entries ...*catalog.SegmentEntry) (scopes []common.ID) {
	for _, entry := range entries {
		scopes = append(scopes, *entry.AsCommonID())
	}
	return
}

func MakeBlockScopes(entries ...*catalog.BlockEntry) (scopes []common.ID) {
	for _, entry := range entries {
		scopes = append(scopes, *entry.AsCommonID())
	}
	return
}
