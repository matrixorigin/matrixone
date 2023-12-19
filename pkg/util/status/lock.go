// Copyright 2021 -2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package status

import (
	"github.com/matrixorigin/matrixone/pkg/lockservice"
)

type LockItem struct {
	TableID  uint64   `json:"table_id"`
	Keys     [][]byte `json:"keys"`
	LockInfo string   `json:"lock_info"`
}

type LockStatus struct {
	Locks []LockItem `json:"locks"`
}

func (s *Status) fillLock(cnStatus *CNStatus, ls lockservice.LockService) {
	ls.IterLocks(func(tableID uint64, keys [][]byte, lock lockservice.Lock) bool {
		cnStatus.LockStatus.Locks = append(cnStatus.LockStatus.Locks, LockItem{
			TableID:  tableID,
			Keys:     keys,
			LockInfo: lock.String(),
		})
		return true
	})
}
