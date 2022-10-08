// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package db

import (
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type Stats struct {
	db           *DB
	CatalogStats *CatalogStats
	TxnStats     *TxnStats
	WalStats     *WalStats
}

func NewStats(db *DB) *Stats {
	return &Stats{
		db: db,
	}
}

func (stats *Stats) Collect() {
	stats.CatalogStats = CollectCatalogStats(stats.db.Catalog)
	stats.TxnStats = CollectTxnStats(stats.db.TxnMgr)
	stats.WalStats = CollectWalStats(stats.db.Wal)
}

func (stats *Stats) ToString(prefix string) string {
	buf, err := json.MarshalIndent(stats, prefix, "  ")
	if err != nil {
		panic(err)
	}
	return string(buf)
}

type CatalogStats struct {
	MaxDBID uint64
	MaxTID  uint64
	MaxSID  uint64
	MaxBID  uint64
}

type TxnStats struct {
	MaxTS  types.TS
	MaxID  uint64
	SafeTS types.TS
}

type WalStats struct {
	MaxLSN     uint64
	MaxCkped   uint64
	PendingCnt uint64
}

func CollectCatalogStats(c *catalog.Catalog) *CatalogStats {
	return &CatalogStats{
		MaxDBID: c.CurrDB(),
		MaxTID:  c.CurrTable(),
		MaxSID:  c.CurrSegment(),
		MaxBID:  c.CurrBlock(),
	}
}

func CollectTxnStats(mgr *txnbase.TxnManager) *TxnStats {
	return &TxnStats{
		//MaxTS:  mgr.TsAlloc.Get(),
		//MaxID:  mgr.IdAlloc.Get(),
		SafeTS: mgr.StatSafeTS(),
	}
}

func CollectWalStats(w wal.Driver) *WalStats {
	return &WalStats{
		MaxLSN:     w.GetCurrSeqNum(),
		MaxCkped:   w.GetCheckpointed(),
		PendingCnt: w.GetPenddingCnt(),
	}
}
