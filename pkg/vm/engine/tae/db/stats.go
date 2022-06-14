package db

import (
	"encoding/json"

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
	MaxTS  uint64
	MaxID  uint64
	SafeTS uint64
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
		MaxTS:  mgr.TsAlloc.Get(),
		MaxID:  mgr.IdAlloc.Get(),
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
