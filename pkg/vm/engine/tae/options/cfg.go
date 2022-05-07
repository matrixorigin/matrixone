package options

type CacheCfg struct {
	IndexCapacity  uint64 `toml:"index-cache-size"`
	InsertCapacity uint64 `toml:"insert-cache-size"`
	TxnCapacity    uint64 `toml:"txn-cache-size"`
}

type StorageCfg struct {
	BlockMaxRows     uint32 `toml:"block-max-rows"`
	SegmentMaxBlocks uint16 `toml:"segment-max-blocks"`
}

type CheckpointCfg struct {
	ScannerInterval    int64 `toml:"scanner-inerterval"`
	ExecutionInterval  int64 `toml:"execution-inerterval"`
	ExecutionLevels    int16 `toml:"execution-levels"`
	CatalogUnCkpLimit  int64 `toml:"catalog-unckp-limit"`
	CatalogCkpInterval int64 `toml:"catalog-ckp-interval"`
}

type SchedulerCfg struct {
	IOWorkers    int `toml:"io-workers"`
	AsyncWorkers int `toml:"async-workers"`
}
