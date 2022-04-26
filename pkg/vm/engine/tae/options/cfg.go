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
	CalibrationInterval int64 `toml:"calibration-inerterval"`
	ExecutionInterval   int64 `toml:"execution-inerterval"`
}
