package options

func (o *Options) FillDefaults(dirname string) *Options {
	if o == nil {
		o = &Options{}
	}

	if o.CacheCfg == nil {
		o.CacheCfg = &CacheCfg{
			IndexCapacity:  DefaultIndexCacheSize,
			InsertCapacity: DefaultMTCacheSize,
			TxnCapacity:    DefaultTxnCacheSize,
		}
	}

	if o.StorageCfg == nil {
		o.StorageCfg = &StorageCfg{
			BlockMaxRows:     DefaultBlockMaxRows,
			SegmentMaxBlocks: DefaultBlocksPerSegment,
		}
	}

	if o.CheckpointCfg == nil {
		o.CheckpointCfg = &CheckpointCfg{
			CalibrationInterval: DefaultCalibrationInterval,
			ExecutionInterval:   DefaultExecutionInterval,
			ExecutionLevels:     DefaultExecutionLevels,
		}
	}

	if o.SchedulerCfg == nil {
		o.SchedulerCfg = &SchedulerCfg{
			IOWorkers:      DefaultIOWorkers,
			TxnTaskWorkers: DefaultTxnTaskWorkers,
		}
	}

	return o
}
