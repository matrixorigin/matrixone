package metric

var FsMemCacheReadCounter = NewCounter(CounterOpts{
	Subsystem:   "fs",
	Name:        "mem_cache_read_total",
	Help:        "Counter for Memcache Reads",
	ConstLabels: sysTenantID,
})

var FsS3ReadCounter = NewCounter(CounterOpts{
	Subsystem:   "fs",
	Name:        "s3_read_total",
	Help:        "Counter for S3 Reads",
	ConstLabels: sysTenantID,
})

func MemCacheReadCounter() Counter {
	return FsMemCacheReadCounter
}

func S3ReadCounter() Counter {
	return FsS3ReadCounter
}
