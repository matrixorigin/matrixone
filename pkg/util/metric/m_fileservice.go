package metric

var FsMemCacheReadCounter = NewCounterVec(
	CounterOpts{
		Subsystem: "fs",
		Name:      "mem_cache_read_total",
		Help:      "Counter for Memcache Reads",
	},
	[]string{constTenantKey, "type"},
	false,
)

var FsS3ReadCounter = NewCounterVec(
	CounterOpts{
		Subsystem: "fs",
		Name:      "s3_read_total",
		Help:      "Counter for S3 Reads",
	},
	[]string{constTenantKey, "type"},
	false,
)

func MemCacheReadCounter() Counter {
	return FsMemCacheReadCounter.WithLabelValues()
}

func S3ReadCounter() Counter {
	return FsS3ReadCounter.WithLabelValues()
}
