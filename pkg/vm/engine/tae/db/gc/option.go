package gc

import "time"

type Option func(*DiskCleaner)

func WithTryGCInterval(interval time.Duration) Option {
	return func(dc *DiskCleaner) {
		dc.options.tryGCInterval = interval
	}
}
