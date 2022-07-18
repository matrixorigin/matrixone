package config

import "time"

const (
	DefaultTickPerSecond   = 10
	DefaultLogStoreTimeout = 5 * time.Minute
	DefaultDnStoreTimeout  = 10 * time.Second
)

type TimeoutConfig struct {
	TickPerSecond int

	LogStoreTimeout time.Duration

	DnStoreTimeout time.Duration
}

func DefaultTimeoutConfig() *TimeoutConfig {
	return &TimeoutConfig{
		TickPerSecond:   DefaultTickPerSecond,
		LogStoreTimeout: DefaultLogStoreTimeout,
		DnStoreTimeout:  DefaultDnStoreTimeout,
	}
}

func (config *TimeoutConfig) SetTickPerSecond(tickPerSecond int) *TimeoutConfig {
	config.TickPerSecond = tickPerSecond
	return config
}

func (config *TimeoutConfig) SetLogStoreTimeout(timeout time.Duration) *TimeoutConfig {
	config.LogStoreTimeout = timeout
	return config
}

func (config *TimeoutConfig) SetDnStoreTimeout(timeout time.Duration) *TimeoutConfig {
	config.DnStoreTimeout = timeout
	return config
}

func (config *TimeoutConfig) LogStoreExpired(start, current uint64) bool {
	return uint64(int(config.LogStoreTimeout/time.Second)*config.TickPerSecond)+start < current
}

func (config *TimeoutConfig) DnStoreExpired(start, current uint64) bool {
	return uint64(int(config.DnStoreTimeout/time.Second)*config.TickPerSecond)+start < current
}
