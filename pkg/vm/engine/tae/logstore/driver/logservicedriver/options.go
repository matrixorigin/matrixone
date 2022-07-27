package logservicedriver

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

var DefaultReadMaxSize = uint64(10)

type Config struct {
	ClientPoolMaxSize int
	ClientPoolInitSize int
	RecordSize           int
	ReadCacheSize        int
	AppenderMaxCount     int
	ReadMaxSize          uint64
	NewRecordSize        int
	NewClientDuration    time.Duration
	ClientAppendDuration time.Duration
	TruncateDuration     time.Duration
	AppendFrequency      time.Duration
	GetTruncateDuration  time.Duration
	ReadDuration         time.Duration

	ClientConfig         *logservice.ClientConfig
}

func newDefaultConfig(cfg *logservice.ClientConfig) *Config {
	return &Config{
		ClientPoolMaxSize: 100,
		ClientPoolInitSize: 100,
		RecordSize:           int(common.K * 16),
		ReadCacheSize:        100,
		ReadMaxSize:          common.K * 20,
		AppenderMaxCount:     100,
		NewRecordSize:        int(common.K * 20),
		NewClientDuration:    time.Second,
		AppendFrequency:      time.Millisecond * 5,
		ClientAppendDuration: time.Second,
		TruncateDuration:     time.Second,
		GetTruncateDuration:  time.Second,
		ReadDuration:         time.Second,
		ClientConfig:         cfg,
	}
}

func NewTestConfig(cfg *logservice.ClientConfig) *Config {
	return &Config{
		ClientPoolMaxSize: 10,
		ClientPoolInitSize: 5,
		RecordSize:           int(common.M*10),
		ReadCacheSize:        10,
		ReadMaxSize:          common.K * 20,
		AppenderMaxCount:     1,
		NewRecordSize:        int(common.K * 20),
		AppendFrequency:      time.Millisecond /1000,
		NewClientDuration:    time.Second,
		ClientAppendDuration: time.Second,
		TruncateDuration:     time.Second,
		GetTruncateDuration:  time.Second,
		ReadDuration:         time.Second,
		ClientConfig:         cfg,
	}
}
