package logservicedriver

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
)

type clientWithRecord struct {
	c      logservice.Client
	record logservice.LogRecord
}

func (c *clientWithRecord) Close() {
	c.c.Close()
}

var DefaultReadMaxSize = uint64(10)

type Config struct {
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

type LogServiceDriver struct {
	clients    []*clientWithRecord
	// clientPool sync.Pool
	config     *Config
	appendable *driverAppender
	*driverInfo
	*readCache

	appendClient *clientWithRecord
	readClient *clientWithRecord
	truncateClient *clientWithRecord

	closeCtx        context.Context
	closeCancel     context.CancelFunc
	preAppendLoop   sm.Queue
	appendQueue     chan any
	appendedQueue   chan any
	appendedLoop    *sm.Loop
	postAppendQueue chan any
	postAppendLoop  *sm.Loop

	flushtimes int
	appendtimes int
}

func NewLogServiceDriver(cfg *Config) *LogServiceDriver {
	d := &LogServiceDriver{
		clients:         make([]*clientWithRecord, 0),
		config:          cfg,
		appendable:      newDriverAppender(),
		driverInfo:      newDriverInfo(),
		readCache:       newReadCache(),
		appendQueue:     make(chan any, 10000),
		appendedQueue:   make(chan any, 10000),
		postAppendQueue: make(chan any, 10000),
	}
	d.closeCtx, d.closeCancel = context.WithCancel(context.Background())
	d.preAppendLoop = sm.NewSafeQueue(10000, 10000, d.onPreAppend)
	d.preAppendLoop.Start()
	d.appendedLoop = sm.NewLoop(d.appendedQueue, d.postAppendQueue, d.onAppendedQueue, 10000)
	d.appendedLoop.Start()
	d.postAppendLoop = sm.NewLoop(d.postAppendQueue, nil, d.onPostAppendQueue, 10000)
	d.postAppendLoop.Start()

	d.appendClient=d.newClient()
	d.readClient=d.newClient()
	d.truncateClient=d.newClient()
	return d
}

func (d *LogServiceDriver) newClient() *clientWithRecord {
	ctx, cancel := context.WithTimeout(context.Background(), d.config.NewClientDuration)
	defer cancel()
	logserviceClient, err := logservice.NewClient(ctx, *d.config.ClientConfig)
	if err != nil {
		panic(err) //TODO retry
	}
	c := &clientWithRecord{
		c:      logserviceClient,
		record: logserviceClient.GetLogRecord(d.config.NewRecordSize),
	}
	d.clients = append(d.clients, c)
	return c
}

func (d *LogServiceDriver) Close() error {
	logutil.Infof("append%d,flush%d",d.appendtimes,d.flushtimes)
	for _, c := range d.clients {
		c.Close()
	}
	d.closeCancel()
	d.preAppendLoop.Stop()
	d.appendedLoop.Stop()
	d.postAppendLoop.Stop()
	return nil
}
