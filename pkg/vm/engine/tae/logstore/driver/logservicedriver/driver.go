package logservicedriver

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
)

var DefaultReadMaxSize = uint64(10)

type Config struct {
	RecordSize           int
	ReadCacheSize        int
	AppenderMaxCount     int
	ReadMaxSize          uint64
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
		AppenderMaxCount:     100,
		NewClientDuration:    time.Second,
		AppendFrequency:      time.Millisecond * 5,
		ClientAppendDuration: time.Second,
		TruncateDuration:     time.Second,
		GetTruncateDuration:  time.Second,
		ReadDuration:         time.Second,
		ClientConfig:         cfg,
	}
}

func newTestConfig(cfg *logservice.ClientConfig) *Config {
	return &Config{
		RecordSize:           int(common.K),
		ReadCacheSize:        10,
		AppenderMaxCount:     10,
		AppendFrequency:      time.Millisecond * 5,
		NewClientDuration:    time.Second,
		ClientAppendDuration: time.Second,
		TruncateDuration:     time.Second,
		GetTruncateDuration:  time.Second,
		ReadDuration:         time.Second,
		ClientConfig:         cfg,
	}
}

type LogServiceDriver struct {
	clients    []logservice.Client
	clientPool sync.Pool
	config     *Config
	appendable *driverAppender
	*driverInfo
	*readCache

	wg              sync.WaitGroup
	closeCtx        context.Context
	closeCancel     context.CancelFunc
	preAppendQueue  chan any
	preAppendLoop   *sm.Loop
	appendQueue     chan any
	appendLoop      *sm.Loop
	appendedQueue   chan any
	appendedLoop    *sm.Loop
	postAppendQueue chan any
	postAppendLoop  *sm.Loop
}

func NewLogServiceDriver(cfg *Config) *LogServiceDriver {
	d := &LogServiceDriver{
		clients:         make([]logservice.Client, 0),
		config:          cfg,
		appendable:      newDriverAppender(),
		driverInfo:      newDriverInfo(),
		readCache:       newReadCache(),
		preAppendQueue:  make(chan any, 100),
		appendQueue:     make(chan any, 100),
		appendedQueue:   make(chan any, 100),
		postAppendQueue: make(chan any, 100),
	}
	d.closeCtx, d.closeCancel = context.WithCancel(context.Background())
	d.preAppendLoop = sm.NewLoop(d.preAppendQueue, nil, d.onPreAppend, 100)
	d.preAppendLoop.Start()
	d.appendLoop = sm.NewLoop(d.appendQueue, d.appendedQueue, d.onAppendQueue, 100)
	d.appendLoop.Start()
	d.appendedLoop = sm.NewLoop(d.appendedQueue, d.postAppendQueue, d.onAppendedQueue, 100)
	d.appendedLoop.Start()
	d.postAppendLoop = sm.NewLoop(d.postAppendQueue, nil, d.onPostAppendQueue, 100)
	d.postAppendLoop.Start()
	d.clientPool = sync.Pool{New: d.newClient}
	d.wg.Add(1)
	go d.appendTicker()
	return d
}

func (d *LogServiceDriver) appendTicker() {
	ticker := time.NewTicker(d.config.AppendFrequency)
	for {
		select {
		case <-d.closeCtx.Done():
			d.wg.Done()
			return
		case <-ticker.C:
			d.flushAppend()
		}
	}
}

func (d *LogServiceDriver) newClient() any {
	ctx, cancel := context.WithTimeout(context.Background(), d.config.NewClientDuration)
	defer cancel()
	client, err := logservice.NewClient(ctx, *d.config.ClientConfig)
	if err != nil {
		panic(err) //TODO retry
	}
	d.clients = append(d.clients, client)
	return client
}

func (d *LogServiceDriver) Close() error {
	for _, c := range d.clients {
		c.Close()
	}
	d.closeCancel()
	d.preAppendLoop.Stop()
	d.appendLoop.Stop()
	d.appendedLoop.Stop()
	d.postAppendLoop.Stop()
	d.wg.Wait()
	return nil
}
