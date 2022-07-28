package logservicedriver

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
)


func RetryWithTimeout(timeoutDuration time.Duration, fn func()(shouldReturn bool)) error{
	ctx,cancel:=context.WithTimeout(context.Background(),timeoutDuration)
	defer cancel()
	for{
		select{
		case <-ctx.Done():
			return ErrRetryTimeOut
		default:
			if fn(){
				return nil
			}
		}
	}
}

type LogServiceDriver struct {
	clientPool *clientpool
	config     *Config
	appendable *driverAppender
	*driverInfo
	*readCache

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
	clientpoolConfig:=&clientConfig{
		cancelDuration: cfg.NewClientDuration,
		recordSize: cfg.NewRecordSize,
		logserviceClientConfig: cfg.ClientConfig,
		GetClientRetryTimeOut: cfg.GetClientRetryTimeOut,
	}
	d := &LogServiceDriver{
		clientPool: newClientPool(cfg.ClientPoolMaxSize,cfg.ClientPoolMaxSize,clientpoolConfig),
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
	return d
}

func (d *LogServiceDriver) Close() error {
	logutil.Infof("append%d,flush%d",d.appendtimes,d.flushtimes)
	d.clientPool.Close()
	d.closeCancel()
	d.preAppendLoop.Stop()
	d.appendedLoop.Stop()
	d.postAppendLoop.Stop()
	return nil
}

func (d *LogServiceDriver) Replay(h driver.ApplyHandle) error {panic("TODO")}