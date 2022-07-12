package logservicedriver

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logservice"
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
	GetTruncateDuration  time.Duration
	ReadDuration         time.Duration
	ClientConfig         *logservice.ClientConfig
}

type LogServiceDriver struct {
	clientPool sync.Pool
	config     *Config
	appendable *driverAppender
	*driverInfo
	*readCache

	appendQueue   chan any
	appendedQueue chan any
}

func (d *LogServiceDriver) newClient() any {
	ctx, cancel := context.WithTimeout(context.Background(), d.config.NewClientDuration)
	defer cancel()
	client, err := logservice.NewClient(ctx, *d.config.ClientConfig)
	if err != nil {
		panic(err) //TODO retry
	}
	return client
}

func NewLogServiceDriver(shardID, replicaID uint64, clientConfig *logservice.ClientConfig) *LogServiceDriver {
	d := &LogServiceDriver{}
	d.clientPool = sync.Pool{New: d.newClient}
	return d
}

func (d *LogServiceDriver) Truncate(lsn uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), d.config.TruncateDuration)
	defer cancel()
	client := d.clientPool.Get().(logservice.Client)
	err := client.Truncate(ctx, lsn)
	if err != nil { //TODO
		panic(err)
	}
	client.Close() //TODO reuse clients without close
	d.clientPool.Put(client)
	return nil
}
func (d *LogServiceDriver) GetTruncated() (lsn uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client := d.clientPool.Get().(logservice.Client)
	lsn, err := client.GetTruncatedLsn(ctx)
	if err != nil { //TODO
		panic(err)
	}
	client.Close() //TODO reuse clients without close
	d.clientPool.Put(client)
	return lsn
}
func (d *LogServiceDriver) Close() error {
	return nil
}
