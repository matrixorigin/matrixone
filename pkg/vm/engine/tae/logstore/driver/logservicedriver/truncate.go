package logservicedriver

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logservice"
)

func (d *LogServiceDriver) Truncate(lsn uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), d.config.TruncateDuration)
	defer cancel()
	client := d.clientPool.Get().(logservice.Client)
	err := client.Truncate(ctx, lsn)
	if err != nil { //TODO
		panic(err)
	}
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
	d.clientPool.Put(client)
	return lsn
}