package logservicedriver

import (
	"context"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	// "time"
)

// driver lsn -> entry lsn
//
//
func (d *LogServiceDriver) Truncate(lsn uint64) error {
	truncated := atomic.LoadUint64(&d.truncating)
	if lsn > truncated {
		atomic.StoreUint64(&d.truncating, lsn)
	}
	d.truncateQueue.Enqueue(struct{}{})
	return nil
}

func (d *LogServiceDriver) GetTruncated() (lsn uint64, err error) {
	lsn = atomic.LoadUint64(&d.truncating)
	return
}

func (d *LogServiceDriver) onTruncate(items ...any){
	d.doTruncate()
}

func (d *LogServiceDriver) doTruncate() {
	target := atomic.LoadUint64(&d.truncating)
	lastServiceLsn := d.truncatedLogserviceLsn
	lsn := lastServiceLsn
	//TODO use valid lsn
	for d.isToTruncate(lsn+1, target) {
		lsn++
	}
	if lsn == lastServiceLsn {
		return
	}
	d.truncateLogservice(lsn)
	d.truncatedLogserviceLsn = lsn
}

func (d *LogServiceDriver) truncateLogservice(lsn uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), d.config.ReadDuration)
	defer cancel()
	client, err := d.clientPool.Get()
	defer d.clientPool.Put(client)
	if err != nil {
		panic(err)
	}
	err = client.c.Truncate(ctx, lsn)
	if err != nil { //TODO
		panic(err)
	}
	logutil.Infof("truncate %d",lsn)
}
