package logservicedriver

import (
	"context"
	// "time"
)

// driver lsn -> entry lsn
// 
//
func (d *LogServiceDriver) Truncate(lsn uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), d.config.TruncateDuration)
	defer cancel()
	client := d.truncateClient
	err := client.c.Truncate(ctx, lsn)
	if err != nil { //TODO
		panic(err)
	}
	return nil
}


func (d *LogServiceDriver) GetTruncated() (lsn uint64, err error) {
	// ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	// defer cancel()
	// client := d.clientPool.Get().(*clientWithRecord)
	// lsn, err = client.c.GetTruncatedLsn(ctx)
	// if err != nil { //TODO
	// 	panic(err)
	// }
	// d.clientPool.Put(client)
	return lsn, nil
}
