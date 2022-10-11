// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logservicedriver

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	// "time"
)

// driver lsn -> entry lsn
func (d *LogServiceDriver) Truncate(lsn uint64) error {
	if lsn > d.truncating.Load() {
		d.truncating.Store(lsn)
	}
	_, err := d.truncateQueue.Enqueue(struct{}{})
	if err != nil {
		panic(err)
	}
	return nil
}

func (d *LogServiceDriver) GetTruncated() (lsn uint64, err error) {
	lsn = d.truncating.Load()
	return
}

func (d *LogServiceDriver) onTruncate(items ...any) {
	d.doTruncate()
}

func (d *LogServiceDriver) doTruncate() {
	target := d.truncating.Load()
	lastServiceLsn := d.truncatedLogserviceLsn
	lsn := lastServiceLsn
	//TODO use valid lsn
	next := d.getNextValidLogserviceLsn(lsn)
	for d.isToTruncate(next, target) {
		lsn = next
		next = d.getNextValidLogserviceLsn(lsn)
		if next <= lsn {
			break
		}
	}
	if lsn == lastServiceLsn {
		return
	}
	d.truncateLogservice(lsn)
	d.truncatedLogserviceLsn = lsn
}

func (d *LogServiceDriver) truncateLogservice(lsn uint64) {
	client, err := d.clientPool.Get()
	if err == ErrClientPoolClosed {
		return
	}
	if err != nil {
		panic(err)
	}
	defer d.clientPool.Put(client)
	ctx, cancel := context.WithTimeout(context.Background(), d.config.ReadDuration)
	err = client.c.Truncate(ctx, lsn)
	cancel()
	if err != nil {
		err = RetryWithTimeout(d.config.RetryTimeout, func() (shouldReturn bool) {
			ctx, cancel := context.WithTimeout(context.Background(), d.config.ReadDuration)
			err = client.c.Truncate(ctx, lsn)
			cancel()
			return err == nil
		})
		panic(err)
	}
	logutil.Infof("Logservice Driver: Truncate %d", lsn)
}
func (d *LogServiceDriver) getLogserviceTruncate() (lsn uint64) {
	client, err := d.clientPool.Get()
	if err == ErrClientPoolClosed {
		return
	}
	if err != nil {
		panic(err)
	}
	defer d.clientPool.Put(client)
	ctx, cancel := context.WithTimeout(context.Background(), d.config.ReadDuration)
	lsn, err = client.c.GetTruncatedLsn(ctx)
	cancel()
	if err != nil {
		err = RetryWithTimeout(d.config.RetryTimeout, func() (shouldReturn bool) {
			ctx, cancel := context.WithTimeout(context.Background(), d.config.ReadDuration)
			lsn, err = client.c.GetTruncatedLsn(ctx)
			cancel()
			return err == nil
		})
		panic(err)
	}
	logutil.Infof("Logservice Driver: Get Truncate %d", lsn)
	return
}
