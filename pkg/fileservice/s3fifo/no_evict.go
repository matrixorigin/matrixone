// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package s3fifo

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

// SetWithoutEviction retains and publishes a reserved fill only when it fits,
// using ordinary S3-FIFO queue placement and notifications, without eviction.
// admit runs under ChargeGuard, when set, and must not re-enter the cache.
func (d *DataCache) SetWithoutEviction(ctx context.Context, key fscache.CacheKey, data fscache.Data, admit func() bool) (inserted bool, err error) {
	logical, size := data.Size(), data.Capacity()
	if logical < 0 || size < logical {
		panic("cache data reports an invalid logical size or backing capacity")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	c := d.c
	hash := c.opts.Hash(key)
	ps, ix := c.policyOf(hash), c.indexOf(hash)
	value := DataValue{Data: data, LogicalSize: logical}
	value.Retain()
	defer func() {
		if !inserted {
			value.Release()
		}
	}()
	e := &entry[fscache.CacheKey, DataValue]{key: key, hash: hash, value: value, size: size}
	var rejected bool
	inserted, rejected = func() (bool, bool) {
		ix.mu.Lock()
		defer ix.mu.Unlock()
		if _, cached := ix.index[key]; cached {
			return false, false
		}
		if c.opts.ChargeGuard != nil {
			c.opts.ChargeGuard.Lock()
			defer c.opts.ChargeGuard.Unlock()
		}
		capacity := c.capacity()
		if c.opts.Admission != nil {
			if _, pressure := c.opts.Admission(capacity); pressure {
				return false, true
			}
		}
		if admit != nil && !admit() {
			return false, true
		}
		for {
			used := c.used.Load()
			if size > capacity-used {
				return false, true
			}
			if c.used.CompareAndSwap(used, used+size) {
				break
			}
		}
		guarded := c.opts.ChargeGuard != nil && (c.opts.Guarded == nil || c.opts.Guarded(value))
		if guarded && c.opts.OnCharge != nil {
			committed := false
			defer func() {
				if !committed {
					c.used.Add(-size)
				}
			}()
			c.opts.OnCharge(value)
			committed = true
		}
		e.seq = c.seq.Add(1)
		ix.index[key] = e
		ix.addToGroup(c.opts.Group, e)
		return true, false
	}()
	if rejected {
		return false, fscache.ErrCacheAdmissionRejected
	}
	if inserted {
		c.queue(ps, e, value, size, e.seq, false)
	}
	return inserted, nil
}
