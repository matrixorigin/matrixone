// Copyright 2023 Matrix Origin
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

package incrservice

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
)

const (
	defaultCountPerAllocate = 10000
	// MaxAutoIDCache bounds the user-configured reservation span, not memory usage.
	MaxAutoIDCache uint64 = 1000000
)

// Config auto increment config
type Config struct {
	// EnableAutoIDCache is an operator opt-in after upgrading every CN/TN.
	// It is immutable for this service lifetime, not a rolling-upgrade admission proof.
	EnableAutoIDCache bool `toml:"enable-auto-id-cache"`
	// CountPerAllocate how many ids are cached in the current cn node for each assignment
	CountPerAllocate int `toml:"count-per-allocate"`
	// LowCapacity when the remaining number of ids is less than this value, the current cn
	// node will initiate asynchronous task assignment in advance
	LowCapacity int `toml:"low-capacity"`
	// demandOnly is set by AUTO_ID_CACHE=1, never by the service TOML defaults.
	demandOnly bool
}

func (c Config) forTable(ctx context.Context, size uint64) (Config, error) {
	if err := validateAutoIDCacheSize(ctx, size); err != nil {
		return Config{}, err
	}
	if size != 0 {
		if !c.EnableAutoIDCache {
			return Config{}, autoIDCacheDisabled(ctx)
		}
		c.CountPerAllocate = int(size)
		c.LowCapacity = int(size / 2)
		c.demandOnly = size == 1
	}
	return c, nil
}

func validateAutoIDCacheSize(ctx context.Context, size uint64) error {
	if size > MaxAutoIDCache {
		return moerr.NewInvalidInputf(ctx, "AUTO_ID_CACHE must be between 0 and %d", MaxAutoIDCache)
	}
	return nil
}

func autoIDCacheDisabled(ctx context.Context) error {
	return moerr.NewNotSupported(ctx, "AUTO_ID_CACHE is disabled; enable cn.auto-increment.enable-auto-id-cache only after upgrading every CN and TN")
}

func checkAutoIDCacheProtocol(ctx context.Context, sid string, size uint64) error {
	if size == 0 {
		return nil
	}
	rt := runtime.ServiceRuntime(sid)
	if rt != nil {
		value, _ := rt.GetGlobalVariables(runtime.MOProtocolVersion)
		if version, ok := value.(int64); ok && version >= defines.MORPCVersion63 {
			return nil
		}
	}
	return moerr.NewNotSupported(ctx, "AUTO_ID_CACHE requires MORPC protocol version 63")
}

// CheckAutoIDCache rejects nonzero policies before DDL or remote operator
// publication. Legacy/unknown service implementations fail closed without
// widening the allocator's public interface or changing the zero/default path.
func CheckAutoIDCache(ctx context.Context, sid string, size uint64) error {
	if err := validateAutoIDCacheSize(ctx, size); err != nil || size == 0 {
		return err
	}
	rt := runtime.ServiceRuntime(sid)
	if rt == nil {
		return autoIDCacheDisabled(ctx)
	}
	value, _ := rt.GetGlobalVariables(runtime.AutoIncrementService)
	enabler, ok := value.(interface{ AutoIDCacheEnabled() bool })
	if !ok || !enabler.AutoIDCacheEnabled() {
		return autoIDCacheDisabled(ctx)
	}
	return checkAutoIDCacheProtocol(ctx, sid, size)
}

func (c *Config) adjust() {
	if c.CountPerAllocate == 0 {
		c.CountPerAllocate = defaultCountPerAllocate
	}
	if c.LowCapacity == 0 ||
		c.LowCapacity > c.CountPerAllocate {
		c.LowCapacity = c.CountPerAllocate / 2
	}
}
