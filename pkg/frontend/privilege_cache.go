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

package frontend

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
)

func (ses *Session) refreshPrivilegeCache(ctx context.Context) error {
	cache := ses.GetPrivilegeCache()
	if cache == nil {
		return nil
	}
	if enabled, err := privilegeCacheIsEnabled(ctx, ses); err != nil || !enabled {
		cache.invalidate()
		return err
	}
	pu := getPuIfPresent(ses.GetService())
	if pu == nil || ses.GetTenantInfo() == nil {
		cache.invalidate()
		return nil
	}
	storage := pu.StorageEngine
	if wrapped, ok := storage.(*engine.EntireEngine); ok {
		storage = wrapped.Engine
	}
	eng, ok := storage.(*disttae.Engine)
	if !ok {
		// Engines without a catalog-version proof retain statement-local grants.
		cache.invalidate()
		return nil
	}
	version, minimum, err := eng.GetPrivilegeCacheVersion(
		ctx, ses.GetTenantInfo().GetTenantID(), ses.getLastCommitTS())
	if err != nil {
		cache.invalidate()
		return err
	}
	ses.updateLastCommitTS(minimum)
	if version == (disttae.PrivilegeCacheVersion{}) || version != cache.catalogVersion {
		cache.invalidate()
		// Capture before evaluation. A concurrent later commit must invalidate
		// the results next time, not be stamped onto an older authorization read.
		cache.catalogVersion = version
	}
	return nil
}
