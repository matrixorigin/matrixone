// Copyright 2026 Matrix Origin
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

package gc

import (
	"context"
	"encoding/base64"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

// SidecarReadProtector adapts Sirius read leases to the exact object filter
// used by TAE GC. Registration inherits SyncProtectionManager's GC-running
// exclusion and bounded-entry checks.
type SidecarReadProtector struct{ Manager *SyncProtectionManager }

func (p SidecarReadProtector) Begin(_ context.Context) (
	func(context.Context, []byte, []string, time.Time) error,
	func(context.Context, []byte) error,
	func(),
	error,
) {
	if p.Manager == nil {
		return nil, nil, nil, moerr.NewInternalErrorNoCtxf("sidecar read protector has no manager")
	}
	guard, err := p.Manager.BeginProtection()
	if err != nil {
		return nil, nil, nil, err
	}
	register := func(_ context.Context, readRef []byte, objects []string, expires time.Time) error {
		return registerSidecarReadProtection(guard, readRef, objects, expires)
	}
	rollback := func(_ context.Context, readRef []byte) error {
		return guard.RollbackSyncProtection(sidecarReadJobID(readRef))
	}
	return register, rollback, guard.Close, nil
}

// Register keeps direct callers and crash replay idempotent while sharing the
// same atomic GC barrier as admission.
func (p SidecarReadProtector) Register(ctx context.Context, readRef []byte, objects []string, expires time.Time) error {
	register, _, closeProtection, err := p.Begin(ctx)
	if err != nil {
		return err
	}
	defer closeProtection()
	return register(ctx, readRef, objects, expires)
}

func registerSidecarReadProtection(guard *SyncProtectionGuard, readRef []byte, objects []string, expires time.Time) error {
	if len(readRef) == 0 {
		return moerr.NewInternalErrorNoCtxf("sidecar read protector has empty reference")
	}
	vec := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	defer vec.Close()
	if len(objects) == 0 {
		objects = []string{"__sidecar_empty_table__"}
	}
	for _, name := range objects {
		if name == "" {
			return moerr.NewInternalErrorNoCtxf("sidecar read protector has empty object name")
		}
		vec.Append([]byte(name), false)
	}
	bf, err := index.NewBloomFilter(vec, nil, nil, nil)
	if err != nil {
		return err
	}
	data, err := bf.Marshal()
	if err != nil {
		return err
	}
	jobID := sidecarReadJobID(readRef)
	return guard.EnsureExpiringSyncProtection(jobID, base64.StdEncoding.EncodeToString(data), expires.UnixNano(), jobID)
}

func (p SidecarReadProtector) Unregister(_ context.Context, readRef []byte) error {
	if p.Manager == nil {
		return moerr.NewInternalErrorNoCtxf("sidecar read protector has no manager")
	}
	return p.Manager.ReleaseSyncProtection(sidecarReadJobID(readRef))
}

func sidecarReadJobID(readRef []byte) string {
	return "sidecar-read/" + base64.RawURLEncoding.EncodeToString(readRef)
}
