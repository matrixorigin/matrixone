//go:build mo_m0

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

package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var ErrM0SchemaIdentityUnavailable error = moerr.NewInternalErrorNoCtx("M0: object writer schema identity unavailable")

// M0ObjectSchemaVersion reads the existing version independently of a writer
// table ID. A proof based on restricted append transitions need not resolve a
// complete historical writer schema.
func M0ObjectSchemaVersion(h Header) (uint32, error) {
	if len(h) < HeaderSize || h.Magic() != uint64(Magic) {
		return 0, moerr.NewInternalErrorNoCtx("M0: invalid object header")
	}
	return types.DecodeUint32(h[8+2+ExtentSize : 8+2+ExtentSize+4]), nil
}

// M0ObjectSchemaIdentity reads existing persisted fields, without inferring a
// writer schema from MaxSeqnum or the destination table's current definition.
// The returned identity still requires a historical-schema lookup. In B's
// ordinary writer, TableID is zero, so its objects deliberately fail this gate.
// The active mo_m0 proof therefore uses the relation-owned schema/version
// contract instead of resolving a global writer schema.
func M0ObjectSchemaIdentity(h Header, meta ObjectDataMeta) (tableID uint64, version uint32, err error) {
	version, err = M0ObjectSchemaVersion(h)
	if err != nil {
		return 0, 0, err
	}
	if len(meta) < int(headerLen) {
		return 0, 0, moerr.NewInternalErrorNoCtx("M0: invalid object metadata")
	}
	tableID = meta.BlockHeader().TableID()
	if tableID == 0 {
		return 0, 0, ErrM0SchemaIdentityUnavailable
	}
	return tableID, version, nil
}
