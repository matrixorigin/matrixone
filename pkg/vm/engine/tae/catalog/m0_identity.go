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

package catalog

import (
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

// resolveM0ObjectSchema is the first provenance gate, not permission to fill a
// missing column. A later reader must also validate the read/write evolution
// and each block against this schema. This rejected-route fixture is not used
// by the active append-domain proof.
func resolveM0ObjectSchema(owner *TableEntry, h objectio.Header, meta objectio.ObjectDataMeta) (*Schema, error) {
	tableID, version, err := objectio.M0ObjectSchemaIdentity(h, meta)
	if err != nil {
		return nil, err
	}
	if owner == nil || owner.ID != tableID {
		return nil, wrapM0Error(objectio.ErrM0SchemaIdentityUnavailable, "source relation %d is not resolved", tableID)
	}
	schema := owner.GetVersionSchema(version)
	if schema == nil {
		return nil, wrapM0Error(objectio.ErrM0SchemaIdentityUnavailable, "source relation %d schema %d is not retained", tableID, version)
	}
	if err := validateM0Schema(schema); err != nil {
		return nil, err
	}
	return schema, nil
}
