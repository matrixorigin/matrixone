// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/collationkey"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// validateUniqueKeyCodecAdmission validates relation-local codec metadata at
// the common DML table-resolution boundary. A relation with no metadata is a
// legacy relation and keeps its existing bytewise path. A v2 relation is
// rejected until the sidecar, lock, transaction, query, and migration
// consumers are all enabled together; admitting one writer early could create
// a physical identity that an older reader cannot interpret.
func validateUniqueKeyCodecAdmission(ctx context.Context, tableDef *planpb.TableDef) error {
	if tableDef == nil || tableDef.UniqueKeyCodecVersion == nil {
		return nil
	}
	version := tableDef.UniqueKeyCodecVersion
	metadata := collationkey.RelationMetadata{
		Version:            version.Value,
		RegistryVersion:    version.RegistryVersion,
		RegistryDigest:     version.RegistryDigest,
		MaxEncodedKeyBytes: version.MaxEncodedKeyBytes,
	}
	if err := metadata.Validate(); err != nil {
		return moerr.NewInternalErrorf(ctx, "invalid unique-key codec metadata: %v", err)
	}
	if metadata.IsV2() {
		return moerr.NewUnsupportedDML(ctx, "collation-aware unique-key writes are not enabled")
	}
	return nil
}

// validateUniqueKeyCodecReadAdmission is the read-side companion to the DML
// fence above.  A v2 relation cannot be opened by a planner that does not yet
// have the versioned point-probe, scan, and fallback consumers wired through
// the same activation generation.  Rejecting the read here prevents a legacy
// reader from silently applying bytewise predicates to a relation whose
// physical identity is collation-aware.
func validateUniqueKeyCodecReadAdmission(ctx context.Context, tableDef *planpb.TableDef) error {
	if tableDef == nil || tableDef.UniqueKeyCodecVersion == nil {
		return nil
	}
	version := tableDef.UniqueKeyCodecVersion
	metadata := collationkey.RelationMetadata{
		Version:            version.Value,
		RegistryVersion:    version.RegistryVersion,
		RegistryDigest:     version.RegistryDigest,
		MaxEncodedKeyBytes: version.MaxEncodedKeyBytes,
	}
	if err := metadata.Validate(); err != nil {
		return moerr.NewInternalErrorf(ctx, "invalid unique-key codec metadata: %v", err)
	}
	if metadata.IsV2() {
		return moerr.NewUnsupportedDML(ctx, "collation-aware unique-key reads are not enabled")
	}
	return nil
}
