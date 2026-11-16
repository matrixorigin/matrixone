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

package plan

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/collation"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// These names carry historical metadata in MatrixOne SQL replay. They are
// not MySQL collations and are not admitted for generic text or sessions.
const (
	legacyEnumCollationName  = collation.LegacyEnumReplayName
	legacyTableCollationName = "__mo_table_legacy_main_v1"
)

func hasLegacyEnumColumn(table *planpb.TableDef) bool {
	if table == nil || table.IsTemporary || (table.TableType != "" && table.TableType != catalog.SystemOrdinaryRel) {
		return false
	}
	for _, col := range table.Cols {
		if col != nil && !col.Hidden && isEnumPlanType(&col.Typ) && col.Typ.EnumCollation == 0 {
			return true
		}
	}
	return false
}

func hasLegacyTableReplayOption(options []tree.TableOption) bool {
	for _, option := range options {
		if collate, ok := option.(*tree.TableOptionCollate); ok && strings.EqualFold(collate.Collate, legacyTableCollationName) {
			return true
		}
	}
	return false
}

func requireEnumCollationMetadataProtocol(ctx context.Context, proc *process.Process, owner any, authoring bool) error {
	if proc != nil {
		if rt := moruntime.ServiceRuntime(proc.GetService()); rt != nil && persistedProtocolRuntimeAllows(rt, defines.MORPCVersion135) {
			if !authoring {
				return nil
			}
			floor, present := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor)
			version, valid := floor.(int64)
			if !present || valid && version >= defines.MORPCVersion135 {
				return nil
			}
		}
	}
	features, err := planpb.RequiredRemoteExpressionFeatures(owner)
	if err != nil || !features.EnumCollation {
		return err
	}
	if authoring {
		return RequirePersistedProtocolVersionForAuthoring(ctx, proc, defines.MORPCVersion135)
	}
	return RequirePersistedProtocolVersion(ctx, proc, defines.MORPCVersion135)
}

// Generic string DDL intentionally admits some compatibility aliases (notably
// 0900 -> general_ci). An ENUM may only claim an implemented equality domain.
func exactEnumCollationName(name string) bool {
	switch strings.ToLower(name) {
	case "utf8mb4_bin", "utf8mb4_general_ci", "utf8mb4_unicode_ci":
		return true
	default:
		return false
	}
}

func enumCharsetCollationCompatible(charset, name string) bool {
	if i := strings.IndexByte(name, '_'); i > 0 {
		return collation.CanonicalCharsetName(charset) == collation.CanonicalCharsetName(name[:i])
	}
	return true
}

// This marker records the original spelling only when it is available. A
// previously normalized database default cannot recover an original alias.
func enumUnsupportedTableDefault(ctx CompilerContext, options []tree.TableOption, useServer bool) string {
	var name string
	hasCharset := false
	var charsetName string
	for _, option := range options {
		switch opt := option.(type) {
		case *tree.TableOptionCharset:
			hasCharset = true
			charsetName = opt.Charset
			if opt.Collate != "" {
				name = opt.Collate
			}
		case *tree.TableOptionCollate:
			name = opt.Collate
		}
	}
	if strings.EqualFold(name, legacyTableCollationName) {
		return ""
	}
	charset := strings.ToLower(charsetName)
	if charset == "utf8" {
		charset = "utf8mb3"
	}
	if charset == "utf8mb3" || charset == "utf32" {
		// Generic DDL accepts these as compatibility aliases. Preserve the
		// requested family for ENUM inheritance, even if the explicit COLLATE
		// was normalized to the same generic identity by legacy DDL handling.
		switch strings.ToLower(name) {
		case "", "utf8mb4_general_ci":
			name = charset + "_general_ci"
		case "utf8mb4_bin":
			name = charset + "_bin"
		}
	}
	if name == "" && !hasCharset && useServer {
		if value, err := ctx.ResolveVariable("collation_server", true, false); err == nil {
			name, _ = value.(string)
		}
	}
	if name != "" && !exactEnumCollationName(name) {
		return name
	}
	return ""
}
