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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type enumDDLDefault struct {
	charset, collation string
}

// The options in this CREATE statement are authoritative for its new columns.
// 4.2 does not persist table/database collation defaults, so later ALTER must
// not infer a default from CREATE text or from any preexisting ENUM column.
func enumCreateTableDefault(options []tree.TableOption) enumDDLDefault {
	var d enumDDLDefault
	for _, option := range options {
		switch opt := option.(type) {
		case *tree.TableOptionCharset:
			d.charset = opt.Charset
		case *tree.TableOptionCollate:
			d.collation = opt.Collate
		}
	}
	return d
}

// getDDLColumnType authors a column definition. Typed reconstruction takes
// its domain from the catalog and does not enter this path.
func getDDLColumnType(ctx context.Context, col *tree.ColumnTableDef, fallback enumDDLDefault) (plan.Type, error) {
	decl, ok := col.Type.(*tree.T)
	if !ok || decl.InternalType.Oid != uint32(defines.MYSQL_TYPE_ENUM) {
		return getTypeFromAst(ctx, col.Type)
	}
	identity, err := resolveEnumColumnCollation(ctx, col.Attributes, fallback)
	if err != nil {
		return plan.Type{}, err
	}
	return makeEnumType(ctx, decl.InternalType.EnumValues, identity)
}

func makeEnumType(ctx context.Context, members []string, identity uint32) (plan.Type, error) {
	if len(members) > types.MaxEnumLen {
		return plan.Type{}, moerr.NewNYI(ctx, "enum type out of max length")
	}
	if len(members) == 0 {
		return plan.Type{}, moerr.NewNYI(ctx, "enum type length err")
	}
	if identity != types.EnumCollationLegacy {
		normalized := make([]string, len(members))
		keys := make(map[string]struct{}, len(members))
		for i, member := range members {
			member = strings.TrimRight(member, " ")
			key, err := types.EnumMemberKey(identity, member)
			if err != nil {
				return plan.Type{}, err
			}
			if _, duplicate := keys[key]; duplicate {
				return plan.Type{}, moerr.NewInvalidInputf(ctx, "duplicate ENUM member %q", member)
			}
			keys[key] = struct{}{}
			normalized[i] = member
		}
		members = normalized
	}
	payload, encoding, err := types.EncodeEnumMembers(members)
	if err != nil {
		return plan.Type{}, err
	}
	return plan.Type{
		Id: int32(types.T_enum), Enumvalues: payload, EnumEncoding: encoding, EnumCollation: identity,
	}, nil
}

// An explicit column COLLATE overrides the default in its CREATE statement.
func resolveEnumColumnCollation(ctx context.Context, attrs []tree.ColumnAttribute, fallback enumDDLDefault) (uint32, error) {
	var collation string
	var charsetSpecified, collationSpecified bool
	for _, attr := range attrs {
		switch opt := attr.(type) {
		case *tree.AttributeCharset:
			charsetSpecified = true
			if !strings.EqualFold(opt.Charset, "utf8mb4") {
				return 0, moerr.NewNotSupportedf(ctx, "ENUM CHARACTER SET %s", opt.Charset)
			}
		case *tree.AttributeCollate:
			collationSpecified = true
			collation = opt.Collate
		}
	}
	if !collationSpecified {
		collation = fallback.collation
		if collation == "" {
			return types.EnumCollationLegacy, nil
		}
		if !charsetSpecified && fallback.charset != "" && !strings.EqualFold(fallback.charset, "utf8mb4") {
			return 0, moerr.NewNotSupportedf(ctx, "ENUM cannot inherit COLLATE %s with CHARACTER SET %s", collation, fallback.charset)
		}
	}
	switch strings.ToLower(collation) {
	case "utf8mb4_bin":
		return types.EnumCollationUTF8Bin, nil
	case "utf8mb4_general_ci":
		return types.EnumCollationGeneralCI, nil
	default:
		return 0, moerr.NewNotSupportedf(ctx, "ENUM collation %s", collation)
	}
}
