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

// applyEnumColumnCollation only authors supported assignment identities. An
// explicit column COLLATE wins; typed reconstruction bypasses this entirely.
func applyEnumColumnCollation(ctx context.Context, typ *plan.Type, attrs []tree.ColumnAttribute, fallback enumDDLDefault) error {
	if typ.Id != int32(types.T_enum) {
		return nil
	}
	var collation string
	var charsetSpecified, collationSpecified bool
	for _, attr := range attrs {
		switch opt := attr.(type) {
		case *tree.AttributeCharset:
			charsetSpecified = true
			if !strings.EqualFold(opt.Charset, "utf8mb4") {
				return moerr.NewNotSupportedf(ctx, "ENUM CHARACTER SET %s", opt.Charset)
			}
		case *tree.AttributeCollate:
			collationSpecified = true
			collation = opt.Collate
		}
	}
	if !collationSpecified {
		collation = fallback.collation
		if collation == "" {
			return nil
		}
		if !charsetSpecified && fallback.charset != "" && !strings.EqualFold(fallback.charset, "utf8mb4") {
			return moerr.NewNotSupportedf(ctx, "ENUM cannot inherit COLLATE %s with CHARACTER SET %s", collation, fallback.charset)
		}
	}
	switch strings.ToLower(collation) {
	case "utf8mb4_bin":
		typ.EnumCollation = types.EnumCollationUTF8Bin
	case "utf8mb4_general_ci":
		typ.EnumCollation = types.EnumCollationGeneralCI
	default:
		return moerr.NewNotSupportedf(ctx, "ENUM collation %s", collation)
	}
	return nil
}
