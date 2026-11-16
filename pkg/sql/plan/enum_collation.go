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
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// RequireEnumCollationProtocol is a local rollout guard, not a cluster-wide
// old-node fence. Deployment must still stop and drain incompatible binaries.
func RequireEnumCollationProtocol(proc *process.Process, owner any) error {
	if proc != nil {
		if rt := moruntime.ServiceRuntime(proc.GetService()); rt != nil {
			value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
			if version, valid := value.(int64); ok && valid && version >= defines.MORPCVersion12 {
				return nil
			}
		}
	}
	if plan.RequiresEnumCollationProtocol(owner) {
		return moerr.NewNotSupportedNoCtx("ENUM collation metadata requires MORPC protocol version 12")
	}
	return nil
}

func enumCollationForName(ctx context.Context, name string) (uint32, error) {
	switch strings.ToLower(name) {
	case "utf8mb4_bin":
		return types.EnumCollationUTF8Bin, nil
	case "utf8mb4_general_ci":
		return types.EnumCollationGeneralCI, nil
	default:
		return 0, moerr.NewNotSupportedf(ctx, "ENUM collation %s", name)
	}
}

func enumCharsetCollationCompatible(charset, collation string) bool {
	canonical := func(name string) string {
		switch strings.ToLower(name) {
		case "utf8", "utf8mb3":
			return "utf8mb3"
		default:
			return strings.ToLower(name)
		}
	}
	if i := strings.IndexByte(collation, '_'); i >= 0 {
		collation = collation[:i]
	}
	return canonical(charset) == canonical(collation)
}

func applyEnumColumnCollation(ctx context.Context, typ *plan.Type, attrs []tree.ColumnAttribute, fallback uint32, unsupportedDefault string) error {
	if !isEnumPlanType(typ) {
		return nil
	}
	id := fallback
	var columnCharset, columnCollation string
	for _, attr := range attrs {
		switch value := attr.(type) {
		case *tree.AttributeCharset:
			if value.Charset == "" {
				return moerr.NewInvalidInput(ctx, "empty ENUM CHARACTER SET")
			}
			columnCharset = value.Charset
		case *tree.AttributeCollate:
			if value.Collate == "" {
				return moerr.NewInvalidInput(ctx, "empty ENUM COLLATE")
			}
			columnCollation = value.Collate
		}
	}
	if columnCharset != "" && columnCollation != "" && !enumCharsetCollationCompatible(columnCharset, columnCollation) {
		return moerr.NewInvalidInputf(ctx, "COLLATION '%s' is not valid for CHARACTER SET '%s'", columnCollation, columnCharset)
	}
	if columnCharset != "" && !strings.EqualFold(columnCharset, "utf8mb4") {
		return moerr.NewNotSupportedf(ctx, "ENUM CHARACTER SET %s", columnCharset)
	}
	if columnCollation != "" {
		var err error
		id, err = enumCollationForName(ctx, columnCollation)
		if err != nil {
			return err
		}
	} else if unsupportedDefault != "" {
		return moerr.NewNotSupportedf(ctx, "ENUM cannot inherit unsupported table collation %s; specify a supported column COLLATE", unsupportedDefault)
	}
	if err := types.ValidateEnumCollation(id); err != nil {
		return err
	}
	typ.EnumCollation = id
	return nil
}
