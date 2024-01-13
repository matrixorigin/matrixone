// Copyright 2021 - 2023 Matrix Origin
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

package upgrader

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"strings"
)

func ParseDataTypeToColType(dataType string) (table.ColType, error) {
	dataTypeStr := strings.ToLower(dataType)
	switch {
	case strings.Contains(dataTypeStr, "datetime"):
		return table.TDatetime, nil
	case strings.EqualFold(dataTypeStr, "bigint unsigned"):
		return table.TUint64, nil
	case strings.EqualFold(dataTypeStr, "bigint"):
		return table.TInt64, nil
	case strings.EqualFold(dataTypeStr, "int unsigned"):
		return table.TUint32, nil
	case strings.EqualFold(dataTypeStr, "int"):
		return table.TUint32, nil
	case strings.EqualFold(dataTypeStr, "double"):
		return table.TFloat64, nil
	case strings.EqualFold(dataTypeStr, "json"):
		return table.TJson, nil
	case strings.EqualFold(dataTypeStr, "text"):
		return table.TText, nil
	case strings.Contains(dataTypeStr, "varchar"):
		return table.TVarchar, nil
	case strings.Contains(dataTypeStr, "char"):
		return table.TChar, nil
	case strings.EqualFold(dataTypeStr, "uuid"):
		return table.TUuid, nil
	case strings.EqualFold(dataTypeStr, "bool"):
		return table.TBool, nil
	default:
		return table.TSkip, moerr.NewInternalError(context.Background(), "unknown data type %s", dataTypeStr)
	}
}

func attachAccount(ctx context.Context, tenant *frontend.TenantInfo) context.Context {
	return defines.AttachAccount(ctx, tenant.GetTenantID(), tenant.GetUserID(), tenant.GetDefaultRoleID())
}

func makeOptions(tenant *frontend.TenantInfo) *ie.OptsBuilder {
	return ie.NewOptsBuilder().AccountId(tenant.GetTenantID()).UserId(tenant.GetUserID()).DefaultRoleId(tenant.GetDefaultRoleID())
}

func appendSemicolon(s string) string {
	if !strings.HasSuffix(s, ";") {
		return s + ";"
	}
	return s
}

func convErrsToFormatMsg(errors []error) string {
	format := "The upgrade error message is listed below: \n"
	for i, err := range errors {
		format += fmt.Sprintf("error[%d]: %s \n", i, err.Error())
	}
	return format
}
