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
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"strings"
)

func attachAccount(ctx context.Context, tenant *frontend.TenantInfo) context.Context {
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, tenant.GetTenantID())
	ctx = context.WithValue(ctx, defines.UserIDKey{}, tenant.GetUserID())
	ctx = context.WithValue(ctx, defines.RoleIDKey{}, tenant.GetDefaultRoleID())
	return ctx
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
