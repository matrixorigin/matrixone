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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	sqlmongodb "github.com/matrixorigin/matrixone/pkg/sql/mongodb"
)

func ensureMongoDBTableSurfaceEnabled(ctx context.Context) error {
	value := ctx.Value(config.ParameterUnitKey)
	pu, _ := value.(*config.ParameterUnit)
	if pu == nil || pu.SV == nil {
		return moerr.NewNotSupported(ctx, "MongoDB external tables are disabled because runtime configuration is unavailable")
	}
	parameters := pu.SV.MongoDB
	if !parameters.Enable {
		return moerr.NewNotSupported(ctx, "MongoDB external tables are disabled")
	}
	if !parameters.EnablePerAccount {
		return nil
	}
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	if accountID == 0 {
		return nil
	}
	for _, allowed := range parameters.AllowedAccounts {
		if allowed == accountID {
			return nil
		}
	}
	return moerr.NewNotSupported(ctx, "MongoDB external tables are disabled for this account")
}

func IsMongoDBTableDef(ctx context.Context, tableDef *TableDef) (bool, error) {
	if tableDef == nil || tableDef.TableType != catalog.SystemExternalRel {
		return false, nil
	}
	_, found, err := sqlmongodb.ParseCreateSQLEnvelope(ctx, tableDef.Createsql)
	if err != nil {
		return false, err
	}
	return found, nil
}
