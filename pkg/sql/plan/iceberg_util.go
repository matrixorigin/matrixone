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
	sqliceberg "github.com/matrixorigin/matrixone/pkg/sql/iceberg"
)

func IsIcebergTableDef(ctx context.Context, tableDef *TableDef) (bool, error) {
	if tableDef == nil || tableDef.TableType != catalog.SystemExternalRel {
		return false, nil
	}
	_, found, err := sqliceberg.ParseCreateSQLEnvelope(ctx, tableDef.Createsql)
	if err != nil {
		return false, err
	}
	return found, nil
}
