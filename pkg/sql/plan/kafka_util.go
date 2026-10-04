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
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	sqlkafka "github.com/matrixorigin/matrixone/pkg/sql/kafka"
)

// IsKafkaTableDef reports whether tableDef is a Kafka external table and
// returns its parsed config. Both the planner-owned envelope and the durable
// feature bit must agree: the envelope alone could be forged through the
// user-controlled rel_createsql JSON of a generic external table.
func IsKafkaTableDef(ctx context.Context, tableDef *TableDef) (sqlkafka.Config, bool, error) {
	if tableDef == nil || tableDef.TableType != catalog.SystemExternalRel {
		return sqlkafka.Config{}, false, nil
	}
	cfg, found, err := sqlkafka.ParseCreateSQLEnvelope(ctx, tableDef.Createsql)
	if err != nil {
		return sqlkafka.Config{}, false, err
	}
	if !found {
		if features.IsKafkaExternal(tableDef.FeatureFlag) {
			return sqlkafka.Config{}, false, moerr.NewInvalidInput(ctx, "kafka external table is missing its catalog envelope")
		}
		return sqlkafka.Config{}, false, nil
	}
	if !features.IsKafkaExternal(tableDef.FeatureFlag) {
		return sqlkafka.Config{}, false, moerr.NewInvalidInput(ctx, "kafka envelope present on a table without the kafka feature flag")
	}
	return cfg, true, nil
}
