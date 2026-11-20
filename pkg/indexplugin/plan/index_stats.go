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

	"github.com/matrixorigin/matrixone/pkg/indexstats"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// IndexStatsHooks is an optional capability of an algorithm's Plan hooks.
// The SQL layer resolves and authorizes the source and groups the definitions
// belonging to one named index. The owner validates its private table layout
// and fills the diagnostic configuration, including parameter defaults.
// The owner also produces the public rows for the same configuration.
// Algorithms without this capability do not support mo_index_stats.
type IndexStatsHooks interface {
	ReadIndexStats(*sqlexec.SqlProcess, indexstats.Config) (indexstats.Rows, error)
	ConfigureIndexStats(context.Context, *plan.TableDef, []*plan.IndexDef, *indexstats.Config) error
}
