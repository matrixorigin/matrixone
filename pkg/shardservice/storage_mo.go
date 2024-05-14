// Copyright 2021-2024 Matrix Origin
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

package shardservice

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
)

var (
	ShardTableSQL = fmt.Sprintf(`create table %s.%s(
		table_id 		bigint       not null,
		policy          varchar(50)  not null,
		count           unsigned int not null,
		version         unsigned int not null,
		primary key(table_id)
	)`, catalog.MO_CATALOG, catalog.MOShardTable)

	InitSQLs = []string{
		ShardTableSQL,
	}
)
