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
	catalog2 "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

var (
	// mo_table_partitions;
	//+----------------------+-----------------------+------+------+---------+-------+---------+
	//| Field                | Type                  | Null | Key  | Default | Extra | Comment |
	//+----------------------+-----------------------+------+------+---------+-------+---------+
	//| table_id             | BIGINT UNSIGNED(64)   | NO   | PRI  | NULL    |       |         |
	//| database_id          | BIGINT UNSIGNED(64)   | NO   |      | NULL    |       |         |
	//| number               | SMALLINT UNSIGNED(16) | NO   |      | NULL    |       |         |
	//| name                 | VARCHAR(64)           | NO   | PRI  | NULL    |       |         |
	//| partition_type       | VARCHAR(50)           | NO   |      | NULL    |       |         |
	//| partition_expression | VARCHAR(2048)         | YES  |      | NULL    |       |         |
	//| description_utf8     | TEXT(0)               | YES  |      | NULL    |       |         |
	//| comment              | VARCHAR(2048)         | NO   |      | NULL    |       |         |
	//| options              | TEXT(0)               | YES  |      | NULL    |       |         |
	//| partition_table_name | VARCHAR(1024)         | NO   |      | NULL    |       |         |
	//+----------------------+-----------------------+------+------+---------+-------+---------+
	tableIdCol          = table.UInt64Column("table_id", "")
	databaseIdCol       = table.UInt64Column("database_id", "")
	numberCol           = table.UInt16Column("number", "")
	nameCol             = table.UInt16Column("name", "")
	partitionTypeCol    = table.UInt16Column("partition_type", "")
	partitionExprCol    = table.UInt16Column("partition_expression", "")
	descriptionUtf8Col  = table.UInt16Column("description_utf8", "")
	commentCol          = table.UInt16Column("comment", "")
	optionsCol          = table.UInt16Column("options", "")
	partitionTblNameCol = table.UInt16Column("partition_table_name", "")

	MoTablePartitionsTable = &table.Table{
		Account:  table.AccountSys,
		Database: catalog2.MO_CATALOG,
		Table:    catalog2.MO_TABLE_PARTITIONS,
		Columns: []table.Column{
			tableIdCol,
			databaseIdCol,
			numberCol,
			nameCol,
			partitionTypeCol,
			partitionExprCol,
			descriptionUtf8Col,
			commentCol,
			optionsCol,
			partitionTblNameCol,
		},
		PrimaryKeyColumn: []table.Column{tableIdCol, nameCol},
		ClusterBy:        nil,
		// Engine
		Engine:        table.NormalTableEngine, // XXX ???
		Comment:       "",
		PathBuilder:   nil,
		AccountColumn: nil,
		// TimestampColumn
		TimestampColumn: nil,
		// SupportUserAccess
		SupportUserAccess: true,
		// SupportConstAccess
		SupportConstAccess: true,
	}
)

var needUpgradNewTable = []*table.Table{MoTablePartitionsTable}
