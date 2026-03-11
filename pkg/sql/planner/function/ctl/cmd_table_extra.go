// Copyright 2024 Matrix Origin
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

package ctl

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// handleTableExtra handles the table-extra command to get table's extra information
// parameter format: "dbName.tableName" or tableId
func handleTableExtra(
	proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender,
) (Result, error) {
	if service != cn {
		return Result{}, moerr.NewWrongServiceNoCtx("CN", string(service))
	}

	var rel engine.Relation
	var err error

	// Check if parameter is a pure number (tableId)
	if tableId, err := strconv.ParseUint(parameter, 10, 64); err == nil {
		// Parameter is a tableId
		txnOp := proc.GetTxnOperator()
		if txnOp == nil {
			return Result{}, moerr.NewInternalError(proc.Ctx, "txn operator is nil")
		}

		// Get table relation by tableId
		_, _, rel, err = proc.GetSessionInfo().StorageEngine.GetRelationById(proc.Ctx, txnOp, tableId)
		if err != nil {
			return Result{}, err
		}
	} else {
		// Parse parameter to get database and table name
		parts := strings.Split(parameter, ".")
		if len(parts) != 2 {
			return Result{}, moerr.NewInvalidInputNoCtx("invalid parameter format, expected 'dbName.tableName' or tableId")
		}
		dbName := parts[0]
		tableName := parts[1]

		// Get table relation
		txnOp := proc.GetTxnOperator()
		if txnOp == nil {
			return Result{}, moerr.NewInternalError(proc.Ctx, "txn operator is nil")
		}

		database, err := proc.GetSessionInfo().StorageEngine.Database(proc.Ctx, dbName, txnOp)
		if err != nil {
			return Result{}, err
		}

		rel, err = database.Relation(proc.Ctx, tableName, nil)
		if err != nil {
			return Result{}, err
		}
	}

	// Get table's extra info
	extraInfo := rel.GetExtraInfo()
	if extraInfo == nil {
		return Result{
			Method: TableExtra,
			Data:   "{}",
		}, nil
	}

	// Convert to JSON string
	jsonBytes, err := json.Marshal(extraInfo)
	if err != nil {
		return Result{}, err
	}

	return Result{
		Method: TableExtra,
		Data:   string(jsonBytes),
	}, nil
}
