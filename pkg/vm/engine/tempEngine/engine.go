// Copyright 2021 Matrix Origin
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

package tempengine

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func NewTempEngine() *TempEngine {
	return &TempEngine{
		tempDatabase: &TempDatabase{
			nameToRelation: make(map[string]*TempRelation),
		},
	}
}

// Delete deletes a database
func (tempEngine *TempEngine) Delete(ctx context.Context, databaseName string, op client.TxnOperator) error {
	return nil
}

// Create creates a database
func (tempEngine *TempEngine) Create(ctx context.Context, databaseName string, op client.TxnOperator) error {
	return nil
}

// Databases returns all database names
func (tempEngine *TempEngine) Databases(ctx context.Context, op client.TxnOperator) (databaseNames []string, err error) {
	return nil, nil
}

// Database creates a handle for a database
func (tempEngine *TempEngine) Database(ctx context.Context, databaseName string, op client.TxnOperator) (engine.Database, error) {
	tempEngine.tempDatabase.currDbName = databaseName
	return tempEngine.tempDatabase, nil
}

// Nodes returns all nodes for worker jobs
func (tempEngine *TempEngine) Nodes() (cnNodes engine.Nodes, err error) {
	return nil, nil
}

// Hints returns hints of engine features
// return value should not be cached
// since implementations may update hints after engine had initialized
func (tempEngine *TempEngine) Hints() (h engine.Hints) {
	h.CommitOrRollbackTimeout = time.Minute
	return
}
