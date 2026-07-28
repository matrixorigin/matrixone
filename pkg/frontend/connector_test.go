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

package frontend

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// TestShowConnectors_TaskServiceNil tests the error case in showConnectors function
// when the task service is nil (line 362 in connector.go)
func TestShowConnectors_TaskServiceNil(t *testing.T) {
	// Initialize server level vars for the test service
	InitServerLevelVars("test-service")

	// Create a parameter unit with nil task service
	pu := &config.ParameterUnit{
		SV: &config.FrontendParameters{},
	}
	// Don't set the task service, so it will be nil

	// Set the parameter unit for the service
	setPu("test-service", pu)

	// Create a session that implements FeSession
	session := &Session{
		feSessionImpl: feSessionImpl{
			service: "test-service",
			mrs:     &MysqlResultSet{},
		},
	}

	// Test the showConnectors function
	err := showConnectors(context.Background(), session)

	// Verify that the expected error is returned
	require.Error(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	assert.Contains(t, err.Error(), "task service not ready yet, please try again later.")
}

// TestHandleCreateConnector_TaskServiceNil tests the error case in handleCreateConnector function
// when the task service is nil (line 107 in connector.go)
func TestHandleCreateConnector_TaskServiceNil(t *testing.T) {
	// Initialize server level vars for the test service
	InitServerLevelVars("test-service")

	// Create a parameter unit with nil task service
	pu := &config.ParameterUnit{
		SV: &config.FrontendParameters{},
	}
	// Don't set the task service, so it will be nil

	// Set the parameter unit for the service
	setPu("test-service", pu)

	// Create a session that implements FeSession
	session := &Session{
		feSessionImpl: feSessionImpl{
			service: "test-service",
		},
	}

	// Create a CreateConnector statement
	prefix := tree.ObjectNamePrefix{
		SchemaName:     tree.Identifier("test_db"),
		ExplicitSchema: true,
	}
	tableName := tree.NewTableName(tree.Identifier("test_table"), prefix, nil)

	createConnectorStmt := &tree.CreateConnector{
		TableName: tableName,
		Options:   []*tree.ConnectorOption{},
	}

	// Test the handleCreateConnector function
	err := handleCreateConnector(context.Background(), session, createConnectorStmt)

	// Verify that the expected error is returned
	require.Error(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	assert.Contains(t, err.Error(), "no task service is found")
}
