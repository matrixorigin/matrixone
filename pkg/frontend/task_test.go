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

// TestHandlePauseDaemonTask_TaskServiceNil tests the error case in handlePauseDaemonTask function
// when the task service is nil
func TestHandlePauseDaemonTask_TaskServiceNil(t *testing.T) {
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
			service:   "test-service",
			accountId: 1,
		},
	}

	// Create a PauseDaemonTask statement
	pauseStmt := &tree.PauseDaemonTask{
		TaskID: 123,
	}

	// Test the handlePauseDaemonTask function
	err := handlePauseDaemonTask(context.Background(), session, pauseStmt)

	// Verify that the expected error is returned
	require.Error(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	assert.Contains(t, err.Error(), "task service not ready yet, please try again later.")
}

// TestHandleCancelDaemonTask_TaskServiceNil tests the error case in handleCancelDaemonTask function
// when the task service is nil
func TestHandleCancelDaemonTask_TaskServiceNil(t *testing.T) {
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
			service:   "test-service",
			accountId: 1,
		},
	}

	// Test the handleCancelDaemonTask function
	err := handleCancelDaemonTask(context.Background(), session, 123)

	// Verify that the expected error is returned
	require.Error(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	assert.Contains(t, err.Error(), "task service not ready yet, please try again later.")
}

// TestHandleResumeDaemonTask_TaskServiceNil tests the error case in handleResumeDaemonTask function
// when the task service is nil
func TestHandleResumeDaemonTask_TaskServiceNil(t *testing.T) {
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
			service:   "test-service",
			accountId: 1,
		},
	}

	// Create a ResumeDaemonTask statement
	resumeStmt := &tree.ResumeDaemonTask{
		TaskID: 123,
	}

	// Test the handleResumeDaemonTask function
	err := handleResumeDaemonTask(context.Background(), session, resumeStmt)

	// Verify that the expected error is returned
	require.Error(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	assert.Contains(t, err.Error(), "task service not ready yet, please try again later.")
}
