// Copyright 2022 Matrix Origin
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

package runtime

import (
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
)

// The names of all global variables should be defined here.
const (
	// ClusterService cluster service
	ClusterService = "cluster-service"
	// ClusterService cluster service
	LockService = "lock-service"
	// CtlService ctl service
	CtlService = "ctl-service"
	// InternalSQLExecutor attr name for internal sql executor
	InternalSQLExecutor = "internal-sql-executor"
	// AutoIncrmentService attr name for AutoIncrmentService
	AutoIncrmentService = "auto-incrment-service"

	// TxnOptions options used to create txn
	TxnOptions = "txn-options"
	// TxnMode runtime default txn mode
	TxnMode = "txn-mode"
	// TxnIsolation runtime default txn isolation
	TxnIsolation = "txn-isolation"
)

// Runtime contains the runtime environment for a MO service. Each CN/DN/LOG service
// needs to receive a Runtime and will pass the Runtime to all components of the service.
// These Runtimes may only be created in main or integration test framework.
//
// Because most of our BVT tests and integration tests are run in a single mo-service
// process, which runs multiple CN, DN and LOG services, the Runtime cannot be set as a
// global variable, otherwise we would not be able to set a single Runtime for each service
// and each component.
//
// In other words, there are no global variables inside mo-service, and the scope of global
// variables is in a Runtime.
//
// Since each component holds a Runtime, all mo-service-level parameters should not appear in
// the parameter list of the component's initialization function, but should be replaced by the
// Runtime's global variables.
//
// Unfortunately, we can't fully support the isolation of the service-level runtime between services
// and they still share a process-level runtime, but at least there is an advantage in that all
// service-level components are placed in the runtime, so that the configuration and components can be
// easily retrieved from the runtime without having to pass them as parameters, so that when adding
// service-level components in the future, there is no need to modify the parameter list.
type Runtime interface {
	// ServiceType return service type
	ServiceType() metadata.ServiceType
	// ServiceUUID return service uuid
	ServiceUUID() string
	// Logger returns the top-level logger is set at the start of the MO and contains
	// the service type and unique ID of the service; all subsequent loggers must be
	// built on this logger.
	Logger() *log.MOLogger
	// SubLogger returns sub-loggers used for different purposes.
	SubLogger(LoggerName) *log.MOLogger
	// Clock returns the Clock instance of the current runtime environment
	Clock() clock.Clock
	// SetGlobalVariables set global variables which scope based in runtime.
	SetGlobalVariables(name string, value any)
	// GetGlobalVariables get global variables, return false if variables not found.
	GetGlobalVariables(name string) (any, bool)
}

// Option used to setup runtime
type Option func(*runtime)
