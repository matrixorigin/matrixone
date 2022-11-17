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

package log

import "math"

// ServiceType used to describe which type of service the log belongs to
type ServiceType string

var (
	// CN cn service type
	CN = ServiceType("cn-service")
	// DN dn service type
	DN = ServiceType("dn-service")
	// LOG log service type
	LOG = ServiceType("log-service")
)

// Module used to describe which component module the log belongs to
type Module string

var (
	// TxnClient txn client module
	TxnClient = Module("txn-client")
)

var (
	// FieldNameServiceUUID service uuid field name
	FieldNameServiceUUID = "uuid"
	// FieldNameProcess process of the mo, e.g. transaction
	FieldNameProcess = "process"
	// FieldNameProcessID the log of a processing process may be distributed in
	// many places, we can search all the logs associated with the current process
	// by process-id. For example, we can use the transaction ID as process-id to
	// find all logs of this transaction in the cluster.
	FieldNameProcessID = "process-id"
	// FieldNameCost cost field name, this field is used to log how long a function,
	// operation or other action takes
	FieldNameCost = "cost"
)

// Process used to describe which process the log belongs to. We can filter out all
// process-related logs by specifying the process field as the value to analyse the
// logs.
type Process string

var (
	// SystemInit system init process
	SystemInit = Process("system-init")
	// Txn transaction process
	Txn = Process("txn")
	// Close close serivce or components process. e.g. close cn/dn/log service
	Close = Process("close")
)

// SampleType there are some behaviours in the system that print debug logs frequently,
// such as the scheduling of HAKeeper, which may print hundreds of logs a second. What
// these logs do is that these behaviours are still happening at debug level. So we just
// need to sample the output.
type SampleType int

var (
	noneSample = SampleType(0)
	// ExampleSample used in examples
	ExampleSample = SampleType(math.MaxInt)
)

// logFilter used to filter the print log, returns false to abort this print
type logFilter func(ctx LogContext) bool
