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

package log_test

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"go.uber.org/zap"
)

func ExampleGetServiceLogger() {
	logger := getServiceLogger(metadata.ServiceType_CN, "cn0")
	logger.Info("this is a info log")
	// Output logs:
	// 2022/11/17 15:25:49.375367 +0800 INFO cn log/logger.go:51 this is a info log {"uuid": "cn0"}
}

func ExampleGetModuleLogger() {
	cnServiceLogger := getServiceLogger(metadata.ServiceType_CN, "cn0")
	txnClientLogger := log.GetModuleLogger(cnServiceLogger, log.TxnClient)
	txnClientLogger.Info("this is a info log")
	// Output logs:
	// 2022/11/17 15:27:24.562799 +0800 INFO cn-service.txn-client log/logger.go:51 this is a info log {"uuid": "cn0"}
}

func ExampleMOLogger_Info() {
	getServiceLogger(metadata.ServiceType_CN, "cn0").Info("this is a info log")
	// Output logs:
	// 2022/11/17 15:27:52.036861 +0800 INFO cn-service log/logger.go:51 this is a info log {"uuid": "cn0"}
}

func ExampleMOLogger_Debug() {
	getServiceLogger(metadata.ServiceType_CN, "cn0").Debug("this is a debug log")
	// Output logs:
	// 2022/11/17 15:27:52.036861 +0800 DEBUG cn-service log/logger.go:51 this is a debug log {"uuid": "cn0"}
}

func ExampleMOLogger_Error() {
	getServiceLogger(metadata.ServiceType_CN, "cn0").Error("this is a error log")
	// Output logs:
	// 2022/11/17 15:27:52.036861 +0800 ERROR cn-service log/logger.go:51 this is a error log {"uuid": "cn0"}
}

func ExampleMOLogger_Warn() {
	getServiceLogger(metadata.ServiceType_CN, "cn0").Warn("this is a warn log")
	// Output logs:
	// 2022/11/17 15:27:52.036861 +0800 WARN cn-service log/logger.go:51 this is a warn log {"uuid": "cn0"}
}

func ExampleMOLogger_Panic() {
	getServiceLogger(metadata.ServiceType_CN, "cn0").Panic("this is a panic log")
	// Output logs:
	// 2022/11/17 15:27:52.036861 +0800 PANIC cn-service log/logger.go:51 this is a panic log {"uuid": "cn0"}
	// panic stacks...
}

func ExampleMOLogger_Fatal() {
	getServiceLogger(metadata.ServiceType_CN, "cn0").Fatal("this is a fatal log")
	// Output logs:
	// 2022/11/17 15:27:52.036861 +0800 FATAL cn-service log/logger.go:51 this is a fatal log {"uuid": "cn0"}
	// fatal stacks...
}

func ExampleMOLogger_Log() {
	getServiceLogger(metadata.ServiceType_CN, "cn0").Log("this is a example log",
		log.DefaultLogOptions(),
		zap.String("field-1", "field-1"),
		zap.String("field-2", "field-2"))
	// Output logs:
	// 2022/11/17 15:27:52.036861 +0800 INFO cn-service log/logger.go:51 this is a example log {"uuid": "cn0", "field-1": "field-1", "field-2": "field-2"}
}

func ExampleMOLogger_LogAction() {
	someAction()
	// Output logs:
	// 2022/11/17 15:28:15.599321 +0800 INFO cn-service log/logger.go:51 do action {"uuid": "cn0"}
	// 2022/11/17 15:28:16.600754 +0800 INFO cn-service log/logger.go:51 do action {"uuid": "cn0", "cost": "1.001430792s"}
}

func ExampleLogOptions_WithProcess() {
	processStep1InCN("txn uuid")
	processStep2InDN("txn uuid")
	processStep3InLOG("txn uuid")

	// Output logs:
	// 2022/11/17 15:36:04.724470 +0800 INFO cn-service log/logger.go:51 step 1 {"uuid": "cn0", "process": "txn", "process-id": "txn uuid"}
	// 2022/11/17 15:36:04.724797 +0800 INFO dn-service log/logger.go:51 step 2 {"uuid": "dn0", "process": "txn", "process-id": "txn uuid"}
	// 2022/11/17 15:36:04.724812 +0800 INFO log-service log/logger.go:51 step 3 {"uuid": "log0", "process": "txn", "process-id": "txn uuid"}
}

func ExampleLogOptions_WithSample() {
	logger := getServiceLogger(metadata.ServiceType_CN, "cn0")

	n := 2
	for i := 0; i < n; i++ {
		logger.Log("example sample log",
			log.DefaultLogOptions().WithSample(log.ExampleSample))
	}
	// Output logs:
	// 2022/11/17 15:43:14.645242 +0800 INFO cn-service log/logger.go:51 example sample log {"uuid": "cn0"}
}

func someAction() {
	logger := getServiceLogger(metadata.ServiceType_CN, "cn0")
	defer logger.InfoAction("do action")()
	time.Sleep(time.Second)
}

func processStep1InCN(id string) {
	logger := getServiceLogger(metadata.ServiceType_CN, "cn0")
	logger.Log("step 1", log.DefaultLogOptions().WithProcess(log.Txn, id))
}

func processStep2InDN(id string) {
	logger := getServiceLogger(metadata.ServiceType_DN, "dn0")
	logger.Log("step 2", log.DefaultLogOptions().WithProcess(log.Txn, id))
}

func processStep3InLOG(id string) {
	logger := getServiceLogger(metadata.ServiceType_LOG, "log0")
	logger.Log("step 3", log.DefaultLogOptions().WithProcess(log.Txn, id))
}

func getServiceLogger(serviceType metadata.ServiceType, uuid string) *log.MOLogger {
	return log.GetServiceLogger(logutil.GetGlobalLogger(), serviceType, uuid)
}
