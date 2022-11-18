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
	"go.uber.org/zap"
)

func ExampleGetServiceLogger() {
	cnServiceLogger := log.GetServiceLogger(logutil.GetGlobalLogger(), log.CN, "cn0")
	log.Info(cnServiceLogger).Log("this is a info log")
	// Output logs:
	// 2022/11/17 15:25:49.375367 +0800 INFO cn-service log/logger.go:51 this is a info log {"uuid": "cn0"}
}

func ExampleGetModuleLogger() {
	cnServiceLogger := log.GetServiceLogger(logutil.GetGlobalLogger(), log.CN, "cn0")
	txnClientLogger := log.GetModuleLogger(cnServiceLogger, log.TxnClient)
	log.Info(txnClientLogger).Log("this is a info log")
	// Output logs:
	// 2022/11/17 15:27:24.562799 +0800 INFO cn-service.txn-client log/logger.go:51 this is a info log {"uuid": "cn0"}
}

func ExampleInfo() {
	log.Info(logutil.GetGlobalLogger()).Log("this is a info log")
	// Output logs:
	// 2022/11/17 15:27:52.036861 +0800 INFO log/logger.go:51 this is a info log
}

func ExampleDebug() {
	log.Debug(logutil.GetGlobalLogger()).Log("this is a debug log")
	// Output logs:
	// 2022/11/17 15:27:52.036861 +0800 DEBUG log/logger.go:51 this is a debug log
}

func ExampleError() {
	log.Error(logutil.GetGlobalLogger()).Log("this is a error log")
	// Output logs:
	// 2022/11/17 15:27:52.036861 +0800 ERROR log/logger.go:51 this is a error log
}

func ExampleWarn() {
	log.Warn(logutil.GetGlobalLogger()).Log("this is a warn log")
	// Output logs:
	// 2022/11/17 15:27:52.036861 +0800 WARN log/logger.go:51 this is a warn log
}

func ExamplePanic() {
	log.Panic(logutil.GetGlobalLogger()).Log("this is a panic log")
	// Output logs:
	// 2022/11/17 15:27:52.036861 +0800 PANIC log/logger.go:51 this is a panic log
	// panic stacks...
}

func ExampleFatal() {
	log.Fatal(logutil.GetGlobalLogger()).Log("this is a fatal log")
	// Output logs:
	// 2022/11/17 15:27:52.036861 +0800 FATAL log/logger.go:51 this is a fatal log
	// fatal stacks...
}

func ExampleLogContext_Log() {
	log.Info(logutil.GetGlobalLogger()).Log("this is a example log",
		zap.String("field-1", "field-1"),
		zap.String("field-2", "field-2"))
	// Output logs:
	// 2022/11/17 15:27:52.036861 +0800 INFO log/logger.go:51 this is a example log {"field-1": "field-1", "field-2": "field-2"}
}

func ExampleLogContext_LogAction() {
	someAction()
	// Output logs:
	// 2022/11/17 15:28:15.599321 +0800 INFO log/logger.go:51 do action
	// 2022/11/17 15:28:16.600754 +0800 INFO log/logger.go:51 do action {"cost": "1.001430792s"}
}

func ExampleLogContext_WithProcess() {
	processStep1InCN("txn uuid")
	processStep2InDN("txn uuid")
	processStep3InLOG("txn uuid")

	// Output logs:
	// 2022/11/17 15:36:04.724470 +0800 INFO cn-service log/logger.go:51 step 1 {"uuid": "cn0", "process": "txn", "process-id": "txn uuid"}
	// 2022/11/17 15:36:04.724797 +0800 INFO dn-service log/logger.go:51 step 2 {"uuid": "dn0", "process": "txn", "process-id": "txn uuid"}
	// 2022/11/17 15:36:04.724812 +0800 INFO log-service log/logger.go:51 step 3 {"uuid": "log0", "process": "txn", "process-id": "txn uuid"}
}

func ExampleLogContext_WithSampleLog() {
	n := 10000
	for i := 0; i < n; i++ {
		log.Info(logutil.GetGlobalLogger()).WithSampleLog(log.ExampleSample, uint64(n)).Log("example sample log")
	}
	// Output logs:
	// 2022/11/17 15:43:14.645242 +0800 INFO log/logger.go:51 example sample log
}

func someAction() {
	defer log.Info(logutil.GetGlobalLogger()).LogAction("do action")()
	time.Sleep(time.Second)
}

func processStep1InCN(id string) {
	logger := log.GetServiceLogger(logutil.GetGlobalLogger(), log.CN, "cn0")
	log.Info(logger).WithProcess(log.Txn, id).Log("step 1")
}

func processStep2InDN(id string) {
	logger := log.GetServiceLogger(logutil.GetGlobalLogger(), log.DN, "dn0")
	log.Info(logger).WithProcess(log.Txn, id).Log("step 2")
}

func processStep3InLOG(id string) {
	logger := log.GetServiceLogger(logutil.GetGlobalLogger(), log.LOG, "log0")
	log.Info(logger).WithProcess(log.Txn, id).Log("step 3")
}
