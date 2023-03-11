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

package lockservice

import (
	"encoding/hex"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"go.uber.org/zap"
)

var (
	logger *log.MOLogger
	once   sync.Once
)

func getLogger() *log.MOLogger {
	once.Do(func() {
		rt := runtime.ProcessLevelRuntime()
		if rt == nil {
			rt = runtime.DefaultRuntime()
		}
		logger = rt.Logger().Named("lockservice").
			WithOptions(zap.AddCallerSkip(1))
	})
	return logger
}

func logStartLock(
	tableID uint64,
	rows [][]byte,
	txnID []byte,
	options LockOptions) {
	logger := getLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("start to lock",
			txnIDField(txnID),
			zap.Uint64("table", tableID),
			zap.Any("rows", rows),
			zap.String("options", options.String()))
	}
}

func logEndLock(
	tableID uint64,
	rows [][]byte,
	txnID []byte,
	options LockOptions) {
	logger := getLogger()
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("end to lock",
			txnIDField(txnID),
			zap.Uint64("table", tableID),
			zap.Any("rows", rows),
			zap.String("options", options.String()))
	}
}

func logKeepBindFailed(
	err error) {
	logger := getLogger()
	if logger.Enabled(zap.ErrorLevel) {
		logger.Error("failed to keep lock table bind",
			zap.Error(err))
	}
}

func txnIDField(id []byte) zap.Field {
	return zap.String("txn-id", hex.EncodeToString(id))
}
