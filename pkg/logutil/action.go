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

package logutil

import (
	"time"

	"go.uber.org/zap"
)

var (
	actionClose     = zap.String("action", "close")
	actionAsyncTask = zap.String("action", "run-async-task")
)

// LogClose used to log close any components.
func LogClose(logger *zap.Logger, components string, fields ...zap.Field) func() {
	startAt := time.Now()
	fields = append(fields,
		actionClose,
		componentsField(components))

	logger.Info("start to close components", fields...)
	return func() {
		fields = append(fields, costField(startAt))
		logger.Info("close components completed", fields...)
	}
}

// LogAsyncTask used to log any async task, and will write a log if the async task exited.
func LogAsyncTask(logger *zap.Logger, task string, fields ...zap.Field) func() {
	startAt := time.Now()
	fields = append(fields,
		actionAsyncTask,
		taskField(task))

	logger.Info("start to run async task", fields...)
	return func() {
		fields = append(fields, costField(startAt))
		logger.Info("async task exited", fields...)
	}
}

func componentsField(components string) zap.Field {
	return zap.String("components", components)
}

func taskField(task string) zap.Field {
	return zap.String("async-task", task)
}

func costField(startAt time.Time) zap.Field {
	return zap.Duration("cost", time.Duration(time.Since(startAt)))
}
