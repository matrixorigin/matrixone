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
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

func GetCronLogger(logInfo bool) cron.Logger {
	logger := GetSkip1Logger().Sugar().Named("cron")
	return &CronLogger{
		SugaredLogger: logger,
		logInfo:       logInfo,
	}
}

type CronLogger struct {
	*zap.SugaredLogger
	logInfo bool
}

// Info implement cron.Logger
func (l *CronLogger) Info(msg string, keysAndValues ...any) {
	if l.logInfo {
		l.Infow(msg, keysAndValues...)
	}
}

// Error implement cron.Logger
func (l *CronLogger) Error(err error, msg string, keysAndValues ...any) {
	keysAndValues = append(keysAndValues, zap.Error(err))
	l.Errorw(msg, keysAndValues...)
}
