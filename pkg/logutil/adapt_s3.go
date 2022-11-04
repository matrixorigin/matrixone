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
	"fmt"
	"github.com/aws/smithy-go/logging"
	"go.uber.org/zap"
)

func GetS3Logger() logging.Logger {
	logger := GetSkip1Logger().Named("S3")
	return &S3Logger{
		Logger: logger,
	}
}

var _ logging.Logger = (*S3Logger)(nil)

type S3Logger struct {
	*zap.Logger
}

func (logger *S3Logger) Logf(classification logging.Classification, format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	switch classification {
	case logging.Warn:
		logger.Warn(msg)
	case logging.Debug:
		logger.Debug(msg)
	default:
		logger.Info(msg)
	}
}
