// Copyright 2021 Matrix Origin
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

package fz

import (
	"encoding/json"
	"os"

	"github.com/reusee/e4"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Logger = *zap.Logger

func (_ Def) Logger(
	testDataFilePath TestDataFilePath,
	id UUID,
	isTesting IsTesting,
) Logger {

	logFilePath := testDataFilePath(id, "cube", "log")
	ce(os.Truncate(logFilePath, 0), e4.Ignore(os.ErrNotExist))
	if isTesting {
		logFilePath = "stdout"
	}

	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevel()
	cfg.Level.SetLevel(zap.DebugLevel)
	cfg.EncoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout("2006-01-02 15:04:05.000")
	cfg.EncoderConfig.EncodeDuration = zapcore.MillisDurationEncoder
	ce(json.Unmarshal([]byte(`
  {
    "encoding": "json",
    "outputPaths": [
      "`+logFilePath+`"
    ]
  }
  `), &cfg))
	logger, err := cfg.Build(
		zap.OnFatal(zapcore.WriteThenPanic),
		zap.AddStacktrace(zapcore.FatalLevel),
		zap.AddCaller(),
	)
	ce(err)

	logger = logger.With(
		zap.String("case", id.String()),
	)

	return logger
}
